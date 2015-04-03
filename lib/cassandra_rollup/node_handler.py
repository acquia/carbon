"""
Portions of this script are forked from ceres_maintenance.py
The rest of it provides abstractions around what to do when walking a
tree of metrics retrieved from Cassandra
"""
import collections
import datetime
import logging
import time

from carbon_cassandra_plugin import carbon_cassandra_db

def fmt_unix(timestamp):
  """Utility function to format timestamps"""
  return datetime.datetime.fromtimestamp(int(timestamp)).strftime(
      '%Y-%m-%d %H:%M:%S')

#######################################################
# Put your custom aggregation logic in this function! #
#######################################################
def aggregate(node, datapoints):
  "Put your custom aggregation logic here."

  values = [value for (timestamp, value) in datapoints if value is not None]
  metadata = node.readMetadata()
  method = metadata.get('aggregationMethod', 'avg')

  if method in ('avg', 'average'):
    return float(sum(values)) / len(values) # values is guaranteed to be nonempty
  elif method == 'sum':
    return sum(values)
  elif method == 'min':
    return min(values)
  elif method == 'max':
    return max(values)
  elif method == 'median':
    values.sort()
    return values[len(values) / 2]
  else:
    raise RuntimeError("Unknown aggregate function {0}".format(method))

class NodeHandler(object):
  @staticmethod
  def node_found(node):
    """Called from the maintenance script to handle a leaf node.
    """
    start_time = datetime.datetime.now()
    logging.info("Started rolling up %s", node.nodePath)
    archives = []
    t = int(time.time())
    metadata = node.readMetadata()

    for (precision, retention) in metadata['retentions']:
      archiveEnd = t - (t % precision)
      archiveStart = archiveEnd - (precision * retention)
      t = archiveStart
      archives.append({
        'precision' : precision,
        'retention' : retention,
        'startTime' : archiveStart,
        'endTime' : archiveEnd,
        'slices' : [s for s in node.slices if s.timeStep == precision]
        })

    for i, archive in enumerate(archives):
      if i == len(archives) - 1:
        NodeHandler.do_rollup(node, archive, None)
      else:
        NodeHandler.do_rollup(node, archive, archives[i+1])
    end_time = datetime.datetime.now()
    logging.info("Finished rolling up %s. Took %s", node.nodePath, str(end_time - start_time))
    return


  @staticmethod
  def do_rollup(node, fineArchive, coarseArchive):
    """Rollup overflow data points from the `fineArchive` into the
      `coarseArchive`.
    """
    # Previously if the course archive was None the code would
    # delete entries from before the start of the fine archive.
    # we do not do that because we rely on TTL to remove data points.
    if not coarseArchive:
      return

    coarseWrapper = coarseArchive if coarseArchive else \
        collections.defaultdict(lambda: None)
    logging.debug("Rollup called on node {path} from fine archive precision "\
        "{fine_precision} retention {fine_retention} start {fine_start} end "\
        "{fine_end} to coarse archive precision {coarse_precision} retention"\
        " {coarse_retention} start {coarse_start} end {coarse_end}".format(
            path=node.nodePath,
            fine_precision=fineArchive["precision"],
            fine_retention=fineArchive["retention"],
            fine_start=fmt_unix(fineArchive["startTime"]),
            fine_end=fmt_unix(fineArchive["endTime"]),
            coarse_precision=coarseWrapper["precision"],
            coarse_retention=coarseWrapper["retention"],
            coarse_start=fmt_unix(coarseWrapper["startTime"] or 0),
            coarse_end=fmt_unix(coarseWrapper["endTime"] or 0)
            )
    )

    overflowSlices = [
        s for s in fineArchive['slices']
        if s.startTime < fineArchive['startTime']
        ]
    if not overflowSlices:
      return

    overflowDatapoints = []
    for slice in overflowSlices:
      try:
        datapoints = slice.read(slice.startTime, fineArchive['startTime'])
      except (carbon_cassandra_db.NoData) as e:
        datapoints = []
    overflowDatapoints.extend(list(datapoints))
    overflowDatapoints.sort()
    logging.debug("Got %s overflow data points for %s" % (len(overflowDatapoints),
        node.nodePath))

    fineStep = fineArchive['precision']
    coarseStep = coarseArchive['precision']
    deletePriorTo = coarseArchive['startTime'] + (coarseStep * coarseArchive['retention'])
    metadata = node.readMetadata()
    xff = metadata.get('xFilesFactor')

    # We define a window corresponding to exactly one coarse datapoint
    # Then we use it to select datapoints for aggregation
    for i in range(coarseArchive['retention']):
      windowStart = coarseArchive['startTime'] + (i * coarseStep)
      windowEnd = windowStart + coarseStep

      fineDatapoints = [
        d for d in overflowDatapoints
        if d[0] >= windowStart and d[0] < windowEnd
      ]
      logging.debug("There are %s fine archive data points in the window %s to %s "\
          "for coarse archive retention %s" % (len(fineDatapoints),
              fmt_unix(windowStart), fmt_unix(windowEnd), i))
      if not fineDatapoints:
        continue

      knownValues = [
          value
          for (timestamp,value) in fineDatapoints
          if value is not None
          ]
      if not knownValues:
        logging.debug("None of the fine archive data points were known, skipping.")
        continue

      knownPercent = float(len(knownValues)) / len(fineDatapoints)
      if knownPercent < xff:  # we don't have enough data to aggregate!
        logging.debug("Percent of known fine data points %s less then "\
            "xFilesFactor %s, skipping." % (knownPercent, xff))
        continue

      coarseValue = aggregate(node, fineDatapoints)
      coarseDatapoint = (windowStart, coarseValue)
      logging.debug("Coarse data point retention %s has timestamp %s and "\
          "value %s" % (i, coarseDatapoint[0], coarseDatapoint[1]))
      fineValues = [d[1] for d in fineDatapoints]

      written = False
      # we only have one slice for all archives, but lets leave this here
      for slice in coarseArchive['slices']:
        if slice.startTime <= windowStart and slice.endTime >= windowStart:
          slice.write([coarseDatapoint])
          logging.debug("Wrote data point to existing coarse archive slice")
          written = True
          break

      # Old comment, think we will never need this code...
      # We could pre-pend to an adjacent slice starting after windowStart
      # but that would be much more expensive in terms of I/O operations.
      # In the common case, append-only is best.
      if not written:
        newSlice = carbon_cassandra_db.DataSlice.create(node, windowStart,
            coarseStep)
        newSlice.write([coarseDatapoint])
        coarseArchive['slices'].append(newSlice)
        deletePriorTo = min(deletePriorTo, windowStart)

    # Previously we would delete the overflow slices from the fine
    # archive at this point. We rely on cassandra TTL to remove them.
    return

class NodePathVisitor(object):
  """Visitor when walking the nodes in the tree.

  Calls through to the `dispatcher` and decides if we should visit the
  children.
  """

  def __init__(self, tree):
    self.tree = tree

  def __call__(self, parentPath, childPath, isMetric):
    """Called to visit the node at `childPath`.

    Returns true if the children should be visited.
    """
    if isMetric:
      if childPath != parentPath:
        NodeHandler.node_found(self.tree.getNode(childPath))
      else:
        # visit the children of this childPath
        return True
    else:
      # got a branch node, visit children
      return True

def walkTree(tree, visitor, nodePath, useDC):
  """Recursively walk the self and childs nodes in `tree` below `nodePath`
    calling the `visitor` function for each visit with nodePath and isMetric.

    If the visitor returns True a recursive call is made to visit the children
    for the current nodePath.

    We use walk the whole tree when we are running a single rollup
    script that will rollup the entire data base.
    """

  pathIter = tree.selfAndChildPaths(nodePath, dcName=useDC)

  for childPath, isMetric in pathIter:
    if visitor(nodePath, childPath, isMetric):
      walkTree(tree, visitor, childPath, useDC=useDC)
  return
