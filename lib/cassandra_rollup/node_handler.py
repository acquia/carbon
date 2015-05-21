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
from pycassa import NotFoundException

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

    if logging.getLogger().getEffectiveLevel() <= logging.DEBUG:
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

    coarseSlices = [s for s in coarseArchive['slices']
                    if s.startTime < coarseArchive['startTime']]
    coarseDatapoints = []
    # Find datapoints in the fine archive's retention period. We need to know
    # if any datapoints have been written so we can skip the rollup if we ran
    # one within the fine archive retention
    if coarseSlices:
      start_time = fineArchive['startTime'] - fineArchive['precision']

      # table, column family. Same thing really.
      fine_archive_cf = node.cfCache.get("ts{}".format(fineArchive['precision']))
      try:
        coarseDatapoints = list(fine_archive_cf.xget(node.nodePath,
                                                column_start=start_time,
                                                column_finish=fineArchive['endTime'],
                                                buffer_size=1000,
                                                include_timestamp=True))

      # There may not be any datapoints if this is a new metric
      except Exception as e:
        coarseDatapoints = []

    # Go through and figure out the last written timestamp
    # If it's within the window then we can skip this rollup
    if coarseDatapoints:
      _, (_, write_time) = coarseDatapoints[-1]

      # CQL/Pycassa insert timestamps in microseconds. fromtimestamp() needs it in seconds
      write_time = datetime.datetime.fromtimestamp(write_time / 1000000.0)
      # Same problem, only we keep track of time in milliseconds in Graphite
      window_end = datetime.datetime.fromtimestamp(fineArchive['endTime'] / 1000.0)
      difference = window_end - write_time

    if (not coarseDatapoints
        or difference.seconds >= (fineArchive['precision'] * fineArchive['retention'])
       ):

      # We only have one slice per dataset so this is fine
      fine_slice = fineArchive['slices'][0]

      # Don't do work if the slice is newer than what we've inserted
      if fine_slice.startTime > fineArchive['startTime']:
        return

      # Get the datapoints in the window specified by the rollup
      # Stop the rollup process for this retention level if there aren't any datapoints
      try:
        datapoints = list(fine_slice.read(fineArchive['startTime'], fineArchive['endTime']))
      except (carbon_cassandra_db.NoData):
        logging.info("No datapoints to rollup in %s", node.nodePath)
        return
      except Exception as e:
        raise e

    # generator to chunk the datapoints into sets of points
    def split_list(data, step):
      for i in xrange(0, len(data), step):
        yield data[i:i+step]

    # We need the xff to know how much data we need in a chunk so that we can roll it up
    metadata = node.readMetadata()
    xff = metadata.get('xFilesFactor')

    # Split the datapoints into chunks to be rolled up
    # Each chunk consists of the set of datapoints that will be aggregated into a single datapoint
    # The number of datapoints is a fraction of the coarse archive:
    # ex.
    #   given 2 retention periods with precision of 10 seconds and 60 seconds
    #   This partitions the list in to groups of 6 (60 seconds / 10 seconds)
    chunked_data = list(split_list(datapoints, coarseArchive['precision'] / fineArchive['precision']))

    rollup_values = []
    window_end = fineArchive['precision'] * fineArchive['retention']
    for i, val in enumerate(range(0, window_end, coarseArchive['precision'])):
      # Get the timestamp with the step
      rollup_timestamp = fineArchive['startTime'] + val
      points = chunked_data[i]

      known_values = [value for (_, value) in points if value is not None]
      # Only rollup if we have enough data to make it worthwhile (the xff)
      if float(len(known_values)) / len(points) >= xff:
        value = aggregate(node, points)
        rollup_point = (rollup_timestamp, value)
        rollup_values.append(rollup_point)

    # Batch write the new values to Cassandra
    if rollup_values:
      coarseArchive['slices'][0].write(rollup_values)
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
