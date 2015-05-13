from concurrent import futures
from datetime import datetime
import logging
import os
import signal
import socket
import sys
import threading

from carbon_cassandra_plugin import carbon_cassandra_db
from pycassa.system_manager import SystemManager
from pycassa import ConsistencyLevel
from cassandra_rollup import Zookeeper, Config, NodePathVisitor

class RollupHandler(object):
  def __init__(self, options):
    self._options = options
    self._config_file = options.config_file
    self._config = None
    self._zookeeper = None
    self._tree = None
    # The abort_rollups() method should be the only interface into changing this flag
    self._abort_rollups = False
    self._thread_count = 32

  @property
  def config(self):
    if not self._config:
      self.read_config()

    return self._config

  def read_config(self):
    config = Config(self._options)
    config.load_config()
    self._config = config

  @property
  def thread_count(self):
    return self._thread_count

  @thread_count.setter
  def thread_count(self, count):
    self._thread_count = count

  @property
  def zookeeper(self):
    if not self._zookeeper:
      self._zookeeper = Zookeeper(servers=self.config.zookeeper_servers, acl_password=self.config.zookeeper_acl)
    return self._zookeeper

  @property
  def credentials(self):
    credentials = None
    if self.config.cassandra_username and self.config.cassandra_password:
      credentials = {'username': self.config.cassandra_username, 'password': self.config.cassandra_password}
    return credentials

  def load_tree(self):
    self._tree = carbon_cassandra_db.DataTree("/", self.config.keyspace,
                                              self.config.cassandra_servers,
                                              localDCName=None,
                                              credentials=self.credentials,
                                              read_consistency_level=ConsistencyLevel.QUORUM,
                                              pool_size=self._thread_count)
  @property
  def tree(self):
    if not self._tree:
      self.load_tree()
    return self._tree

  def abort_rollups(self):
    self._abort_rollups = True

  def rollup(self):
    """
    Since this script is designed to run as a daemon, we need to check for a
    few things before proceeding to roll up metrics:
      - We need to have a current list of Zookeeper servers
      - We need to have a current list of Cassandra servers
      - Using that information, we need to update the partition accordingly and
        get back the servers to rollup

    Once that is all done, we can proceed to divide up the token ring and rollup metrics
    """
    start_time = datetime.now()
    logging.info("Starting rollup process at %s", str(start_time))
    cassandra_servers = self.config.cassandra_servers
    self.read_config()
    self.zookeeper.update_hosts(self.config.zookeeper_servers)

    token_ranges = []
    # @todo: This can be simplified if we don't return nodeIP from tokenRangesForNodes
    for (start_token, end_token, _) in self.tokenRangesForNodes(self.config.cassandra_servers):
      token_range = ':'.join([start_token, end_token])
      token_ranges.append(token_range)
    self.zookeeper.partition(token_ranges)

    partitioner = self.zookeeper.partitioner
    if partitioner.release:
      partitioner.release_set()
      return
    elif partitioner.failed:
      logging.error("Failed to acquire a partition. Skipping run.")
      return
    elif partitioner.allocating:
      partitioner.wait_for_acquire()

    if partitioner.acquired:
      logging.info("Working on token ranges: %s", ','.join([p for p in self.zookeeper.partitioner]))
      partition = [p.split(':') for p in self.zookeeper.partitioner]
      visitor = NodePathVisitor(self.tree)

      # Operate only on the partitioned token ranges
      with futures.ThreadPoolExecutor(max_workers=self._thread_count) as executor:
        for start_token, end_token in partition:
          executor.submit(self.walkRange, visitor, False, start_token, end_token)

    end_time = datetime.now()
    logging.info("Ended rollup process at %s", str(end_time))
    logging.info("Rollup process took %s", str(end_time - start_time))

  def walkRange(self, visitor, useDC, startToken, endToken):
    """Visit the data nodes in between `startToken` and `endToken` and call the
    `visitor` with the nodePath and isMetric flag.

    `useDC` is passed to selfAndChildPaths() on the `tree`

    We walk token ranges when the rollup script is running on multiple
    cassandra nodes, so we are breaking the work up amongst many nodes.
    """
    key = os.path.join(self.zookeeper.token_ranges, ':'.join([startToken, endToken]))
    zk = self.zookeeper.client
    lock = zk.retry(zk.Lock, key,
                    "{0}-{1}".format(socket.gethostname(), os.getpid()))
    contenders = zk.retry(lock.contenders)
    if contenders:
      logging.warning("Competing for lock on {0} with {1}".format(key, contenders))
      return
    with lock:
      pathIter = self.tree.selfAndChildPaths(None, dcName=useDC,
        startToken=startToken, endToken=endToken)

      for childPath, isMetric in pathIter:
        # This allows the thread to abandon any subsequent work it has to do
        # while preserving the current metric rollup operation if it is in progress
        if self._abort_rollups:
          lock.release()
          return
        # we do not know what parent to tell the visitor.
        # well we could get it from the child, but I dont want to.
        visitor(None, childPath, isMetric)
    lock.release()
    return

  def tokenRangesForNodes(self, targetNodes):
    """Get a list of the token ranges owned by the nodes in `targetNodes`.

    The list can be used to partition the maintenance process, e.g. run a
    rollup daemon on each cassandra node that only works with carbon nodes in
    the cassandra nodes primary token ranges.

    Return a list of of [ (startToken, endToken, nodeIP)]
    """
    sysManager = None
    for server in self.config.cassandra_servers:
      sysManager = SystemManager(server, credentials=self.credentials)
      try:
        sysManager.describe_cluster_name()
      except (Exception) as e:
        sysManager = None
      else:
        break
    if not sysManager:
      raise RuntimeError("Could not connect to cassandra nodes %s" % (
          self.config.cassandra_servers,))

    # Get a list of the token ranges in the cluster.
    # create {endToken : tokenRange} from the list of TokenRanges
    # TokenRange is from pycasssa
    allRanges = {
        tokenRange.end_token : tokenRange
        for tokenRange in sysManager.describe_ring(self.config.keyspace)
    }

    # get the tokens assigned to the nodes we care about
    # either initial_token or the random tokens with vnodes
    # dict {'endToken' : ip_address}
    targetNodesSet = set(targetNodes)
    assignments = {
        endToken : nodeIP
        for endToken, nodeIP in sysManager.describe_token_map().iteritems()
        if nodeIP in targetNodesSet
    }

    # merge to find the token ranges for the nodes we care about.
    # the token range will tell us what data to read for the rollups
    tokenRanges = []
    seenRanges = set()
    for endToken, nodeIP in assignments.iteritems():
      try:
        thisRange = allRanges[endToken]
      except (KeyError) as e:
        raise RuntimeError("Could not match assigned token %s from %s "\
            "to a token range from describe_ring()" % (endToken, nodeIP))

      assert nodeIP in thisRange.endpoints
      assert thisRange not in seenRanges
      seenRanges.add(thisRange)

      tokenRanges.append((thisRange.start_token, thisRange.end_token,
          nodeIP))

    assert len(tokenRanges) == len(assignments)
    return tokenRanges

