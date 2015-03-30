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
    self._carbon_config_dir = options.configdir
    self._config_file = options.config_file
    self._config = None
    self._zookeeper = None
    self._tree = None

    # This ensures that we leave the party cleanly if the script is told to
    # shut down
    # It's defined as a closure so that we have access to the partitioner
    def signal_handler(*args):
      self.zookeeper.partitioner.release_set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

  @property
  def config(self):
    if not self._config:
      self.read_config()

    return self._config

  def read_config(self):
    self._config = Config(self._options).load_config

  @property
  def zookeeper(self):
    return self._zookeeper

  def zk(self):
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
                                              read_consistency_level=ConsistencyLevel.QUORUM)
  @property
  def tree(self):
    if not self._tree:
      self.load_tree()
    return self._tree

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
    cassandra_servers = self.config.cassandra_servers
    self.read_config()
    self.zookeeper.update_hosts(self.config.zookeeper_servers)

    self.zookeeper.partition(self.config.cassandra_servers)

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
      partition = [p for p in self.zookeeper.partitioner]

      visitor = NodePathVisitor(self.tree)
      # work on a sub set of the data nodes whose nodePath is in the the
      # token ranges owned by the cassandra nodes in rollup_targets
      for startToken, endToken, nodeIP in self.tokenRangesForNodes(partition):
        t = threading.Thread(target=self.walkRange, args=(visitor, False, startToken, endToken))
        t.start()

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

