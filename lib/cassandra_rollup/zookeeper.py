"""
Wrapper around a Kazoo client
This provides a way to quickly partition a set of Cassandra servers
"""
import logging
import os

from kazoo.client import KazooClient, KazooState

class Zookeeper(object):
  """Wrap the Kazoo module and handle locking for rollups
  """
  def __init__(self, servers, acl_password):
    self.zk_servers = servers
    self._client = None
    self._partitioner = None
    self.base_path = '/cassandra/'
    self.token_ranges = os.path.join(self.base_path, 'token_ranges')
    self.servers = os.path.join(self.base_path, 'servers')
    self.acl_password = acl_password
    self._token_set = None

  @property
  def client(self):
    """Return a KazooClient
    """
    if not self._client:
      self._client = KazooClient(hosts=','.join(self.zk_servers),
                                 auth_data=[('digest', "client:{0}".format(self.acl_password))],
                                 connection_retry={'max_tries': 5})
      self._client.start()

      listeners = self._listeners()
      for listener in listeners:
        self._client.add_listener(listener)

      self._client.ensure_path(self.servers)
      self._client.ensure_path(self.token_ranges)

    return self._client

  def update_hosts(self, servers):
    """Update the hosts that the Kazoo client needs to connect to
    """
    hosts = [x for (x, _) in self.client.hosts]
    if set(hosts) != set(servers):
      self.client.set_hosts(servers)

  @staticmethod
  def _listeners():
    """Define listeners to define on a KazooClient
    """
    def connection_handler(state):
      """Listener to reconnect to a ZK cluster if lost
      """
      if state == KazooState.LOST:
        logging.warning('Lost ZK session')
      elif state == KazooState.SUSPENDED:
        logging.warning('disconnected from ZK')
      else:
        pass

    return [connection_handler]

  def partition(self, token_ranges):
    """Partition the set of token ranges
    """
    # Store the set in this class so we can re-create the partition if needed
    if self._token_set != token_ranges:
      self._token_set = token_ranges
      if self._partitioner:
        self._partitioner.finish()
      self._partitioner = self.client.SetPartitioner(path=self.servers, set=token_ranges)
      if not self._partitioner.acquired:
        self._partitioner.wait_for_acquire()

  @property
  def partitioner(self):
    """Accessor for the partitioner
    """
    return self._partitioner
