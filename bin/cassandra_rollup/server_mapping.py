import os
import socket

class ServerMapping(object):
  def __init__(self, zk_client, cassandra_servers, graphite_servers):
    self.client = zk_client
    self._cassandra_servers = cassandra_servers
    self._graphite_servers = graphite_servers

  def generate_mappings(self):
    lock = self.client.Lock(self.client.graphite_servers, socket.gethostname)
    with lock:
      children = self.client.get_children(self.client.graphite_servers)
      for znode in children:
        graphite_ip, _ = znode.split(':')
        if graphite_ip in self.graphite_servers:
          self.client.delete(znode)
      mapping = self.partition_servers()
      znodes = self.mapping_to_znode_str(mapping)

      for znode in znodes:
        self.client.ensure_path(znode)

  @staticmethod
  def mapping_to_znode_str(mapping):
    return [':'.join([x, ','.join(y)]) for x, y in mapping.iteritems()]

  @staticmethod
  def split_znode(znode):
    graphite, cassandra = znode.split(':')
    return (graphite, cassandra.split(','))

  def partition_servers(self):
    num_graphite_servers = len(self.graphite_servers)

    # Python hackery - map over the lists on a single pass and assign a 1:1
    # mapping between cassandra and graphite servers, preserving the remainder
    # so we can go through and assign things later
    result = map(None, self.graphite_servers, self.cassandra_servers)
    mapping = {x: [y] for (x, y) in result if x != None and y != None}

    # Only care about the case where we have unassigned Cassandra nodes
    missing_graphite = [y for (x, y) in result if x == None]
    cassandra_with_indexes = [(x, y) for y, x  in enumerate(missing_graphite)]
    for cass, index in cassandra_with_indexes:
      key = index % num_graphite_servers
      mapping[self.graphite_servers[key]].append(cass)

    return mapping

  @property
  def cassandra_servers(self):
    return self._cassandra_servers

  @cassandra_servers.setter
  def cassandra_servers(self, servers):
    self._cassandra_servers = servers

  @property
  def graphite_servers(self):
    return self._graphite_servers

  @graphite_servers.setter
  def graphite_servers(self, servers):
    self._graphite_servers = servers
