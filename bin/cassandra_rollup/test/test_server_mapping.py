import unittest
from mock import MagicMock

from cassandra_rollup import ServerMapping

class ServerMappingTest(unittest.TestCase):
  def setUp(self):
    mock_kazoo = MagicMock
    self.cassandra_servers = ["65.193.60.101", "121.87.63.119", "212.172.27.249",
                              "19.86.239.159", "199.249.17.45"]
    self.graphite_servers = ["27.91.217.73", "23.201.26.200", "226.23.135.221",
                             "60.42.58.185", "238.92.35.168"]
    self.mapping = ServerMapping(mock_kazoo, self.cassandra_servers,
                                 self.graphite_servers)

  def test_partitioning(self):
    self.assertEqual(self.mapping.partition_servers(), {'226.23.135.221': ['212.172.27.249'],
                                                        '27.91.217.73': ['65.193.60.101'],
                                                        '23.201.26.200': ['121.87.63.119'],
                                                        '238.92.35.168': ['199.249.17.45'],
                                                        '60.42.58.185': ['19.86.239.159']})

    # Add a new Cassandra server
    # It should be assigned to one of the existing Graphite servers
    self.mapping.cassandra_servers.append('108.252.37.112')
    self.assertEqual(self.mapping.partition_servers(), {'226.23.135.221': ['212.172.27.249'],
                                                        '27.91.217.73': ['65.193.60.101', '108.252.37.112'],
                                                        '23.201.26.200': ['121.87.63.119'],
                                                        '238.92.35.168': ['199.249.17.45'],
                                                        '60.42.58.185': ['19.86.239.159']})

    # Add a new Graphite server
    # The new Graphite server should be assigned to the new Cassandra server
    self.mapping.graphite_servers.append('241.227.223.147')
    self.assertEqual(self.mapping.partition_servers(), {'226.23.135.221': ['212.172.27.249'],
                                                        '27.91.217.73': ['65.193.60.101'],
                                                        '23.201.26.200': ['121.87.63.119'],
                                                        '238.92.35.168': ['199.249.17.45'],
                                                        '60.42.58.185': ['19.86.239.159'],
                                                        '241.227.223.147': ['108.252.37.112']})

    # Add a Graphite server without a corresponding Cassandra server
    self.mapping.graphite_servers.append('86.85.11.247')
    self.assertEqual(self.mapping.partition_servers(), {'226.23.135.221': ['212.172.27.249'],
                                                        '27.91.217.73': ['65.193.60.101'],
                                                        '23.201.26.200': ['121.87.63.119'],
                                                        '238.92.35.168': ['199.249.17.45'],
                                                        '60.42.58.185': ['19.86.239.159'],
                                                        '241.227.223.147': ['108.252.37.112']})


  def test_mapping(self):
    self.assertEqual(self.mapping.mapping_to_znode_str(self.mapping.partition_servers()),
                     ['226.23.135.221:212.172.27.249', '27.91.217.73:65.193.60.101',
                      '23.201.26.200:121.87.63.119', '238.92.35.168:199.249.17.45',
                      '60.42.58.185:19.86.239.159'])

  def test_split_znode(self):
    result = ServerMapping.split_znode('4.57.147.110:8.106.219.235,208.48.253.178,54.9.164.96,145.241.190.43,199.130.247.12')
    actual = ('4.57.147.110', ['8.106.219.235',
                               '208.48.253.178',
                               '54.9.164.96',
                               '145.241.190.43',
                               '199.130.247.12'])

    self.assertEqual(result, actual)
