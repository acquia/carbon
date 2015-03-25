import argparse
import json
import os
import tempfile
import unittest
from mock import MagicMock

from cassandra_rollup import Config

class ConfigTest(unittest.TestCase):
  def setUp(self):
    config = argparse.Namespace()
    config.servers = ["109.112.166.142", "239.36.163.39", "246.254.127.96", "88.164.37.48", "202.56.95.85"]
    config.password = 'b2av8yqqp'
    config.username = 'cassandra'
    config.keyspace = 'graphite'
    self.config = config
    self.zk_servers = ["222.211.220.148", "89.154.41.113", "21.159.69.237", "215.200.93.245", "192.176.231.168"]
    self.zk_password = 'mdlzqgphl'
    self.zk_coordination = True

  def test_load_config(self):
    with tempfile.NamedTemporaryFile() as f:
      self.config.config_file = f.name
      f.write(json.dumps({
        'acl_password': self.zk_password,
        'zk_servers': self.zk_servers,
        'zk_coordination': self.zk_coordination,
        'cassandra_servers': self.config.servers,
        'cassandra_password': self.config.password,
        'cassandra_username': self.config.username,
      }))
      f.seek(0)

      config = Config(self.config)
      config.load_config()
      print [i for i in config.__dict__.keys()]
      self.assertEqual(config.cassandra_password, self.config.password)
      self.assertEqual(config.cassandra_username, self.config.username)
      self.assertEqual(config.cassandra_servers, self.config.servers)
      self.assertEqual(config.keyspace, self.config.keyspace)
      self.assertEqual(config.zookeeper_servers, self.zk_servers)
      self.assertEqual(config.zookeeper_acl, self.zk_password)
      self.assertEqual(config.zookeeper_coordination, self.zk_coordination)

  def test_env_vars_load_config(self):
    del self.config.servers
    del self.config.password
    del self.config.username
    servers = ','.join(["31.156.222.7", "11.87.52.128", "22.155.213.237",
                        "245.107.9.181", "53.96.73.20"])
    username = 'test'
    password = '1ox90mfz5'
    os.environ["CASSANDRA_SERVERS"] = servers
    os.environ["CASSANDRA_USERNAME"] = username
    os.environ["CASSANDRA_PASSWORD"] = password
    os.environ["ZOOKEEPER_ACL_PASSWORD"] = self.zk_password
    os.environ["ZOOKEEPER_SERVERS"] = ','.join(self.zk_servers)

    config = Config(self.config)
    config.load_config()
    self.assertEqual(config.cassandra_servers, servers.split(','))
    self.assertEqual(config.cassandra_password, password)
    self.assertEqual(config.cassandra_username, username)
    self.assertEqual(config.zookeeper_servers, self.zk_servers)
    self.assertEqual(config.zookeeper_acl, self.zk_password)
