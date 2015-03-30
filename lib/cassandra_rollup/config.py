import json
import os

class Config(object):
  KEY_MAPPINGS = {
    'CASSANDRA_USERNAME': 'username',
    'CASSANDRA_PASSWORD': 'password',
    'CASSANDRA_SERVERS': 'servers',
  }

  ZOOKEEPER_ENV = ['ZOOKEEPER_ACL_PASSWORD',
    'ZOOKEEPER_SERVERS',
    'ZOOKEEPER_COORDINATION',
  ]

  def __init__(self, config):
    self._config = config
    self._zookeeper_acl = None
    self._zookeeper_servers = None
    self._zookeeper_coordination = False
    self._cassandra_username = None
    self._cassandra_password = None
    self._cassandra_servers = None
    self._keyspace = None

  def load_config(self):
    # Get ZK configuration data
    if hasattr(self._config, 'config_file'):
      with open(self._config.config_file) as conf:
        config = json.load(conf)
        self._zookeeper_acl = config.get('acl_password')
        self._zookeeper_coordination = config.get('zk_coordination')
        self._zookeeper_servers = config.get('zk_servers')
        self._cassandra_servers = config.get('cassandra_servers')
        self._cassandra_password = config.get('cassandra_password')
        self._cassandra_username = config.get('cassandra_username')
        self._keyspace = config.get('keyspace')
    else:
      config = {x.lower(): os.getenv(x) for x in Config.ZOOKEEPER_ENV}
      self._zookeeper_acl = config['zookeeper_acl_password']
      self._zookeeper_servers = config['zookeeper_servers'].split(',')
      self._zookeeper_coordination = config['zookeeper_coordination'] == 'true'
      self._keyspace = os.getenv('CASSANDRA_KEYSPACE')

      # Load Cassandra settings through the environment
      for env_key, option_key in Config.KEY_MAPPINGS.iteritems():
        if hasattr(self._config, option_key):
          setattr(self, "_{0}".format(env_key.lower()), getattr(self._config, option_key))
        else:
          value = os.environ[env_key]
          if env_key == 'CASSANDRA_SERVERS':
            value = value.split(',')
          setattr(self, "_{0}".format(env_key.lower()), value)


  @property
  def zookeeper_coordination(self):
    return self._zookeeper_coordination

  @property
  def zookeeper_servers(self):
    return self._zookeeper_servers

  @property
  def zookeeper_acl(self):
    return self._zookeeper_acl

  @property
  def cassandra_servers(self):
    return self._cassandra_servers

  @property
  def cassandra_password(self):
    return self._cassandra_password

  @property
  def cassandra_username(self):
    return self._cassandra_username

  @property
  def keyspace(self):
    return self._keyspace
