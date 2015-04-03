import json
import os

class Config(object):
  """Container for storing rollup configuration options
  """
  KEY_MAPPINGS = {'CASSANDRA_USERNAME': 'username',
                  'CASSANDRA_PASSWORD': 'password',
                  'CASSANDRA_SERVERS': 'servers'}

  ZOOKEEPER_ENV = ['ZOOKEEPER_ACL_PASSWORD',
                   'ZOOKEEPER_SERVERS',
                   'ZOOKEEPER_COORDINATION']

  GENERAL_OPTIONS = ['CASSANDRA_KEYSPACE',
                     'ROLLUP_THREADS']

  def __init__(self, config):
    self._config = config
    self._zookeeper_acl = None
    self._zookeeper_servers = None
    self._zookeeper_coordination = False
    self._cassandra_username = None
    self._cassandra_password = None
    self._cassandra_servers = None
    self._keyspace = None
    self._num_threads = 32

  def load_config(self):
    """Load the rollup configuration from a JSON configuration file or using
    ENV values
    """
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
        self._num_threads = config.get('num_threads') or self._num_threads
    else:
      config = {x.lower(): os.getenv(x) for x in Config.ZOOKEEPER_ENV}
      self._zookeeper_acl = config['zookeeper_acl_password']
      self._zookeeper_servers = config['zookeeper_servers'].split(',')
      self._zookeeper_coordination = config['zookeeper_coordination'] == 'true'
      self._keyspace = os.getenv('CASSANDRA_KEYSPACE')
      self._num_threads = os.getenv('ROLLUP_THREADS') or self._num_threads

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
    """zookeeper_coordination accessor
    """
    return self._zookeeper_coordination

  @property
  def zookeeper_servers(self):
    """zookeeper_servers accessor
    """
    return self._zookeeper_servers

  @property
  def zookeeper_acl(self):
    """zookeeper_acl accessor
    """
    return self._zookeeper_acl

  @property
  def cassandra_servers(self):
    """cassandra_servers accessor
    """
    return self._cassandra_servers

  @property
  def cassandra_password(self):
    """cassandra_password accessor
    """
    return self._cassandra_password

  @property
  def cassandra_username(self):
    """cassandra_username accessor
    """
    return self._cassandra_username

  @property
  def keyspace(self):
    """cassandra keyspace accessor
    """
    return self._keyspace

  @property
  def num_threads(self):
    """num_threads accessor
    """
    return self._num_threads
