#!/usr/bin/env python
import argparse
import logging
import signal

from apscheduler.schedulers.blocking import BlockingScheduler
from carbon_cassandra_plugin import carbon_cassandra_db
from cassandra_rollup import Zookeeper, Config, RollupHandler

scheduler = None

def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--config-file', help='Path to a JSON configuration file with Zookeeper information.')
  parser.add_argument('--log-level', default='info', help='Verbosity of logging (info or debug)')
  parser.add_argument('--interval', default=60, help='Interval to call the rollup process', type=int)
  parser.add_argument('--log-file', default='/var/log/cassandra_rollup.log', help='Path to write the log to')

  # not used right now
  parser.add_argument('--dc-name', default=None, help='Name of the Cassandra Data Center to rollup nodes from')

  args = parser.parse_args()

  if not args.config_file:
    parser.error("You must supply a path to a config file")

  return args

def setup(options):
  filename = 'log'
  if options.log_file:
    filename = options.log_file

  if options.log_level == 'debug':
    level = logging.DEBUG
  else:
    level = logging.INFO
  logging.basicConfig(filename=filename, level=level)

def signal_handler(signal, frame):
  scheduler.shutdown()

def main(options):
  handler = RollupHandler(options)
  scheduler = BlockingScheduler()
  scheduler.add_job(handler.rollup, trigger='interval', seconds=options.interval,
                    coalesce=True)

  signal.signal(signal.SIGTERM, signal_handler)
  signal.signal(signal.SIGHUP, signal_handler)
  signal.signal(signal.SIGINT, signal_handler)
  scheduler.start()

if __name__ == '__main__':
  args = get_args()
  setup(args)
  main(args)
