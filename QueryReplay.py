# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import trino
import logging
import threading
import csv
import time
from datetime import datetime
import base64
from jproperties import Properties
import signal
import os
from trino.dbapi import Cursor

## Retrieve query history
## 1) it can be retrieved from the same cluster where it is going to be replayed (insights catalog or a copy of the insights completed_queries table in some other catalog most be available)
## 2) it can be retrieved from a different cluster (insights catalog or a copy of the insights completed_queries table in some other catalog most be available)
## 3) it can be retrieved from an insights db directly -- NOT SUPPORTED YET
## 4) it can be retrieved from a text file (comma-delimited, tab delimited, or pipe delimited)

## Run queries on different threads
## Will attempt to run queries with the same cadence as originally executed
## The user must have access to all referenced tables
## ONLY SELECT STATEMENTS retrieved from query history when using SEP cluster as source
## Optionally queries can be run sequentially instead of concurrently

class QueryReplay:
  def __init__(self) -> None:
    self.connections = {}
    self.queries = None
    self.running_queries = {}
    self.using_blackhole_catalog = False
    logging.getLogger().setLevel(logging.INFO)

  def __interrupt_handler(self, signum, frame):
    global RUNNING
    if RUNNING:
      RUNNING=False
      if self.using_blackhole_catalog:
        self.__cancelAllRunningQueries()
        self.__closeConnections()
        os._exit(1)

  def __loadConfig(self):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - loading configuration")
    configs = Properties()
    try:
      with open('config.properties', 'rb') as read_prop:
        configs.load(read_prop)
    except FileNotFoundError as fileNotFoundError:
      logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - config.properties file not found")
      os._exit(1)
    except Exception as error:
      logging.error(error)
      os._exit(1)

    props = {}

    for item in configs.items():
      key = item[0]

      value = ""
      try:
        value = base64.b64decode(configs.get(key).data).decode("utf-8").replace('\n', '')
      except:
        value = configs.get(key).data

      props[key] = value

    return props

  def __validateConfig(self, config: dict):
    validConfig = True

    queries_src_type = config.get('queries-src.type')
    queries_src_filename = config.get('queries-src.filename')
    queries_src_host = config.get('queries-src.host')
    queries_src_port = config.get('queries-src.port')
    queries_src_ssl = config.get('queries-src.ssl')
    queries_src_username = config.get('queries-src.username')
    queries_src_password = config.get('queries-src.password')
    queries_src_queries_table = config.get('queries-src.insights-queries-table')
    queries_starttime = config.get('queries.startTime')
    queries_endtime = config.get('queries.endTime')
    queries_dst_host = config.get('queries-dst.host')
    queries_dst_port = config.get('queries-dst.port')
    queries_dst_ssl = config.get('queries-dst.ssl')
    queries_dst_username = config.get('queries-dst.username')
    queries_dst_password = config.get('queries-dst.password')
    queries_dst_unique_connection_per_query = config.get('queries-dst.unique-connection-per-query')
    queries_dst_impersonate_query_user = config.get('queries-dst.impersonate-query-user')
    queries_dst_blackhole_catalog = config.get('queries-dst.blackhole-catalog')
    queries_run_sequentially = config.get('queries.run-sequentially')

    if queries_src_type == None:
      logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries-src.type is required")
      validConfig = False
    elif queries_src_type.lower() not in ['sep', 'csv-file', 'tsv-file', 'pipe-delimited-file']:
      logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: queries-src.type is '" + queries_src_type + "', valid values are 'sep', 'csv-file', 'tsv-file', or 'pipe-delimited-file'")
      validConfig = False

    if queries_src_type.lower() in ['csv-file', 'tsv-file', 'pipe-delimited-file']:
      if queries_src_filename == None:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries-src.filename is required")
        validConfig = False
      elif not os.path.isfile(queries_src_filename):
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: file " + queries_src_filename  + " not found")
        validConfig = False

      if queries_src_host is not None:
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration ignored: queries-src.host will not be used")

      if queries_src_port is not None:
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration ignored: queries-src.port will not be used")

      if queries_src_ssl is not None:
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration ignored: queries-src.ssl will not be used")

      if queries_src_username is not None:
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration ignored: queries-src.username will not be used")

      if queries_src_password is not None:
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration ignored: queries-src.password will not be used")

      if queries_src_queries_table is not None:
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration ignored: queries-src.insights-queries-table will not be used")

      if queries_starttime is not None:
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration ignored: queries.startTime will not be used")

      if queries_endtime is not None:
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration ignored: queries.endTime will not be used")

    if queries_src_type.lower() == 'sep':
      if queries_src_host == None:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries-src.host is required")
        validConfig = False

      if queries_src_port is None:
        config['queries-src.port'] = '8080'
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration default: setting queries-src.port to 8080")
      elif not queries_src_port.isnumeric():
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: queries-src.port '" + queries_src_port  + "' is not a number")
        validConfig = False

      if queries_src_ssl is None:
        config['queries-src.ssl'] = 'false'
        queries_src_ssl = 'false'
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration default: setting queries-src.ssl to false")
      elif queries_src_ssl.lower() not in ['true', 'false', '1', '0']:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: queries_src_ssl is '" + queries_src_ssl  + "', valid values are 'true', 'false', '1', or '0'")
        validConfig = False

      if queries_src_ssl.lower() in ['true', '1']:
        if queries_src_username is None:
          logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries-src.username is required")
          validConfig = False

        if queries_src_password is None:
          logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries-src.password is required")
          validConfig = False

      if queries_src_queries_table is None:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries-src.insights-queries-table is required")
        validConfig = False

      if queries_starttime is None:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries.startTime is required")
        validConfig = False

      if queries_endtime is None:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries.endTime is required")
        validConfig = False

      try:
        res = datetime.strptime(queries_starttime, "%Y-%m-%d %H:%M:%S %Z")
      except ValueError:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: queries.startTime is not a valid timestamp")
        validConfig = False

      try:
        res = datetime.strptime(queries_endtime, "%Y-%m-%d %H:%M:%S %Z")
      except ValueError:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: queries.endTime is not a valid timestamp")
        validConfig = False

      if queries_src_filename is not None:
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration ignored: queries-src.filename will not be used")

    if queries_dst_host == None:
      logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries-dst.host is required")
      validConfig = False

    if queries_dst_port is None:
      config['queries-dst.port'] = '8080'
      logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration default: setting queries-dst.port to 8080")
    elif not queries_dst_port.isnumeric():
      logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: queries-dst.port '" + queries_dst_port  + "' is not a number")
      validConfig = False

    if queries_dst_ssl is None:
      config['queries-dst.ssl'] = 'false'
      queries_dst_ssl = 'false'
      logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration default: setting queries-dst.ssl to false")
    elif queries_dst_ssl.lower() not in ['true', 'false', '1', '0']:
      logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: queries_dst_ssl is '" + queries_dst_ssl  + "', valid values are 'true', 'false', '1', or '0'")
      validConfig = False

    if queries_dst_ssl.lower() in ['true', '1']:
      if queries_dst_username is None:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries-dst.username is required")
        validConfig = False

      if queries_dst_password is None:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration missing: queries-dst.password is required")
        validConfig = False

    if queries_dst_unique_connection_per_query is not None:
      if queries_dst_unique_connection_per_query.lower() not in ['true', 'false', '1', '0']:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: queries-dst.unique-connection-per-query is '" + queries_dst_unique_connection_per_query + "', valid values are 'true', 'false', '1', or '0'")
        validConfig = False
    else:
      config['queries-dst.unique-connection-per-query'] = 'false'
      queries_dst_unique_connection_per_query = 'false'
      logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration default: setting queries-dst.unique-connection-per-query to false")

    if queries_dst_impersonate_query_user is not None:
      if queries_dst_impersonate_query_user.lower() not in ['true', 'false', '1', '0']:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: queries-dst.impersonate-query-user is '" + queries_dst_impersonate_query_user + "', valid values are 'true', 'false', '1', or '0'")
        validConfig = False
      elif queries_dst_unique_connection_per_query.lower() in ['false', '0']:
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration ignored: queries-dst.impersonate-query-user will not be used")
    else:
      config['queries-dst.impersonate-query-user'] = 'false'
      logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration default: setting queries-dst.impersonate-query-user to false")

    if queries_run_sequentially is not None:
      if queries_run_sequentially.lower() not in ['true', 'false', '1', '0']:
        logging.error(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration invalid: queries.run-sequentially is '" + queries_run_sequentially + "', valid values are 'true', 'false', '1', or '0'")
        validConfig = False
    else:
      config['queries.run-sequentially'] = 'false'
      logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - configuration default: setting queries.run-sequentially to false")

    if not validConfig:
      os._exit(1)

  def __addConnection(self, name: str, _host: str, _port: int, _username: str, _user: str, _catalog: str, _schema: str, _password: str = None, _https: bool = False):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - adding connection: " + name)
    http_scheme = 'https' if _https else 'http'
    conn = None
    if _password:
      conn = trino.dbapi.connect(
        host=_host,
        port=_port,
        user=_user,
        catalog=_catalog,
        schema=_schema,
        http_scheme=http_scheme,
        auth=trino.auth.BasicAuthentication(_username, _password))
    else:
      conn = trino.dbapi.connect(
        host=_host,
        port=_port,
        user=_user,
        catalog=_catalog,
        schema=_schema,
        http_scheme=http_scheme)
    self.connections[name] = conn
    return conn

  def __closeConnection(self, name: str):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - closing connection: " + name)
    conn = self.connections.get(name)
    conn.close()

  def __getConnection(self, name: str):
    return self.connections[name]

  def __closeConnections(self):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - closing connections")
    for conn in self.connections.values():
      conn.close()

  def __retrieveQueryHistoryFromSEP(self, table: str, startTime: str, endTime: str, conn: str = 'src'):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - retrieving query history")
    cur = self.__getConnection(conn).cursor()
    cur.execute(
      "SELECT query_id, catalog, schema, principal, usr, query, query_type, to_unixtime(create_time) as create_time, end_time " +
      "FROM " + table + " " +
      "WHERE create_time >= timestamp '" + startTime + "' " +
      "AND create_time <= timestamp '" + endTime + "' " +
      "AND query_type = 'SELECT' " +
      "ORDER BY create_time")
    self.queries = cur.fetchall()
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - " + str(len(self.queries)) + " queries to be run")

  def __retrieveQueryHistoryFromCSVFile(self, filename: str):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - loading query history from CSV file " + filename)
    with open(filename, mode='r') as csv_file:
      self.queries = list(csv.DictReader(csv_file))
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - " + str(len(self.queries)) + " queries to be run")

  def __retrieveQueryHistoryFromFile(self, _filename: str, _delimiter: str):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - loading query history from file " + _filename)
    with open(_filename, mode='r') as txt_file:
      self.queries = list(csv.DictReader(txt_file, delimiter=_delimiter))
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - " + str(len(self.queries)) + " queries to be run")

  def __run_query(self, threadName: str, catalog: str, schema: str, query: str, user: str, runtime: float, config: dict, blackholeSchema: str):
    logging.info(threadName + ": " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - run query thread started")

    uniqueConn = config.get('queries-dst.unique-connection-per-query').lower() in ['true', '1']

    conn = None
    if RUNNING:
      if uniqueConn:
        conn = self.__addConnection(
          name=threadName,
          _host=config.get('queries-dst.host'),
          _port=int(config.get('queries-dst.port')),
          _username=config.get('queries-dst.username'),
          _user=user if config.get('queries-dst.impersonate-query-user').lower() in ['true', '1'] else config.get('queries-dst.username'),
          _catalog=catalog,
          _schema=schema,
          _password=config.get('queries-dst.password'),
          _https=config.get('queries-dst.ssl').lower() in ['true', '1'])
      else:
        conn = self.__getConnection('dst')

      cur = conn.cursor()

    while RUNNING and datetime.timestamp(datetime.now()) < runtime:
      pass

    if RUNNING:
      if not uniqueConn and catalog:
        logging.info(threadName + ": " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - setting catalog '" + catalog + "' and schema '" + schema + "'" )
        if not schema:
          schema = 'system'
        cur.execute('USE ' + catalog + '.' + schema)

      if blackholeSchema:
        query = 'CREATE TABLE ' + blackholeSchema + '.' + threadName.replace(' ', '') + ' AS \n' + query

    if RUNNING:
      logging.info(threadName + ": " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - running query")
      self.__addRunningQuery(name=threadName, cur=cur)
      try:
        cur.execute(query)
      except Exception as error:
        logging.error(error)

      while True:
        if not RUNNING:
          logging.info(threadName + ": " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - cancelling query")
          cur.close()
          break
        rows = cur.fetchmany(1000)
        if not rows:
          break

    if RUNNING:
      logging.info(threadName + ": " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - query complete")
      cur.close()

    if uniqueConn and conn is not None:
      self.__closeConnection(threadName)

  def __runQueries(self, config: dict):
    try:
      now = datetime.timestamp(datetime.now()) + 30
      gap = 0

      blackholeSchema = None
      if config.get('queries-dst.blackhole-catalog'):
        blackholeSchema = config.get('queries-dst.blackhole-catalog') + "." + "test_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - create blackhole schema: " + blackholeSchema)
        cur = self.__getConnection('dst').cursor()
        cur.execute('CREATE SCHEMA ' + blackholeSchema)

      threads = list()
      runQueriesSequentially = False
      if config.get('queries.run-sequentially'):
        runQueriesSequentially = config.get('queries.run-sequentially').lower() in ['true', '1']

      i = 0
      for query in self.queries:
        if isinstance(query, list):
          catalog = query[1]
          schema = query[2]
          queryText = query[5]
          user = query[4]
          createTime = query[7]

        if isinstance(query, dict):
          catalog = query['catalog']
          schema = query['schema']
          queryText = query['query'].replace(r'\n', '\n')
          user = query['usr']
          createTime = float(query['create_time'])

        if (i == 0):
          first = createTime
        gap = createTime - first
        i = i + 1

        if RUNNING:
          if runQueriesSequentially:
            self.__run_query('Query ' + str(i), catalog, schema, queryText, user, datetime.timestamp(datetime.now()), config, blackholeSchema)
          else:
            t = threading.Thread(target=self.__run_query, kwargs={'threadName': 'Thread ' + str(i), 'catalog': catalog, 'schema': schema, 'query': queryText, 'user': user, 'runtime': + now + gap, 'config': config, 'blackholeSchema': blackholeSchema})
            threads.append(t)
            t.start()

      if not runQueriesSequentially:
        for index, thread in enumerate(threads):
          thread.join()

    except Exception as error:
      logging.error(error)

  def __addRunningQuery(self, name: str, cur: Cursor):
    logging.debug(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - adding running query: " + name)
    self.running_queries[name] = cur

  def __removeRunningQuery(self, name: str):
    logging.debug(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - remove running query: " + name)
    self.running_queries.pop(name)

  def __cancelAllRunningQueries(self):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - cancelling all running queries")
    for cur in self.running_queries.values():
      try:
        cur.close()
      except:
        pass

  def main(self):
    config = self.__loadConfig()

    self.__validateConfig(config)

    self.using_blackhole_catalog = config.get('queries-dst.blackhole-catalog') is not None

    signal.signal(signal.SIGINT, self.__interrupt_handler)

    src = config.get('queries-src.type')

    if RUNNING and src == 'sep':
      self.__addConnection(
        name='src',
        _host=config.get('queries-src.host'),
        _port=int(config.get('queries-src.port')),
        _username=config.get('queries-src.username'),
        _user=config.get('queries-src.username'),
        _catalog='system',
        _schema='runtime',
        _password=config.get('queries-src.password'),
        _https=config.get('queries-src.ssl').lower() in ['true', '1'])

      self.__retrieveQueryHistoryFromSEP(
        table=config.get('queries-src.insights-queries-table'),
        startTime=config.get('queries.startTime'),
        endTime=config.get('queries.endTime'),
        conn='src')

    if RUNNING and src == 'csv-file':
      self.__retrieveQueryHistoryFromFile(
        _filename=config.get('queries-src.filename'), _delimiter=',')

    if RUNNING and src == 'tsv-file':
      self.__retrieveQueryHistoryFromFile(
        _filename=config.get('queries-src.filename'), _delimiter='\t')

    if RUNNING and src == 'pipe-delimited-file':
      self.__retrieveQueryHistoryFromFile(
        _filename=config.get('queries-src.filename'), _delimiter='|')

    if RUNNING:
      self.__addConnection(
        name='dst',
        _host=config.get('queries-dst.host'),
        _port=int(config.get('queries-dst.port')),
        _username=config.get('queries-dst.username'),
        _user=config.get('queries-dst.username'),
        _catalog='system',
        _schema='runtime',
        _password=config.get('queries-dst.password'),
        _https=bool(config.get('queries-dst.ssl')))

    if RUNNING:
      self.__runQueries(config)

    self.__closeConnections()

if __name__ == "__main__":
  RUNNING=True
  qr = QueryReplay()
  qr.main()
