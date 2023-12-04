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
    self.connections = {};
    self.queries = None
    logging.getLogger().setLevel(logging.INFO)

  def __loadConfig(self):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - loading configuration")
    configs = Properties()
    with open('config.properties', 'rb') as read_prop:
      configs.load(read_prop)

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

  def __validateConfig(self):
    return True

  def __addConnection(self, name: str, _host: str, _port: int, _username: str, _user: str, _catalog: str, _schema: str, _password: str = None, _https: bool = False):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - adding connection: " + name)
    http_scheme = 'https' if _https else 'http'
    conn = None;
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

  def __removeConnection(self, name: str):
    logging.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - closing connection: " + name)
    conn = self.connections.pop(name)
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

    uniqueConn = bool(config.get('queries-dst.unique-connection-per-query'))

    conn = None
    if uniqueConn:
      conn = self.__addConnection(
        name=threadName,
        _host=config.get('queries-dst.host'), 
        _port=int(config.get('queries-dst.port')), 
        _username=config.get('queries-dst.username'), 
        _user=user if bool(config.get('queries-dst.impersonate-query-user')) else config.get('queries-dst.username'),
        _catalog=catalog,
        _schema=schema,
        _password=config.get('queries-dst.password'), 
        _https=bool(config.get('queries-dst.ssl')))
    else:
      conn = self.__getConnection('dst')
    
    cur = conn.cursor()

    while datetime.timestamp(datetime.now()) < runtime:
      pass 

    if not uniqueConn and catalog:
      logging.info(threadName + ": " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - setting catalog '" + catalog + "' and schema '" + schema + "'" )
      if not schema:
        schema = 'system'
      cur.execute('USE ' + catalog + '.' + schema)

    if blackholeSchema:
      query = 'CREATE TABLE ' + blackholeSchema + '.' + threadName.replace(' ', '') + ' AS \n' + query

    logging.info(threadName + ": " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - running query")
    cur.execute(query)
    
    while True:
      rows = cur.fetchmany(1000)
      if not rows:
        break

    logging.info(threadName + ": " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - query complete")
    cur.close()

    if uniqueConn:
      self.__removeConnection(threadName)

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
        runQueriesSequentially = bool(config.get('queries.run-sequentially'))

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

        if runQueriesSequentially:
          self.__run_query('Query # ' + str(i), catalog, schema, queryText, user, datetime.timestamp(datetime.now()), config, blackholeSchema)
        else:
          t = threading.Thread(target=self.__run_query, kwargs={'threadName': 'Thread ' + str(i), 'catalog': catalog, 'schema': schema, 'query': queryText, 'user': user, 'runtime': + now + gap, 'config': config, 'blackholeSchema': blackholeSchema}) 
          threads.append(t)
          t.start()

      if not runQueriesSequentially:
        for index, thread in enumerate(threads):
          thread.join()

    except Exception as error:
      logging.error(error)

  def main(self):
    config = self.__loadConfig()

    self.__validateConfig()
    
    src = config.get('queries-src.type')

    if src == 'sep':
      self.__addConnection(
        name='src',
        _host=config.get('queries-src.host'), 
        _port=int(config.get('queries-src.port')), 
        _username=config.get('queries-src.username'), 
        _user=config.get('queries-src.username'),
        _catalog='system',
        _schema='runtime',
        _password=config.get('queries-src.password'), 
        _https=bool(config.get('queries-src.ssl')))

      self.__retrieveQueryHistoryFromSEP(
        table=config.get('queries-src.insights-queries-table'),
        startTime=config.get('queries.startTime'),
        endTime=config.get('queries.endTime'),
        conn='src')

    if src == 'csv-file':
      self.__retrieveQueryHistoryFromFile(
        _filename=config.get('queries-src.filename'), _delimiter=',')

    if src == 'tsv-file':
      self.__retrieveQueryHistoryFromFile(
        _filename=config.get('queries-src.filename'), _delimiter='\t')

    if src == 'pipe-delimited-file':
      self.__retrieveQueryHistoryFromFile(
        _filename=config.get('queries-src.filename'), _delimiter='|')

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

    self.__runQueries(config)

    self.__closeConnections()

if __name__ == "__main__":
  qr = QueryReplay()
  qr.main()