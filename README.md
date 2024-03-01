# Query Replay

# THIS TOOL IS NOT OFFICIALLY SUPPORTED BY STARBURST DATA.  IT WAS CREATED BY OUR PROFESSIONAL SERVICES TEAM TO AID WITH SPECIFIC USE CASES.

[QueryReplay.py](https://github.com/starburstdata/query-replay/blob/main/QueryReplay.py) is a python script that can replay queries with the original execution cadence.  The replayer uses threads to time query submission in an attempt to emulate the original query concurrency.  It uses the [trino-python-client](https://github.com/trinodb/trino-python-client) to submit the queries to a SEP/Galaxy/Trino cluster and optionally also retrieve the query history from the same or different SEP/Galaxy/Trino cluster.  

Queries can be replayed for troubleshooting to attempt to replicate an issue that only seems to happen with specific cluster load.  It can also be a way to test new cluster configuration on a secondary cluster before rolling changes out to production.

The queries to be replayed can be retrieved from a source cluster that has a catalog configured to access an Insights database, or a copy of the completed_queries table made available in some other catalog.  The queries can also be provided in a CSV, TSV, or pipe-delimited file.  

This is the query used to retrieve the queries to be replayed when the source is set to SEP. If query history is provided with a delimited file the same columns are expected.  See example [CSV](https://github.com/starburstdata/query-replay/blob/main/sample_query_history_comma_delimited.csv), [TSV](https://github.com/starburstdata/query-replay/blob/main/sample_query_history_tab_delimited.tsv), and [pipe-delimited](https://github.com/starburstdata/query-replay/blob/main/sample_query_history_pipe_delimited.txt) files.
```console
SELECT
  query_id, catalog, schema, principal, usr, query, query_type,
  to_unixtime(create_time) as create_time, end_time
FROM <completed_queries>
WHERE create_time >= timestamp '<startTime>'
  AND create_time <= timestamp '<endTime>'
  AND query_type = 'SELECT'
ORDER BY create_time
```

**IMPORTANT: As you can see the query above limits queries to SELECT statements.  There is no additional filtering or validation of the SQL statement type when queries are provided via file.  It is the responsibility of the user running the script to provide only statements that are ok to be rerun.  It is fine to include CREATE, UPDATE, and DELETE statements as long as it is intended and the outcome is understood.  Also, those kinds of statements are not compatible with using the blackhole catalog option, see more below.**

## Example Usage
```console
python queryReplay.py
```

## Requirements
- Python3
- Python packages: trino (trino-python-client), jproperties, rich.
- completed_queries table queryable from source SEP cluster or queries list in CSV, TSV, or pipe-delimited file.
- The credentials for the source cluster should be allowed to query the completed_queries table.
- The credentials for the destination cluster should be able to run queries or impersonate the users that ran the queries originally.
- Suggestion: Increase ulimit openfiles to a high number in the client running the QueryReplay.py script.

## Configuration
All configuration is read from a [config.properties](https://github.com/starburstdata/query-replay/blob/main/config.properties) file that should be in the same directory as the QueryReplay.py script.  See the example configuration provided.

## Authentication
The replayer supports username/password authentication.  The credentials used for the destination cluster should be able to run queries or impersonate the users that ran the queries originally.

## Unique connection per query
Queries can be executed using a single connection, however, this could potentially cause some queries to fail if two or more queries that use not-fully-qualified-tables are executed at the exact same time.  When using a single connection, the catalog/schema is set before running the actual query using the statement `USE <catalog>.<schema>;` if values for catalog and/or schema are provided in the query history.  The default catalog and schema are set at the **session level**.  This could be an issue with queries running at exact same time with different default catalog and schema values.  One query could overwrite the other one's session settings before it executes which would likely fail the query.

## Impersonate query user
Queries can be run using the cluster destination credentials or as the original user that submitted the query by enabling user impersonation `queries-dst.impersonate-query-user=True`.  Either way the credentials provided must have enough privilege.

## Blackhole catalog
It could be possible for the script to fail due to memory issues in the client running the replayer if there is very high query concurrency and queries retrieve large amounts of data.  An option is to enable the blackhole catalog in the destination cluster.  Queries would be executed as `CREATE TABLE blackhole.test_YYYYMMDD_HHMMSS.Thread## AS <query>` so no data is streamed to the client running the replayer.  No data is actually written anywhere but a schema and test tables are created (just in memory metadata).  See more information on the [Blackhole connector](https://docs.starburst.io/latest/connector/blackhole.html).

One known issue is that queries with non-unique column names will fail when using the blackhole catalog.  While `SELECT 1 AS col1, 2 AS col1` is a valid query that will succeed, `CREATE TABLE test AS SELECT 1 AS col1, 2 AS col1` will fail because the column names are not unique.

## Run queries sequentially
Optionally queries can be run sequentially if desired `queries.run-sequentially=True`, in the order that they are provided.  Each query will be executed once the previous one completes or fails.

## Interruption handling
Ctrl+C can be used to stop execution at any time.  In-flight queries will be canceled.
