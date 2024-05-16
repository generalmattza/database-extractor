# database-extractor
### A module for extracting data from a database

This module facilitates the extraction of data from a database and returning it as a Pandas dataframe.
Currently only Influx database queries are supported, but other database types could be added.

```python
from database_extractor import (
    create_influxdb_client,
    query_database,
)

# Create a client to interact with the InfluxDB database
# Configuration is fetched from a .toml file
database_client = create_influxdb_client("config/.influxdb.toml")

query_config = dict(
    bucket = 'prototype-zero',
    time_format = "%Y-%m-%dT%H:%M:%SZ",
    delta_time_start = [0, -1, 0, 0],
    delta_time_end = [0, 1, 0, 0],
    tz_offset = -8,
    columns_to_drop = ["result", "table", "_start", "_stop"],
    filter = 'r["id"] =~ /.*/',
)

# This is the base time for the query in the local timezone
# The offsets specified in query_config is applied to this base time
query_time = "2024-05-15T17:00:00Z"

# Query the database, and return a Pandas DataFrame object
result = query_database(
    client=database_client,
    query_time=query_time,
    **query_config,
)

# Do something with the result
print(result.head(10))

```
