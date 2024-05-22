# database-extractor
### A module for extracting data from a database

This module is a client to facilitate the extraction of data from a database and returns it as a Pandas dataframe.
Currently only Influx database queries are supported, but other database types could be added.

## Installation Process
Clone the repo
> git clone https://github.com/generalmattza/database-extractor

Create a new virtual environment
> cd database-extractor
> python -m venv .venv
> .venv/bin/activate.ps1 #assuming powershell terminal

Install dependancies
> pip install .

Rename the influx configuration file
> mv config/.influxdb.toml-default config/.influxdb.toml

You can then update the .influxdb.toml file to your desired influxdb server settings.
The main settings that are required to connect are <i>url, token, org</i> and <i>default_bucket</i>

You can test the client configuration as follows:
```python
database_client = create_influxdb_client("config/.influxdb.toml")
database_client.ping() # Returns True if all is well
```

## Example Usage
In this example, the configuration is set in a dict and passed to the query_database method.

See the [main.py](https://github.com/generalmattza/database-extractor/blob/main/main.py) file for an implementation that is configured using the configuration files in <i>config/*</i>

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
