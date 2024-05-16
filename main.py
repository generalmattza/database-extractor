#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-05-16
# ---------------------------------------------------------------------------
"""Some demonstration code for querying a database and returning the results as a dataframe"""
# ---------------------------------------------------------------------------


from datetime import datetime
from logging.config import dictConfig

from database_extractor import (
    load_config,
    create_influxdb_client,
    DataExtractorQueryConfig,
    query_database,
)


def setup_logging(filepath="config/logger.yaml"):
    import yaml
    from pathlib import Path

    if Path(filepath).exists():
        with open(filepath, "r") as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)
    else:
        raise FileNotFoundError
    Path("logs/").mkdir(exist_ok=True)
    dictConfig(config)


def main():

    # Fetch application configuration from file
    application_config = load_config("config/application.toml")

    # Create a client to interact with the InfluxDB database
    # Configuration is fetched from the .influxdb.toml file
    database_client = create_influxdb_client("config/.influxdb.toml")

    # Create a query configuration object to parse the configuration file
    query_config = DataExtractorQueryConfig(**application_config["query"])

    # This is the base time for the query, it can be set to the current time in the local timezone
    # The appropriate timezone offset is set in the application configuration
    # and applied in the query_database function
    query_time = datetime.now()

    # Query the database, and return a Pandas DataFrame object
    result = query_database(
        client=database_client,
        query_time=query_time,
        **query_config,
    )

    # Print the first 10 rows of the result
    print(result.head(10))


if __name__ == "__main__":
    setup_logging()
    main()
