#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-05-16
# ---------------------------------------------------------------------------
"""Some demonstration code for querying a database and returning the results as a dataframe"""
# ---------------------------------------------------------------------------

from datetime import datetime, timedelta
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
    # Time format is set in the application configuration
    # The appropriate timezone offset is set in the application configuration
    # and applied in the query_database function
    # query_time = datetime.now()
    query_time = "2024-05-22T17:00:00Z"

    # Query the database, and return a Pandas DataFrame object
    result = query_database(
        client=database_client,
        query_time=query_time,
        **query_config,
    )

    # Do something with the result
    # print(result.head(10))
    result.to_pickle("./influxdb_live_1.pkl")


def generate_datetime_list(
    start_date: str, end_date: str, delta: timedelta, date_format="%Y-%m-%dT%H:%M:%SZ"
):
    """
    Generate a list of datetimes as strings in a given format.

    :param start_date: The start date as a string in the format 'YYYY-MM-DD HH:MM:SS'.
    :param end_date: The end date as a string in the format 'YYYY-MM-DD HH:MM:SS'.
    :param delta: A tuple of (days, hours, minutes, seconds) representing the time interval.
    :param date_format: The format in which to output the datetime strings.
    :return: A list of datetime strings in the specified format.
    """
    start = datetime.strptime(start_date, date_format)
    end = datetime.strptime(end_date, date_format)

    current = start
    datetimes = []

    while current <= end:
        datetimes.append(current.strftime(date_format))
        current += delta

    return datetimes


def extract_date(datetime_str):
    """
    Extract the date from a datetime string in the format 'YYYY-MM-DDTHH:MM:SSZ'.

    :param datetime_str: The datetime string.
    :return: The date part of the datetime string.
    """
    dt = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
    date_str = dt.strftime("%Y-%m-%d")
    return date_str


# Example usage:


def batched_data():
    # Fetch application configuration from file
    application_config = load_config("config/application.toml")

    # Create a client to interact with the InfluxDB database
    # Configuration is fetched from the .influxdb.toml file
    database_client = create_influxdb_client("config/.influxdb.toml")

    # Create a query configuration object to parse the configuration file
    query_config = DataExtractorQueryConfig(**application_config["query"])

    # List of dates to query
    # Query midnight to midnight for each day
    start_date = "2024-02-01T00:00:00Z"
    end_date = "2024-06-01T00:00:00Z"
    delta = timedelta(days=1, hours=0, minutes=0, seconds=0)  # Every 24 hours

    query_datetimes_list: list[datetime] = generate_datetime_list(
        start_date, end_date, delta
    )

    # set smallest number of lines, to prevent saving near-empty files
    data_threshold = 20

    # Query the database, and return a Pandas DataFrame object
    for query_time in query_datetimes_list:
        result = query_database(
            client=database_client,
            query_time=query_time,
            **query_config,
        )
        if len(result) >= data_threshold:
            result.to_csv(
                f"out/prototype-zero_realtime-data_{extract_date(query_time)}.csv"
            )


if __name__ == "__main__":
    setup_logging()
    main()
    # batched_data()
