#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-05-16
# ---------------------------------------------------------------------------
"""
A module for extracting data from a database and returning it as a dataframe
Currently InfluxDB is supported, but other database types could be added
"""
# ---------------------------------------------------------------------------
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from pathlib import Path
import time
from typing import Union
from collections.abc import Mapping

from fast_database_clients import FastInfluxDBClient


logger = logging.getLogger(__name__)

DEFAULT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class DeltaTime(Mapping):
    """
    A class to represent a time delta
    """

    time_format = DEFAULT_TIME_FORMAT

    def __init__(
        self, days: int = 0, hours: int = 0, minutes: int = 0, seconds: int = 0
    ):
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds

    def to_timedelta(self) -> timedelta:
        return timedelta(
            days=self.days, hours=self.hours, minutes=self.minutes, seconds=self.seconds
        )

    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        raise KeyError(f"{key} not found in DeltaTime")

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __add__(self, other: timedelta):
        if isinstance(other, timedelta):
            return self.to_timedelta() + other
        elif isinstance(other, str):
            return self.to_timedelta() + datetime.strptime(other, self.time_format)
        elif isinstance(other, datetime):
            return other + self.to_timedelta()
        elif isinstance(other, DeltaTime):
            return self.to_timedelta() + other.to_timedelta()
        else:
            raise TypeError("Unsupported type for addition")

    def __radd__(self, other: timedelta):
        return self.__add__(other)

    def __sub__(self, other):
        if isinstance(other, timedelta):
            return self.to_timedelta() - other
        elif isinstance(other, str):
            return self.to_timedelta() - datetime.strptime(other, self.time_format)
        elif isinstance(other, datetime):
            return other - self.to_timedelta()
        elif isinstance(other, DeltaTime):
            return self.to_timedelta() - other.to_timedelta()
        else:
            raise TypeError("Unsupported type for subtraction")

    def __rsub__(self, other):
        return self.__sub__(other)


def load_config(filepath: Union[str, Path]) -> dict:
    """
    Load a configuration file from a file path
    :param filepath: The path to the configuration file
    :return: The configuration as a dictionary
    """
    if isinstance(filepath, str):
        filepath = Path(filepath)

    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # if extension is .json
    if filepath.suffix == ".json":
        import json

        with open(filepath, "r") as file:
            return json.load(file)

    # if extension is .yaml
    if filepath.suffix == ".yaml":
        import yaml

        with open(filepath, "r") as file:
            return yaml.safe_load(file)
    # if extension is .toml
    if filepath.suffix == ".toml":
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib

        with open(filepath, "rb") as file:
            return tomllib.load(file)

    # else load as binary
    with open(filepath, "rb") as file:
        return file.read()


@dataclass
class DataExtractorQueryConfig(Mapping):
    """
    A class to represent the configuration for a data extractor
    """

    time_format: str = DEFAULT_TIME_FORMAT
    delta_time_start: DeltaTime = DeltaTime()
    delta_time_end: DeltaTime = DeltaTime()
    tz_offset: int = 0
    bucket: str = ""
    columns_to_drop: list[str] = None
    filter: str = 'r["_measurement"] =~ /.*/'

    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        raise KeyError(f"{key} not found in DataExtractorQueryConfig")

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __repr__(self):
        return f"DataExtractorQueryConfig({self.__dict__})"


def shift_string_time(
    time_string: str,
    delta_time: Union[DeltaTime, int] = None,
    timeformat=DEFAULT_TIME_FORMAT,
) -> str:
    if delta_time is None or delta_time == 0:
        return time_string
    if isinstance(delta_time, int):
        delta_time = DeltaTime(hours=delta_time)

    return (datetime.strptime(time_string, timeformat) + delta_time).strftime(
        timeformat
    )


def create_influxdb_client(config_path: str) -> FastInfluxDBClient:
    """
    Create an InfluxDB client from a configuration file
    :param config_path: The path to the configuration file
    :return: The InfluxDB client
    """
    client = FastInfluxDBClient.from_config_file(config_path)

    if client.ping():
        logger.info(
            f"Connected to InfluxDB at url:{client._client.url}, org:{client._client.org}",
            extra={"config_path": config_path, "client": client},
        )
        return client
    else:
        logger.error(
            "Could not connect to InfluxDB", extra={"config_path": config_path}
        )
        raise ConnectionError("Could not connect to InfluxDB")


def construct_query_time_endpoints(
    query_time: Union[datetime, str],
    delta_time_start: Union[DeltaTime, tuple, list],
    delta_time_end: Union[DeltaTime, tuple, list],
    tz_offset: int = 0,
    time_format: str = DEFAULT_TIME_FORMAT,
) -> tuple[str, str]:
    """
    Construct the start and end time for a query
    :param query_time: The time to query around
    :param delta_time_start: The time delta to subtract from the query time
    :param delta_time_end: The time delta to add to the query time
    :param tz_offset: The timezone offset in hours
    :param time_format: The time format to return
    :return: The start and end time as strings in utc time format
    """

    if isinstance(delta_time_start, (tuple, list)):
        delta_time_start = DeltaTime(*delta_time_start)
    if isinstance(delta_time_end, (tuple, list)):
        delta_time_end = DeltaTime(*delta_time_end)
    if isinstance(query_time, str):
        query_time = datetime.strptime(query_time, time_format)

    tz_offset = timedelta(hours=tz_offset)

    start_time_utc = (query_time + delta_time_start - tz_offset).strftime(time_format)
    end_time_utc = (query_time + delta_time_end - tz_offset).strftime(time_format)

    return start_time_utc, end_time_utc


def query_database(
    client,
    bucket,
    query_time,
    delta_time_start,
    delta_time_end,
    columns_to_drop=None,
    filter='r["_measurement"] =~ /.*/',
    tz_offset=0,
    time_format=DEFAULT_TIME_FORMAT,
):
    """
    Query a database and return the results as a dataframe
    :param client: The database client
    :param bucket: The bucket to query
    :param query_time: The time to query around
    :param delta_time_start: The time delta to subtract from the query time
    :param delta_time_end: The time delta to add to the query time
    :param columns_to_drop: Columns to drop from the resulting dataframe
    :param filter: A filter to be applied to the returned data
    :param tz_offset: The timezone offset in hours
    :param time_format: The time format to return
    :return: The query result as a dataframe
    """
    # Construct the endpoints of the query time using the specified time deltas
    # An optional timezone offset can be applied to the query time
    start_time_utc, end_time_utc = construct_query_time_endpoints(
        query_time,
        delta_time_start,
        delta_time_end,
        tz_offset=tz_offset,
        time_format=time_format,
    )

    # Construct the Flux query. This is a simple query that selects all fields from the specified bucket
    # Returned data is timeshifted to account for the timezone offset
    query = f"""from(bucket: "{bucket}")
    |> range(start: {start_time_utc}, stop: {end_time_utc})
    |> timeShift(duration: {tz_offset}h)
    |> filter(fn: (r) => {filter})
    |> pivot(rowKey:["_time"], columnKey: ["id"], valueColumn: "_value")
    |> group()
    """
    start_time_local = shift_string_time(start_time_utc, tz_offset)
    end_time_local = shift_string_time(end_time_utc, tz_offset)
    logger.info(
        f"Querying {client}, bucket:{bucket}, query_time:{start_time_local} to {end_time_local}",
        extra={
            "bucket": bucket,
            "start_time": start_time_utc,
            "end_time": end_time_utc,
            "query": query,
        },
    )

    query_start_time = time.perf_counter()

    result = client.query_dataframe(query)

    if columns_to_drop:
        try:
            result = result.drop(columns=columns_to_drop)
            logger.info(
                f"Dropped columns from dataframe: {columns_to_drop}",
                extra={"columns_to_drop": columns_to_drop},
            )
        except KeyError as e:
            logger.error(
                f"Failed to drop columns from dataframe: {e.args[0]}",
                extra={"columns_to_drop": columns_to_drop},
            )

    query_end_time = time.perf_counter()
    query_total_time = query_end_time - query_start_time

    if result is not None:
        logger.info(
            f"Query returned table of size {result.shape[0]} rows x {result.shape[1]} columns in {query_total_time:.2f}s",
            extra={"result.shape": result.shape},
        )

        return result
