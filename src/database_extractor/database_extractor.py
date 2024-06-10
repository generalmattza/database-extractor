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
import pandas as pd

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
    delta_time_start: DeltaTime = None
    delta_time_end: DeltaTime = None
    tz_offset: int = 0
    bucket: str = ""
    columns_to_drop: list[str] = None
    filter: str = 'r["_measurement"] =~ /.*/'
    column_key: str = "id"
    aggregate_function: str = "last"
    aggregate_window: str = "1s"
    sort_by: list[str] = None

    def __post_init__(self):
        if self.delta_time_start is None:
            self.delta_time_start = DeltaTime()
        if self.delta_time_end is None:
            self.delta_time_end = DeltaTime()
        if self.sort_by is None:
            self.sort_by = ["_time", "_field"]

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


def list_to_fstring(str_list):
    """
    Convert a list of strings to a formatted string in the format ["str1", "str2"].

    :param str_list: List of strings.
    :return: Formatted string.
    """
    formatted_elements = [f'"{s}"' for s in str_list]
    return f'[{", ".join(formatted_elements)}]'


def query_database(
    client,
    bucket,
    query_time,
    delta_time_start,
    delta_time_end,
    columns_to_drop=None,
    filter='r["_measurement"] =~ /.*/',
    column_key="id",
    tz_offset=0,
    time_format=DEFAULT_TIME_FORMAT,
    aggregate_function="last",
    aggregate_window="1s",
    sort_by=["_time", "_field"],
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
    |> aggregateWindow(every: {aggregate_window}, fn: {aggregate_function}, createEmpty: false)
    |> pivot(rowKey:["_time"], columnKey: ["{column_key}"], valueColumn: "_value")
    |> group()
    |> sort(columns: {list_to_fstring(sort_by)})
    """
    print(list_to_fstring(sort_by))
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
    if result is not None:
        query_total_time = time.perf_counter() - query_start_time
        logger.info(
            f"Query returned table of size {result.shape[0]} rows x {result.shape[1]} columns in {query_total_time:.2f}s",
            extra={"result.shape": result.shape},
        )
        result = drop_columns(result, columns_to_drop)
    
    return result


def drop_columns(df: pd.DataFrame, columns_to_drop: list[str]) -> pd.DataFrame:
    columns_exist = []
    for column in columns_to_drop:
        if column in df.columns:
            columns_exist.append(column)
    df.drop(columns = columns_exist, inplace = True)
    return df


def process_results(df: pd.DataFrame, current_date: datetime) -> None:
    # df is empty
    if df.size == 0:
        logger.info(f"No data for {current_date.year}-{current_date.month:02d}-{current_date.day:02d}.")
        return
    # df has less than 10 rows
    if df.shape[0] < 10:
        logger.info(f"Less than 10 rows for {current_date.year}-{current_date.month:02d}-{current_date.day:02d}; Ignoring results.")
        return

    # Do something with the result
    df = df.set_index("_time")
    # df = df.resample(rule = "1s").mean()
    df = df.dropna(axis = 0, how = "all")
    try:
        df.to_csv(f"/srv/data/influx/prototype-zero_realtime-data_{current_date.year}-{current_date.month:02d}-{current_date.day:02d}_mqtt.csv")
        # df.to_csv(f"/nfs/research/gfyvrdatadash/influx/prototype-zero_realtime-data_{current_date.year}-{current_date.month:02d}-{current_date.day:02d}_mqtt.csv")
    except Exception as error:
        logger.error(f"{error}")
    else:
        logger.info(f"csv created for {current_date.year}-{current_date.month:02d}-{current_date.day:02d}.")


def timezone_offset(current_date: datetime) -> int:
    # NOTE: This currently is only set up for 2024
    DST_start = datetime(2024, 3, 10, 2)
    DST_end = datetime(2024, 11, 3, 1)
    
    if (current_date - DST_start).total_seconds() > 0 and (DST_end - current_date).total_seconds() > 0:
        return -7
    else:
        return -8
    

def query_data_for_day(client: FastInfluxDBClient, current_date: datetime) -> None:
    time_fmt = "%Y-%m-%dT%H:%M:%SZ"
    query_time = current_date.strftime(time_fmt)
    tz_offset = timezone_offset(current_date)

    drop_list = ["result", "table", "_start",
                 "_stop", "_measurement", "datatype",
                 "_field", "_measurement", "category",
                 "level", "machine", "module", "display_name"]

    query_config = dict(
        bucket = 'prototype-zero',
        time_format = time_fmt,
        delta_time_start = [0, 0, 0, 0],
        delta_time_end = [0, 24, 0, 0],
        tz_offset = tz_offset,
        columns_to_drop = drop_list,
        filter = 'r["id"] =~ /.*/',
        # filter = 'r["_measurement"] == "liner_heater"'
        sort_by = ['_time'],
        column_key = 'id',
    )

    # Query the database, and return a Pandas DataFrame object
    result = query_database(
        client=client,
        query_time=query_time,
        **query_config,
    )

    process_results(result, current_date)


def query_data_for_range(client: FastInfluxDBClient, start_date: datetime, end_date: datetime) -> None:
    # NOTE: Watch for leap year
    days_in_each_month = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    up_to_date = False
    for (month, days) in enumerate(days_in_each_month):
        if start_date.month > month:
            continue
        if not up_to_date:
            for j in range(days):
                if start_date.day > days:
                    continue
                if (end_date.month == month+1) and (end_date.day == j+1):
                    logger.info("Reached end date.")
                    up_to_date = True
                    break
                date = datetime(2024,month+1,j+1)
                query_data_for_day(client, date)
        else:
            break
