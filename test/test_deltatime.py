from database_extractor import DeltaTime


def test_deltatime_unpacking():

    dt = DeltaTime(1, 2, 3, 4)

    days, hours, minutes, seconds = dt.values()

    assert days == 1
    assert hours == 2
    assert minutes == 3
    assert seconds == 4

    dt = DeltaTime(days=1, hours=2, minutes=3)

    days, hours, minutes, seconds = dt.values()

    assert days == 1
    assert hours == 2
    assert minutes == 3
    assert seconds == 0


def test_query_time_construct():

    time_format = "%Y-%m-%dT%H:%M:%SZ"
    delta_time_start = DeltaTime(0, -2, 0, 0)
    delta_time_end = DeltaTime(0, 1, 0, 0)

    query_time = "2024-05-16T10:00:00Z"

    start_time = (query_time + delta_time_start).strftime(time_format)
    end_time = (query_time + delta_time_end).strftime(time_format)

    assert start_time == "2024-05-16T08:00:00Z"
    assert end_time == "2024-05-16T11:00:00Z"


def test_construct_query_time_endpoints():
    from database_extractor import construct_query_time_endpoints

    query_time = "2024-05-16T10:00:00Z"
    delta_time_start = (0, -2, 0, 0)
    delta_time_end = (0, 1, 0, 0)
    time_start, time_end = construct_query_time_endpoints(
        query_time, delta_time_start, delta_time_end
    )
    assert time_start == "2024-05-16T08:00:00Z"
    assert time_end == "2024-05-16T11:00:00Z"


def test_create_query_endpoints_timezone():
    from database_extractor import construct_query_time_endpoints

    query_time = "2024-05-16T10:00:00Z"
    delta_time_start = (0, -2, 0, 0)
    delta_time_end = (0, 1, 0, 0)
    time_start, time_end = construct_query_time_endpoints(
        query_time, delta_time_start, delta_time_end, tz_offset=-8
    )
    assert time_start == "2024-05-16T00:00:00Z"
    assert time_end == "2024-05-16T03:00:00Z"
