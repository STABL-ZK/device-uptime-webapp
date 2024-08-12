# imports
from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta
import pandas as pd

# disable influx piviot warnings
import warnings
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction)

# constants
with open("./token","r") as token_src:
    API_TOKEN = token_src.read()
    
HOST = "https://eu-central-1-1.aws.cloud2.influxdata.com"
ORG = "cloud@stabl.com"
BUCKET = "sbc-prod"

# Interval size used to window the system_state measurements.
# Due to imperfect precision in timing in the data, this makes
# the data density predictable, which is needed for uptime calculation
WINDOW_INTERVAL_IN_SECONDS = 4


def get_uptimes_of_last_week_until_now() -> dict:
    """Produces the uptime of each device between *now* and exactly one week ago"""
    # One week in seconds
    time_interval = 60*60*24*7
    query = f"""
        from(bucket: "{BUCKET}")
        |> range(start: -{time_interval}s)
        |> filter(fn: (r) => r._measurement == "telemetry_v2")
        |> filter(fn: (r) => r._field == "system_state")
        |> aggregateWindow(every: {WINDOW_INTERVAL_IN_SECONDS}s, fn: first)
        |> filter(fn: (r) => r._value != "ERROR" or r._value != "STANDBY")
        |> group(columns: ["device_id"])
        |> count()
        |> group()
    """
    with InfluxDBClient(HOST, API_TOKEN, timeout=20000) as influx_db_client:
        client = influx_db_client.query_api()

        query_res = client.query_data_frame(query, org=ORG)
        query_res.insert(
            0,
            "uptime",
            value=[
                (i * WINDOW_INTERVAL_IN_SECONDS) / time_interval
                for i in query_res._value
            ],
        )
    res = {query_res.device_id[i]: query_res.uptime[i] for i in range(len(query_res))}
    res_df = pd.DataFrame(res.items(),columns=["device_id","uptime"])
    res_df.insert(1, "uptime_readable", ["{:.2f}%".format(100*t) for t in res_df.uptime])
    return res, res_df, datetime.now(), (datetime.now() - timedelta(days=7))


def get_uptimes_between(start: datetime, stop: datetime) -> dict:
    """Produces the uptime of each device between the two specified timestamps"""
    time_interval = (stop - start).total_seconds()
    if time_interval <= 0:
        raise Exception("Illegal time interval!")

    query = f"""
        from(bucket: "{BUCKET}")
        |> range(start: {int(start.timestamp())}, stop: {int(stop.timestamp())})
        |> filter(fn: (r) => r._measurement == "telemetry_v2")
        |> filter(fn: (r) => r._field == "system_state")
        |> aggregateWindow(every: {WINDOW_INTERVAL_IN_SECONDS}s, fn: first)
        |> filter(fn: (r) => r._value != "ERROR" or r._value != "STANDBY")
        |> group(columns: ["device_id"])
        |> count()
        |> group()
    """
    with InfluxDBClient(HOST, API_TOKEN, timeout=20000) as influx_db_client:
        client = influx_db_client.query_api()

        query_res = client.query_data_frame(query, org=ORG)
        query_res.insert(
            0,
            "uptime",
            value=[
                (i * WINDOW_INTERVAL_IN_SECONDS) / time_interval
                for i in query_res._value
            ],
        )
    res = {query_res.device_id[i]: query_res.uptime[i] for i in range(len(query_res))}
    res_df = pd.DataFrame(res.items(),columns=["device_id","uptime"])
    res_df.insert(1, "uptime_readable", ["{:.2f}%".format(100*t) for t in res_df.uptime])
    return res, res_df, start, stop


def get_uptimes_last_week_starting_monday() -> dict:
    """Produces the uptime of each device between the last monday at 00:00 and the monday the week prior at 00:00"""
    def last_monday():
        today = datetime.now()
        # 0 is Monday
        days_since_monday = (today.weekday()) % 7
        last_monday = today - timedelta(days=days_since_monday)
        y, m, d = last_monday.year, last_monday.month, last_monday.day
        return datetime(y, m, d)

    time_stop = last_monday()
    time_start = time_stop - timedelta(days=7)
    return get_uptimes_between(time_start, time_stop)
