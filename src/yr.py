import os
import datetime
import requests
import json
from collections import namedtuple
  

TimeSeries = namedtuple('TimeSeries', ['element_id'])
Observation = namedtuple('Observation', ['element_id', 'value', 'timestamp', 'level'])
   

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
BLINDERN = "SN18700"

MET_API_USER = os.getenv('MET_API_USER')
MET_API_PASSWORD = os.getenv('MET_API_PASSWORD')


def fetch_time_series(
        source: str,
        element: str,
        _from: datetime.datetime,
):
    f = datetime.datetime.strftime(_from, "%Y-%m-%dT%H:%M:%S.000Z")
    now = datetime.datetime.strftime(datetime.datetime.now(), DATE_FORMAT)

    response = requests.get(
        f"https://frost.met.no/observations/v0.jsonld",
        params={
            "sources": source,
            "referencetime": f"{f}/{now}",
            "elements": element,
            "timeresolutions": "PT1H"
        },
        auth=requests.auth.HTTPBasicAuth(MET_API_USER, MET_API_PASSWORD),
    )

    json_response = response.json()
    if "error" in json_response:
        print(json.dumps(json_response["error"], indent=2))
        return []

    observations = []
    for data in json_response["data"]:
        for observation in data["observations"]:
            timestamp = datetime.datetime.strptime(
                data["referenceTime"], DATE_FORMAT
            )
            element_id = observation["elementId"]
            value = float(observation["value"])
            level = None
            if "level" in observation:
                level = observation["level"]["value"]
            observations.append(Observation(element_id=element_id, value=value, timestamp=timestamp, level=level))
    return observations


def available_time_series(source: str):
    response = requests.get(
        f"https://frost.met.no/observations/availableTimeSeries/v0.jsonld?sources={source}",
        auth=requests.auth.HTTPBasicAuth(MET_API_USER, MET_API_PASSWORD),
    )
    json_response = response.json()
    if "error" in json_response:
        print(json.dumps(json_response["error"], indent=2))
        return {}

    time_series = []
    for data in json_response["data"]:
        if "validTo" in data:  # Only use timeseries that are valid now
            continue
        if data["timeResolution"] != "PT1H":  # Only use hourly data
            continue
        time_series.append(TimeSeries(element_id=data["elementId"]))
    return time_series


def fetch_missing_observations(source_id: str, element_id: str, latest_timestamp: datetime.datetime):
    if latest_timestamp is not None:
        return fetch_time_series(source_id, element_id, latest_timestamp)
    else:
        print("No latest found. Using 3 days ago")
        _from = datetime.datetime.now() - datetime.timedelta(days=3)
        return fetch_time_series(source_id, element_id, _from)

