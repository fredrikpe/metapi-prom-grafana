import psycopg2
import os
import datetime
import requests
import json
import sys

SOURCES = {}

with open("sources.json", "r") as sources_file:
    ss = json.load(sources_file)
    for source in ss:
        SOURCES[source["id"]] = source["shortName"]
    

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
BLINDERN = "SN18700"

MET_API_USER = os.getenv('MET_API_USER')
MET_API_PASSWORD = os.getenv('MET_API_PASSWORD')


#daily = f"https://frost.met.no/observations/v0.jsonld?sources={source}:0&referencetime={f}/{now}&elements=mean(surface_air_pressure P1D)&timeoffsets=PT0H&timeresolutions=P1D&timeseriesids=0&performancecategories=C&exposurecategories=2&levels=2.0",


def yr_surface(source: str, _from: datetime.datetime):
    f = datetime.datetime.strftime(_from, "%Y-%m-%dT%H:%M:%S.000Z")
    now = datetime.datetime.strftime(datetime.datetime.now(), DATE_FORMAT)

    response = requests.get(
        f"https://frost.met.no/observations/v0.jsonld?sources={source}:0&referencetime={f}/{now}&elements=surface_air_pressure&timeoffsets=PT0H&timeresolutions=PT1H&timeseriesids=0&performancecategories=C&exposurecategories=2&levels=2.0",
        auth=requests.auth.HTTPBasicAuth(MET_API_USER, MET_API_PASSWORD),
    )
    json_response = response.json()
    if "error" in json_response:
        print(json.dumps(json_response["error"], indent=2))
        return []
    return json_response["data"]



def open(url):
    return psycopg2.connect(url)


def create_tables(db):
    cursor = db.cursor()
    cursor.execute(
        """ CREATE TABLE IF NOT EXISTS mean_surface_pressure (
            time TIMESTAMP NOT NULL,
            source_id TEXT NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY(source_id, time)
        ) """
    )
    cursor.execute(
        """ CREATE TABLE IF NOT EXISTS sources (
            source_id TEXT PRIMARY KEY,
            short_name TEXT NOT NULL
        ) """
    )
    db.commit()


def insert_sources(db):
    stmt = """INSERT INTO sources (
        source_id, short_name
    ) VALUES(%s,%s)
    ON CONFLICT (source_id) DO UPDATE
    SET short_name = excluded.short_name;"""
    cur = db.cursor()

    for k, v in SOURCES.items():
        cur.execute(stmt, (k, v))

    db.commit()


def insert(db, source: str, value: float, timestamp):
    stmt = """INSERT INTO mean_surface_pressure (
        source_id, value, time
    ) VALUES(%s,%s,%s)
    ON CONFLICT (source_id, time) DO UPDATE
    SET
        value = excluded.value;
    """
    cur = db.cursor()
    cur.execute(stmt, (source, value, timestamp))
    db.commit()


def latest_timestamp(db, source_id: str):
    cur = db.cursor()
    cur.execute("SELECT time FROM mean_surface_pressure WHERE source_id = %s ORDER BY time LIMIT 1", (source_id,))
    res = cur.fetchone()
    if res is None:
        return None
    return res[0]


def insert_observations(db, observations):
    for data in observations:
        timestamp = datetime.datetime.strptime(
            data["referenceTime"], DATE_FORMAT
        )
        value = float(data["observations"][0]["value"])
        source = data["sourceId"][:-2]
        insert(db, source, value, timestamp)


def fetch_missing_observations(source_id: str, latest_timestamp: datetime.datetime):
    if latest_timestamp is not None:
        return yr_surface(source_id, latest_timestamp)
    else:
        print("No latest found. Using 3 days ago")
        _from = datetime.datetime.now() - datetime.timedelta(days=3)
        return yr_surface(source_id, _from)

 
if __name__ == "__main__":
    db = open("postgresql://user:password@postgres:5432/met-data")
    create_tables(db)
    insert_sources(db)
    
    for k, v in SOURCES.items():
        missing_observations = fetch_missing_observations(k, latest_timestamp(db, k))
        insert_observations(db, missing_observations)


