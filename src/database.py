import psycopg2
import datetime


DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"


def open(url):
    return psycopg2.connect(url)


def create_tables(db):
    cursor = db.cursor()
    cursor.execute(
        """ CREATE TABLE IF NOT EXISTS hourly_data (
            time TIMESTAMP NOT NULL,
            source_id TEXT NOT NULL,
            element_id TEXT NOT NULL,
            level INTEGER NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY(source_id, element_id, time, level)
        ) """
    )
    cursor.execute(
        """ CREATE TABLE IF NOT EXISTS sources (
            source_id TEXT PRIMARY KEY,
            name TEXT NOT NULL
        ) """
    )
    db.commit()


def insert_sources(db, sources: dict):
    stmt = """INSERT INTO sources (
        source_id, name
    ) VALUES(%s,%s)
    ON CONFLICT (source_id) DO UPDATE
    SET name = excluded.name;"""
    cur = db.cursor()

    for k, v in sources.items():
        cur.execute(stmt, (k, v))

    db.commit()


def insert(db, source: str, element_id: str, value: float, timestamp: datetime.datetime, level: int):
    stmt = """INSERT INTO hourly_data (
        source_id, value, time, element_id, level
    ) VALUES(%s,%s,%s,%s,%s)
    ON CONFLICT (source_id, time, element_id, level) DO UPDATE
    SET
        value = excluded.value;
    """
    cur = db.cursor()
    cur.execute(stmt, (source, value, timestamp, element_id, -1 if level is None else level))
    db.commit()


def latest_timestamp(db, source_id: str, element_id: str):
    cur = db.cursor()
    cur.execute("SELECT time FROM hourly_data WHERE source_id = %s and element_id = %s ORDER BY time LIMIT 1", (source_id,element_id,))
    res = cur.fetchone()
    if res is None:
        return None
    return res[0]


