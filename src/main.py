import json

import yr
import database

import sys

def read_sources():
    result = {}
    with open("sources.json", "r") as sources_file:
        sources = json.load(sources_file)
        for source in sources:
            if source["include"]:
                result[source["id"]] = source["name"]
    return result
 

if __name__ == "__main__":
    db = database.open("postgresql://user:password@postgres:5432/met-data")
    database.create_tables(db)

    sources = read_sources()
    database.insert_sources(db, sources)


    for source_id, v in sources.items():
        time_series = yr.available_time_series("SN18700")
        for time_serie in time_series:
            latest_timestamp = database.latest_timestamp(db, source_id, time_serie.element_id)
            missing_observations = yr.fetch_missing_observations(source_id, time_serie.element_id, latest_timestamp)

            for o in missing_observations:
                database.insert(db, source_id, o.element_id, o.value, o.timestamp, o.level)

    print("Done")
