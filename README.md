# metapi-prom-grafana

Local grafana dashboard for historical yr data. Will fetch missing observations since last run on startup.


## How to

Edit `sources.json` to select which sources to include (by setting `include` to `true`).
```
docker compose up
```


## Requirements

docker compose


## TODO

 - collect via availableTimeSeries first then
