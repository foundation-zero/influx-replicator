# Influx Replicator

Influx replicator is a light weight InfluxDB replication utility. It support full syncs and partial syncs, where the source is queried starting from the latest entry in the sink. We've tried to keep RAM usage low, while supporting parallel importing.

## Command Line Reference

Influx replicator has 3 commands:

- run: first runs a partial sync and then periodically starts a new partial sync. You probably want to use this command.
- sync: runs a partial sync
- full-sync: runs a full sync

For more information on the command line arguments or environment variables, run `docker run ghcr.io/foundation-zero/influx-replicator:main --help`.

## Example Using `docker-compose`

If you use `docker-compose`, the following should give you a starting point. Note that this assumes you define the envirnment variables in a `.env` file.

```
  influx-replicator:
    image: ghcr.io/foundation-zero/influx-replicator:main
    depends_on:
      - influxdb
    environment:
      INFLUXDB_SOURCE_URL: ${INFLUXDB_SOURCE_URL}
      INFLUXDB_SOURCE_ORGANISATION: ${INFLUXDB_SOURCE_ORGANISATION}
      INFLUXDB_SOURCE_TOKEN: ${INFLUXDB_SOURCE_TOKEN}
      INFLUXDB_SOURCE_BUCKET: ${INFLUXDB_SOURCE_BUCKET}
      INFLUXDB_SINK_URL: ${INFLUXDB_SINK_URL}
      INFLUXDB_SINK_ORGANISATION: ${INFLUXDB_SINK_ORGANISATION}
      INFLUXDB_SINK_TOKEN: ${INFLUXDB_SINK_TOKEN}
      INFLUXDB_SINK_BUCKET: ${INFLUXDB_SINK_BUCKET}
    command:
      - run
```

## Possible Future Additions

It'd be nice to have the replicator automatically vary the batch size to equalize the size of each batch depending on the amount of data in the batch.

It'd also be nice to have the ability to offset the beginning of the sync. Currently it's assumed data isn't back written. It's not beyond impossible for some to write data to some point in the past, so being able to have syncs start a variable number of days before the latest point.
