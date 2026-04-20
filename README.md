# Influx Replicator

Influx replicator is a light weight InfluxDB replication utility. It supports full syncs and partial syncs, where the source is queried starting from the latest entry in the sink. We've tried to keep RAM usage low, while supporting parallel importing.

## Command Line Reference

Influx replicator has 3 commands:

- `run`: Runs an immediate partial sync at startup, then repeats on a schedule. This is the command you want for normal, ongoing operation.
- `sync`: Runs a single partial sync and exits.
- `full-sync`: Runs a single full sync and exits.

### Full sync vs. partial sync

A **partial sync** starts from the most recent timestamp already present in the sink. It only replicates data that is newer than what the sink already has. If the sink is empty it falls back to the oldest data point in the source.

A **full sync** always starts from the very first timestamp in the source, regardless of what is already in the sink. Use it when the sink bucket has been wiped, or when you suspect the sink has gaps from a previous failed run.

Both sync to the most recent data point currently in the source. Both start one `BATCH_MINUTES` window before the computed start point as a safety margin, so a small amount of data near the boundary is always re-written. InfluxDB's line protocol is idempotent for identical points (same measurement, tags, field, and timestamp), so re-writing existing data is safe. If the source data was modified between syncs, old and new values will coexist in the sink.

### No backfill support

The replicator assumes data is not written to the past. If a point is written to the source with a timestamp earlier than the sink's latest timestamp, a partial sync will not replicate it. Only a `full-sync` would catch such cases. This is a known limitation.

### Skipped runs under load

The `run` command drops a scheduled sync if the previous one is still running. No queue builds up; the skipped run is logged as "Skipping sync, because another sync is already running". If your data volume grows and syncs routinely take longer than `INTERVAL_MINUTES`, you will fall behind with no further warning. Monitor sync duration against your interval.

For more information on all command line arguments, run `docker run ghcr.io/foundation-zero/influx-replicator:main --help`.

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
