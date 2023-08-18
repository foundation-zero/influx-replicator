# Influx replicator

Influx replicator is a light weight influx replication utility. It support full syncs and partial syncs, where the source is queried starting from the latest entry in the sink. We've tried to keep RAM usage low, while supporting parallel importing.

## Commands

Influx replicator has 3 commands:

- run: first runs a partial sync and then periodically starts a new partial sync
- sync: runs a partial sync
- full-sync: runs a full sync

For more information on the args, see `influx-replicator --help`

## Future possible additions

It'd be nice to have the replicator automatically vary the batch size to equalize the size of each batch depending on the amount of data in the batch.

It'd also be nice to have the ability to offset the beginning of the sync. Currently it's assumed data isn't back written. It's not beyond impossible for some to write data to some point in the past, so being able to have syncs start a variable number of days before the latest point.
