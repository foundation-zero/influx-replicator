FROM scratch
ARG TARGET
COPY target/${TARGET}/release/influx-replicator /app/influx-replicator

ENTRYPOINT ["/app/influx-replicator"]
