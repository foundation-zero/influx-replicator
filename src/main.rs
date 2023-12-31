use std::fmt::Display;
use std::io::BufWriter;
use std::iter;

use std::sync::Arc;
use std::time::Instant;

use async_stream::stream;

use anyhow::Error;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use clokwerk::{AsyncScheduler, TimeUnits};
use env_logger::{Builder, Target};
use fallible_iterator::FallibleIterator;

use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{join, Future, StreamExt};
use influxdb2::api::query::{FluxRecord, QueryTableIter};
use influxdb2::models::{Query, WriteDataPoint};
use influxdb2::{Client, ClientBuilder};
use influxdb2_structmap::value::Value;
use itertools::Itertools;
use log::{debug, error, info, LevelFilter};
use tokio::pin;
use tokio::sync::mpsc::{self, Receiver};
use tokio::sync::Mutex;

use clap::{Parser, Subcommand};

use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about=None)]
struct Args {
    #[arg(env = "INFLUXDB_SOURCE_URL", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    influx_src_url: String,
    #[arg(env = "INFLUXDB_SOURCE_ORGANISATION", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    influx_src_org: String,
    #[arg(env = "INFLUXDB_SOURCE_TOKEN", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    influx_src_token: String,
    #[arg(env = "INFLUXDB_SOURCE_BUCKET", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    influx_src_bucket: String,

    #[arg(env = "INFLUXDB_SINK_URL", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    influx_sink_url: String,
    #[arg(env = "INFLUXDB_SINK_ORGANISATION", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    influx_sink_org: String,
    #[arg(env = "INFLUXDB_SINK_TOKEN", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    influx_sink_token: String,
    #[arg(env = "INFLUXDB_SINK_BUCKET", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    influx_sink_bucket: String,

    #[arg(short, long, env= "LOG_LEVEL", default_value_t=LevelFilter::Info)]
    log_level: LevelFilter,

    #[arg(
        env = "SIMULTANEOUS_BATCHES",
        default_value_t = 1,
        help = "Amount of workers simultaneously requesting time slices from source"
    )]
    simul_batches: usize,

    #[arg(
        env = "SIMULTANEOUS_PAGES",
        default_value_t = 100,
        help = "Amount of workers simultaneously requesting tables from source"
    )]
    simul_pages: usize,

    #[arg(
        env = "SIMULTANEOUS_WRITES",
        default_value_t = 10,
        help = "Amount of workers simultaneously writing to sink"
    )]
    simul_writes: usize,

    #[arg(
        env = "CHANNEL_SIZE",
        default_value_t = 500,
        help = "Size of sink queue containing the responses from source"
    )]
    channel_size: usize,

    #[arg(
        env = "BATCH_MINUTES",
        default_value_t = 5,
        help = "Duration in minutes of time slice to request at a time (within the bound of max rows)"
    )]
    batch_minutes: i64,

    #[arg(
        env = "PAGE_MAX_ROW_AMOUNT",
        default_value_t = 1000000usize,
        help = "Maximum amount of rows in a single request"
    )]
    max_rows: usize,

    #[arg(
        env = "REQUEST_RETRIES",
        default_value_t = 3,
        help = "Number of retries made to reads and writes"
    )]
    request_retries: usize,

    #[arg(
        short,
        long,
        env = "READ_ONLY",
        default_value_t = false,
        help = "Disables the writing to sink. Can be useful to identify whether the replication is bottlenecked by reading or writing"
    )]
    read_only: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Sync,
    FullSync,
    Run {
        #[arg(env = "INTERVAL_MINUTES", default_value_t = 5)]
        interval_minutes: u32,

        #[arg(env = "SYNC_FIRST", default_value_t = true)]
        sync_first: bool,
    },
}

#[tokio::main]
async fn main() {
    let args = Arc::new(Args::parse());
    Builder::new()
        .filter_level(args.log_level)
        .target(Target::Stdout)
        .init();

    let is_running = Arc::new(Mutex::new(()));

    match &args.command {
        Commands::Sync => {
            wrap_task("Partial Sync", || async move {
                sync(args.clone(), SyncType::Partial).await
            })
            .await;
        }
        Commands::FullSync => {
            wrap_task("Full sync", || async move {
                sync(args.clone(), SyncType::Full).await
            })
            .await;
        }
        Commands::Run {
            interval_minutes,
            sync_first,
        } => {
            let initial_args = args.clone();
            if *sync_first {
                wrap_task("Initial Partial Sync", || async move {
                    sync(initial_args.clone(), SyncType::Partial).await
                })
                .await;
            }

            let mut scheduler = AsyncScheduler::with_tz(Utc);
            scheduler.every((*interval_minutes).minutes()).run(move || {
                let args = args.clone();
                let is_running = is_running.clone();
                async move {
                    // prevent tasks from overlapping
                    if is_running.try_lock().is_ok() {
                        wrap_task("Scheduled Partial Sync", || async {
                            sync(args, SyncType::Partial).await
                        })
                        .await;
                    } else {
                        info!("Skipping sync, because another sync is already running");
                    }
                }
            });
            loop {
                scheduler.run_pending().await;
                tokio::time::sleep(Duration::milliseconds(100).to_std().unwrap()).await;
            }
        }
    }
}

async fn wrap_task<T, F, V>(task: &str, f: F) -> Option<V>
where
    F: FnOnce() -> T,
    T: Future<Output = Result<V, Error>>,
{
    let start = Utc::now();
    info!("Starting {}", task);
    let outcome = f().await;
    let stop = Utc::now();
    match outcome {
        Ok(v) => {
            info!("Finished {} took {:?}", task, stop - start);
            Some(v)
        }
        Err(e) => {
            error!("Error in {}: {} took {:?}", task, e, stop - start);
            None
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum SyncError {
    #[error("no data in source")]
    NoDataInSource,
}

enum SyncType {
    Partial,
    Full,
}

async fn sync(args: Arc<Args>, sync: SyncType) -> Result<(), Error> {
    let src_client = ClientBuilder::new(
        &args.influx_src_url,
        &args.influx_src_org,
        &args.influx_src_token,
    )
    .gzip(false)
    .build()?;
    let sink_client = ClientBuilder::new(
        &args.influx_sink_url,
        &args.influx_sink_org,
        &args.influx_sink_token,
    )
    .gzip(false)
    .build()?;

    let (first_source_res, latest_source_res, latest_dest_res) = join!(
        get_edge(&src_client, &args.influx_src_bucket, Edge::First),
        get_edge(&src_client, &args.influx_src_bucket, Edge::Last),
        get_edge(&sink_client, &args.influx_sink_bucket, Edge::Last)
    );
    let first_source = first_source_res?;
    let latest_source = latest_source_res?;
    let latest_dest = latest_dest_res?;

    let sync_start = match (sync, latest_dest, first_source) {
        (SyncType::Full, _, Some(first_source)) => first_source,
        (SyncType::Partial, Some(latest_in_destination), _) => latest_in_destination,
        (SyncType::Partial, _, Some(first_source)) => first_source,
        _ => Err(SyncError::NoDataInSource)?,
    } - Duration::minutes(args.batch_minutes);

    match latest_source {
        Some(latest_source) if latest_source > sync_start => {
            info!("Syncing from {} to {}", sync_start, latest_source);
            let batches = batches(sync_start, latest_source, args.batch_minutes).collect_vec();

            let (tx, mut rx) = mpsc::channel::<Arc<Input>>(args.channel_size);

            let read_args = args.clone();
            let read = tokio::spawn(async move {
                stream_batches(
                    &src_client,
                    &read_args.influx_src_bucket,
                    batches,
                    tx,
                    ReadConfig {
                        simul_batches: read_args.simul_batches,
                        max_rows: read_args.max_rows,
                        retries: read_args.request_retries,
                        simul_pages: read_args.simul_pages,
                    },
                )
                .await
            });

            let write_args = args.clone();
            let write = if args.read_only {
                tokio::spawn(async move {
                    while (rx.recv().await).is_some() {
                        // ignore results
                    }
                    Ok(())
                })
            } else {
                tokio::spawn(async move {
                    write_batches(
                        &sink_client,
                        &write_args.influx_sink_bucket,
                        &mut rx,
                        write_args.request_retries,
                        write_args.simul_writes,
                    )
                    .await
                })
            };

            let (read_res, write_res) = join!(read, write);
            write_res??;
            read_res??;
        }
        _ => {
            info!("Skipping sync, already up-to-date");
        }
    }

    Ok(())
}

enum Edge {
    First,
    Last,
}

async fn get_edge(
    client: &Client,
    bucket: &str,
    edge: Edge,
) -> Result<Option<DateTime<FixedOffset>>, anyhow::Error> {
    let edge_op = match edge {
        Edge::First => "first",
        Edge::Last => "last",
    };
    let qs = format!(
        "from(bucket: \"{bucket}\")
        |> range(start: 1970-01-01T00:00:00.000Z)
        |> group()
        |> {edge_op}()
        |> keep(columns: [\"_time\"])"
    );

    let latest = client.query_raw(Some(Query::new(qs))).await?;

    Ok(latest.first().and_then(|rec| {
        rec.values.get("_time").and_then(|time| match time {
            Value::TimeRFC(x) => Some(*x),
            _ => None,
        })
    }))
}

async fn write_batch(client: &Client, bucket: &str, item: Arc<Input>) -> Result<(), Error> {
    let first = item
        .1
        .result()
        .next()
        .map(|rec| rec.and_then(|r| r.values.get("_time").cloned()));
    let vals = stream! {
        for value in item.1.result().iterator() {
            yield (value, item.clone())
        }
    };

    let body = vals.filter_map(|(record, item)| async move {
        match record {
            Ok(rec) => {
                let rec = LPRecord(rec, Arc::new(item.0.record.clone()));
                Some(rec)
            }
            Err(_) => None,
        }
    });

    client.write(bucket, body).await?;
    if let Ok(Some(Value::TimeRFC(t))) = first {
        debug!("Wrote from {}", t.to_rfc3339());
    };
    Ok(())
}

async fn write_batches(
    client: &Client,
    bucket: &str,
    rx: &mut Receiver<Arc<Input>>,
    retries: usize,
    simul_writes: usize,
) -> Result<(), Error> {
    let stream = stream! {
        while let Some(item) = rx.recv().await {
            yield item
        }
    };
    let stream = stream
        .map(|item| async move {
            let retry_strategy = ExponentialBackoff::from_millis(100)
                .map(jitter)
                .take(retries);

            Retry::spawn(retry_strategy, || write_batch(client, bucket, item.clone())).await
        })
        .buffered(simul_writes);

    pin!(stream);

    while let Some(res) = stream.next().await {
        res?;
    }
    Ok(())
}

type Batch = (DateTime<FixedOffset>, DateTime<FixedOffset>);

fn batches(
    start: DateTime<FixedOffset>,
    stop: DateTime<FixedOffset>,
    minutes: i64,
) -> impl Iterator<Item = Batch> {
    let step = Duration::minutes(minutes);
    let diff = stop - start;
    let steps = (diff.num_milliseconds() / step.num_milliseconds()) as i32;
    (0..=steps)
        .map(move |i| start + (step * i))
        .chain(iter::once(stop))
        .tuple_windows()
}

#[derive(Clone, Copy)]
struct ReadConfig {
    simul_batches: usize,
    max_rows: usize,
    retries: usize,
    simul_pages: usize,
}
type Input = (Table, QueryRes);

async fn stream_batches<'a>(
    client: &'a Client,
    bucket: &'a str,
    batches: Vec<Batch>,
    tx: mpsc::Sender<Arc<Input>>,
    read_config: ReadConfig,
) -> Result<(), Error> {
    let mut futs = FuturesOrdered::new();

    let mut begin = Instant::now();
    let mut first: Option<&DateTime<FixedOffset>> = None;

    let len = batches.len();

    for (i, (start, stop)) in batches.iter().enumerate() {
        let new_tx = tx.clone();
        let fut =
            async move { stream_batch(&new_tx, client, bucket, start, stop, read_config).await };
        futs.push_back(fut);

        if futs.len() == read_config.simul_batches {
            futs.next().await.unwrap()?; // unwrap is safe, because it is preceeded by a length check
        }
        if i % read_config.simul_batches == 0 {
            let end = Instant::now();
            let seconds = (end - begin).as_secs_f64();
            if let Some(f) = first {
                let processed_duration = stop.signed_duration_since(*f).num_seconds();
                info!(
                    "Batch {} / {}, {}% At {} took {:.2} seconds speed up {:.2}x queue capacity {}",
                    i,
                    len,
                    i / len,
                    start,
                    seconds,
                    (processed_duration as f64) / seconds,
                    tx.capacity()
                );
            };
            first = Some(start);
            begin = Instant::now();
        }
    }

    while let Some(item) = futs.next().await {
        item?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct BatchStart(usize);

impl Display for BatchStart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

type QueryRes = QueryTableIter;

enum BatchResult {
    Finished(QueryRes),
    Unfinished(QueryRes, BatchStart),
}

async fn stream_batch(
    tx: &mpsc::Sender<Arc<Input>>,
    client: &Client,
    bucket: &str,
    start: &DateTime<FixedOffset>,
    stop: &DateTime<FixedOffset>,
    read_config: ReadConfig,
) -> Result<(), Error> {
    let tables = read_batch_tables(client, bucket, start, stop).await?;
    let mut futures = FuturesUnordered::new();
    for table in tables {
        let future = read_table_batches(
            client,
            bucket,
            start,
            stop,
            table,
            read_config.max_rows,
            read_config.retries,
        );
        futures.push(future);
        if futures.len() == read_config.simul_pages {
            let (table, results) = futures.next().await.unwrap()?; // unwrap is safe, because it is preceeded by a length check
            for i in results.iter() {
                let val: Arc<Input> = Arc::new((table.to_owned(), i.clone()));
                tx.send(val.clone()).await?;
            }
        }
    }
    while let Some(item) = futures.next().await {
        let (table, query_results) = item?;
        for result in query_results.iter() {
            let val: Arc<Input> = Arc::new((table.to_owned(), result.clone()));
            tx.send(val.clone()).await?;
        }
    }
    Ok(())
}

#[derive(Clone)]
struct Table {
    record: FluxRecord,
}

impl Table {
    fn new(record: &FluxRecord) -> Table {
        Table {
            record: record.to_owned(),
        }
    }

    fn filter(&self) -> TableFilter {
        let clauses = self
            .record
            .values
            .iter()
            .filter(|(column, _)| *column != "_time" && *column != "_value" && *column != "result")
            .filter_map(|(column, value)| match value {
                Value::String(str) => Some((column, str)),
                _ => None,
            })
            .map(|(column, value)| format!("r[\"{column}\"] == \"{value}\""))
            .join(" and ");
        TableFilter {
            query_part: format!("filter(fn: (r) => {clauses})"),
        }
    }
}

struct TableFilter {
    query_part: String,
}

impl Display for TableFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.query_part.as_str())
    }
}

async fn read_batch_tables(
    client: &Client,
    bucket: &str,
    start: &DateTime<FixedOffset>,
    stop: &DateTime<FixedOffset>,
) -> Result<Vec<Table>, Error> {
    let start_str = start.to_rfc3339();
    let stop_str = stop.to_rfc3339();
    let qs = format!(
        "from(bucket: \"{bucket}\")
        |> range(start: {start_str}, stop: {stop_str})
        |> drop(columns: [\"_start\", \"_stop\"])
        |> limit(n: 1)"
    );
    let rows = client.query_raw(Some(Query::new(qs))).await?;
    Ok(rows.iter().map(Table::new).collect_vec())
}

async fn read_table_batches(
    client: &Client,
    bucket: &str,
    start: &DateTime<FixedOffset>,
    stop: &DateTime<FixedOffset>,
    table: Table,
    max_rows: usize,
    retries: usize,
) -> Result<(Table, Vec<QueryRes>), Error> {
    let mut offset = BatchStart(0);
    let mut results: Vec<QueryRes> = Vec::new();
    loop {
        let retry_strategy = ExponentialBackoff::from_millis(100)
            .map(jitter)
            .take(retries);
        match Retry::spawn(retry_strategy, || {
            read_table_batch(client, bucket, start, stop, &table, offset, max_rows)
        })
        .await?
        {
            BatchResult::Unfinished(result, new_offset) => {
                offset = new_offset;
                results.push(result);
            }
            BatchResult::Finished(result) => {
                results.push(result);
                break;
            }
        }
    }
    Ok((table.to_owned(), results))
}

async fn read_table_batch(
    client: &Client,
    bucket: &str,
    start: &DateTime<FixedOffset>,
    stop: &DateTime<FixedOffset>,
    table: &Table,
    offset: BatchStart,
    max_rows: usize,
) -> Result<BatchResult, Error> {
    let start_str = start.to_rfc3339();
    let stop_str = stop.to_rfc3339();
    let qs = format!(
        "from(bucket: \"{bucket}\")
        |> range(start: {start_str}, stop: {stop_str})
        |> {}
        |> limit(n: {max_rows}, offset: {offset})
        |> keep(columns: [\"_time\", \"_value\"])",
        table.filter()
    );
    debug!("Querying {qs}");
    let query_res = client.query_raw_iter(Some(Query::new(qs))).await?;
    if query_res.is_empty() {
        Ok(BatchResult::Finished(query_res))
    } else {
        Ok(BatchResult::Unfinished(
            query_res,
            BatchStart(offset.0 + max_rows),
        ))
    }
}

#[derive(Clone)]
struct LPRecord(FluxRecord, Arc<FluxRecord>);

impl WriteDataPoint for LPRecord {
    fn write_data_point_to<W>(&self, mut w: W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        match (
            self.1.values.get("_measurement"),
            self.1.values.get("_field"),
            self.0.values.get("_time"),
            self.0.values.get("_value"),
        ) {
            (Some(m), Some(f), Some(t), Some(v)) => {
                write!(w, "{}", LPValue::new(m))?;
                for (k, v) in self
                    .1
                    .values
                    .iter()
                    .filter(|(k, _)| !matches!(k.chars().next(), Some('_') | None))
                    .filter(|(_, v)| !matches!(v, Value::String(x) if x.is_empty()))
                    .filter(|(k, _)| !matches!(k.as_str(), "result" | "table"))
                {
                    write!(w, ",{}={}", k, LPValue(v))?;
                }
                writeln!(w, " {}={} {}", LPValue(f), LPValue(v), LPValue(t))?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl Display for LPRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = BufWriter::new(Vec::new());
        self.write_data_point_to(&mut buf)
            .map_err(|_| std::fmt::Error)?;
        let bytes = buf.into_inner().map_err(|_| std::fmt::Error)?;
        f.write_str(
            String::from_utf8(bytes)
                .map_err(|_| std::fmt::Error)?
                .as_str(),
        )
    }
}

struct LPValue<'a>(&'a Value);

impl<'a> Display for LPValue<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            // possible to optimize this by replacing on write instead of replacing in memory first
            Value::String(s) => f.write_str(&s.as_str().replace(' ', "\\ ").replace(',', "\\,")),
            Value::Bool(true) => f.write_str("t"),
            Value::Bool(false) => f.write_str("f"),
            Value::Double(d) => write!(f, "{}", d),
            Value::Long(l) => write!(f, "{}", l),
            Value::TimeRFC(d) => write!(f, "{}", d.timestamp_nanos()),
            _ => Err(std::fmt::Error),
        }
    }
}

impl<'a> LPValue<'a> {
    fn new(val: &'a Value) -> LPValue<'a> {
        return LPValue(val);
    }
}
