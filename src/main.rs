use std::fmt::Display;
use std::io::{self, Write};
use std::iter;

use std::sync::Arc;
use std::time::Instant;

use async_stream::stream;

use anyhow::Error;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use clokwerk::{AsyncScheduler, TimeUnits};
use env_logger::{Builder, Target};
use fallible_iterator::FallibleIterator;

use futures::stream::FuturesOrdered;
use futures::{join, Future, StreamExt};
use influxdb2::api::query::{FluxRecord, QueryTableIter};
use influxdb2::models::{Query, WriteDataPoint};
use influxdb2::Client;
use influxdb2_structmap::value::Value;
use itertools::Itertools;
use log::{debug, error, info, LevelFilter};
use reqwest::Body;
use tokio::sync::mpsc::{self, Receiver};
use tokio::sync::Mutex;

use clap::{Parser, Subcommand};

use bytes::BufMut;
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

    #[arg(env= "LOG_LEVEL", default_value_t=LevelFilter::Info)]
    log_level: LevelFilter,

    #[arg(
        env = "SIMULTANEOUS_BATCHES",
        default_value_t = 10,
        help = "Amount of workers simultaneously requesting from source"
    )]
    simul_batches: usize,

    #[arg(
        env = "CHANNEL_SIZE",
        default_value_t = 20,
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
        default_value_t = 400000usize,
        help = "Maximum amount of rows in a single request"
    )]
    max_rows: usize,

    #[command(subcommand)]
    command: Option<Commands>,
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
        Some(Commands::Sync) => {
            wrap_task("Partial Sync", || async move {
                sync(args.clone(), SyncType::Partial).await
            })
            .await;
        }
        Some(Commands::FullSync) => {
            wrap_task("Full sync", || async move {
                sync(args.clone(), SyncType::Full).await
            })
            .await;
        }
        Some(Commands::Run {
            interval_minutes,
            sync_first,
        }) => {
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
        None => {}
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
    let src_client = Client::new(
        &args.influx_src_url,
        &args.influx_src_org,
        &args.influx_src_token,
    );
    let sink_client = Client::new(
        &args.influx_sink_url,
        &args.influx_sink_org,
        &args.influx_sink_token,
    );

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
                    read_args.simul_batches,
                    read_args.max_rows,
                )
                .await
            });

            let write_args = args.clone();
            let write = tokio::spawn(async move {
                write_batches(
                    &sink_client,
                    &write_args.influx_sink_org,
                    &write_args.influx_sink_bucket,
                    &mut rx,
                )
                .await
            });

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

async fn write_batch(
    client: &Client,
    org: &str,
    bucket: &str,
    item: Arc<QueryTableIter>,
) -> Result<(), Error> {
    let first = item
        .result()
        .next()
        .map(|rec| rec.and_then(|r| r.values.get("_time").cloned()));
    let mut buffer = bytes::BytesMut::new();
    let vals = stream! {
        for value in item.result().iterator() {
            yield value
        }
    };

    let x = vals.map(move |record| match record {
        Ok(rec) => {
            let mut w = (&mut buffer).writer();
            LPRecord(rec).write_data_point_to(&mut w)?;
            w.flush()?;
            Ok::<_, io::Error>(buffer.split().freeze())
        }
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
    });
    let body: Body = Body::wrap_stream(x);

    client.write_line_protocol(org, bucket, body).await?;
    if let Ok(Some(Value::TimeRFC(t))) = first {
        debug!("Wrote from {}", t.to_rfc3339());
    };
    Ok(())
}

async fn write_batches(
    client: &Client,
    org: &str,
    bucket: &str,
    rx: &mut Receiver<Arc<Input>>,
) -> Result<(), Error> {
    while let Some(item) = rx.recv().await {
        let retry_strategy = ExponentialBackoff::from_millis(100).map(jitter).take(3);

        Retry::spawn(retry_strategy, || {
            write_batch(client, org, bucket, item.clone())
        })
        .await?;
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

type Input = QueryRes;

async fn stream_batches<'a>(
    client: &'a Client,
    bucket: &'a str,
    batches: Vec<Batch>,
    tx: mpsc::Sender<Arc<Input>>,
    simul_batches: usize,
    max_rows: usize,
) -> Result<(), Error> {
    let mut futs = FuturesOrdered::new();

    let mut begin = Instant::now();
    let mut first: Option<&DateTime<FixedOffset>> = None;

    let len = batches.len();

    for (i, (start, stop)) in batches.iter().enumerate() {
        let fut = async move { stream_batch(client, bucket, start, stop, max_rows).await };
        futs.push_back(fut);

        if futs.len() == simul_batches {
            let results = futs.next().await.unwrap()?; // unwrap is safe, because it is preceeded by a length check
            for i in results.iter() {
                let val: Arc<QueryTableIter> = Arc::new(i.clone());
                tx.send(val.clone()).await?;
            }
        }
        if i % simul_batches == 0 {
            let end = Instant::now();
            let seconds = (end - begin).as_secs_f64();
            if let Some(f) = first {
                let processed_duration = stop.signed_duration_since(*f).num_seconds();
                info!(
                    "Batch {} / {}, {}% At {} took {:.2} seconds speed up {:.2}x",
                    i,
                    len,
                    i / len,
                    start,
                    seconds,
                    (processed_duration as f64) / seconds
                );
            };
            first = Some(start);
            begin = Instant::now();
        }
    }

    while let Some(item) = futs.next().await {
        for i in item?.iter() {
            let val: Arc<QueryTableIter> = Arc::new(i.clone());
            tx.send(val.clone()).await?;
        }
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

async fn stream_batch(
    client: &Client,
    bucket: &str,
    start: &DateTime<FixedOffset>,
    stop: &DateTime<FixedOffset>,
    max_rows: usize,
) -> Result<Vec<QueryRes>, Error> {
    let mut offset = BatchStart(0);
    let mut results: Vec<QueryRes> = Vec::new();
    loop {
        let retry_strategy = ExponentialBackoff::from_millis(100).map(jitter).take(3);

        match Retry::spawn(retry_strategy, || {
            read_batch(client, bucket, start, stop, offset, max_rows)
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
    Ok(results)
}

type QueryRes = QueryTableIter;

enum BatchResult {
    Finished(QueryRes),
    Unfinished(QueryRes, BatchStart),
}

async fn read_batch(
    client: &Client,
    bucket: &str,
    start: &DateTime<FixedOffset>,
    stop: &DateTime<FixedOffset>,
    offset: BatchStart,
    max_rows: usize,
) -> Result<BatchResult, Error> {
    let start_str = start.to_rfc3339();
    let stop_str = stop.to_rfc3339();
    let qs = format!(
        "from(bucket: \"{bucket}\")
        |> range(start: {start_str}, stop: {stop_str})
        |> drop(columns: [\"_start\", \"_stop\"])
        |> group()
        |> limit(n: {max_rows}, offset: {offset})"
    );
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
struct LPRecord(FluxRecord);

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

impl WriteDataPoint for LPRecord {
    fn write_data_point_to<W>(&self, mut w: W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        match (
            self.0.values.get("_measurement"),
            self.0.values.get("_field"),
            self.0.values.get("_time"),
            self.0.values.get("_value"),
        ) {
            (Some(m), Some(f), Some(t), Some(v)) => {
                write!(w, "{}", LPValue::new(m))?;
                for (k, v) in self
                    .0
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
