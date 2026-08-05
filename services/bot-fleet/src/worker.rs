//! This module implements worker behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use iicpc_schemas_rust::{
    BotProfile, OrdType, OrderSentEvent, PayloadType, Protocol, ReadySignal, Side, TargetSpec,
    TaskSpec, WorkloadSpec,
};
use rand::{rngs::SmallRng, Rng};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::{lookup_host, TcpStream},
    sync::{watch, Notify},
    task::JoinSet,
    time::{self, Instant},
};
use tokio_tungstenite::{
    connect_async, tungstenite::Message as WsMessage, MaybeTlsStream, WebSocketStream,
};
use tracing::{error, info, warn};

use crate::{
    config::Config,
    content::{self, TaskGenerator},
    fix::{self, OrderFrame},
    kafka::{self, KafkaProducer},
    metrics,
    telemetry::TelemetrySink,
    time::unix_nanos,
};

const RESPONSE_TIMEOUT_NS: u64 = 5_000_000_000;

const BARRIER_WAIT: Duration = Duration::from_secs(120);

// Closed-loop in-flight cap (per task, `Config::max_inflight_per_task`), shared by
// EVERY protocol write loop (FIX, REST, WS) so backpressure behaviour is identical
// across them. A slow contestant — or a stalled telemetry path — makes the per-task
// pending map (sent-but-unacked orders) grow without bound and OOMs the worker. Cap
// in-flight orders: at the cap the send loop backpressures instead of firing more,
// pacing the connection to the contestant's real ack rate. A healthy contestant
// keeps in-flight at ~rate×RTT (orders of magnitude below the cap → never engages);
// only an over-driven slow contestant is throttled, to its honest sustainable rate.
// Default 10k/task (~2.4 MiB) → bounded worker memory that fits a small node, and
// well above per_task_rate × RESPONSE_TIMEOUT for any realistic rate, so it never
// false-throttles (e.g. an 835k/500-task drain sits at ~8.4k in-flight, under 10k).

#[derive(Clone)]
/// CancelToken stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct CancelToken {
    tx: Arc<watch::Sender<bool>>,
    rx: watch::Receiver<bool>,
}

impl CancelToken {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            tx: Arc::new(tx),
            rx,
        }
    }

    /// cancel performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn cancel(&self) {
        let _ = self.tx.send(true);
    }

    /// is_cancelled performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    /// cancelled performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn cancelled(&self) {
        let mut rx = self.rx.clone();
        if *rx.borrow() {
            return;
        }
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
    }
}

/// should_stop_sending performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn should_stop_sending(cancel: &CancelToken, task_end_ns: u64) -> bool {
    cancel.is_cancelled() || unix_nanos() >= task_end_ns
}

/// Default WORKLOAD_SPEC_MAX_AGE_S: a workload spec older than this at consume
/// time is a stale leftover from a session whose controller-side partition
/// lease was already released and reused by a newer session (runner.go
/// releases the lease the instant Run() returns, regardless of whether the
/// published spec was ever consumed) — so it's skipped rather than run.
const DEFAULT_WORKLOAD_SPEC_MAX_AGE_S: u64 = 300;

/// workload_spec_max_age_s reads WORKLOAD_SPEC_MAX_AGE_S from the environment,
/// following this codebase's inline env-var parsing convention (see
/// kafka.rs::telemetry_producer's KAFKA_TELEMETRY_COMPRESSION_LEVEL).
fn workload_spec_max_age_s() -> u64 {
    std::env::var("WORKLOAD_SPEC_MAX_AGE_S")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_WORKLOAD_SPEC_MAX_AGE_S)
}

/// is_spec_stale reports whether a workload spec published at
/// `published_at_unix_ns` is older than `max_age_s` as of `now_unix_ns`.
/// `published_at_unix_ns == 0` (unset — older/other producers that don't
/// stamp this field) is treated as NOT stale, for backward compatibility.
fn is_spec_stale(published_at_unix_ns: u64, now_unix_ns: u64, max_age_s: u64) -> bool {
    if published_at_unix_ns == 0 {
        return false;
    }
    let max_age_ns = max_age_s.saturating_mul(1_000_000_000);
    now_unix_ns.saturating_sub(published_at_unix_ns) > max_age_ns
}

/// Default MAX_CONCURRENT_WORKLOADS: how many workload specs one worker executes at
/// once.
///
/// Was effectively 1 (the consume loop awaited each `run_workload` inline), which
/// serialized two concurrent sessions that happened to land on the same pod no matter
/// how small their shards were. The cap is what keeps concurrency from breaking the
/// memory reasoning in k8s/benchmark/bot-fleet/deployment.yaml: worst case per
/// workload is tasks x BOT_MAX_INFLIGHT_PER_TASK x ~350 B, so the invariant to hold is
///
///   MAX_CONCURRENT_WORKLOADS x (tasks x inflight x ~350 B) <= container memory limit
///
/// 2 is safe for EKS-sized shards (1000 tasks x 10k inflight ~= 3.5 GiB each against a
/// 6Gi limit); local shards are ~200 tasks and tolerate more.
const DEFAULT_MAX_CONCURRENT_WORKLOADS: usize = 2;

/// max_concurrent_workloads reads MAX_CONCURRENT_WORKLOADS from the environment,
/// following this codebase's inline env-var parsing convention. A parsed 0 is
/// promoted to 1: zero would deadlock the loop by admitting nothing.
fn max_concurrent_workloads() -> usize {
    std::env::var("MAX_CONCURRENT_WORKLOADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.max(1))
        .unwrap_or(DEFAULT_MAX_CONCURRENT_WORKLOADS)
}

/// InFlight tracks which partitions currently have a workload executing, and enforces
/// the two admission rules that keep concurrent execution safe:
///
///  1. **At most one workload in flight per partition.** Offsets are committed per
///     partition after a workload finishes, so two concurrent workloads on the same
///     partition would make commits order-dependent: if the second finished first and
///     committed its offset, the committed position would move PAST the first, losing
///     it on restart. One-per-partition makes every commit independent and correct.
///     This costs nothing in practice because the controller's partition lease already
///     guarantees two concurrent sessions occupy different partitions.
///  2. **A global concurrency cap** (see MAX_CONCURRENT_WORKLOADS) so memory stays
///     bounded.
///
/// Kept as a separate type with no Kafka dependency so the admission logic is unit
/// testable without a broker.
#[derive(Debug, Default)]
struct InFlight {
    /// partition -> the concurrency SLOT index that partition's workload occupies.
    /// The slot is what makes each concurrent workload's barrier consumer group
    /// distinct (see barrier_group): slots are bounded by MAX_CONCURRENT_WORKLOADS and
    /// reused, so a pod only ever creates a small fixed set of broker-side groups.
    partitions: std::collections::HashMap<i32, usize>,
}

/// Why a workload could not be admitted right now. Both cases mean "retry later",
/// never "drop": the caller leaves the message uncommitted so Kafka redelivers it.
#[derive(Debug, PartialEq, Eq)]
enum Reject {
    /// This partition already has a workload executing (rule 1).
    PartitionBusy,
    /// The global concurrency cap is reached (rule 2).
    AtCapacity,
}

impl InFlight {
    fn new() -> Self {
        Self {
            partitions: std::collections::HashMap::new(),
        }
    }

    /// try_admit reserves `partition` for a workload and returns the concurrency SLOT
    /// index it was given, or explains why it cannot be admitted.
    ///
    /// The partition-busy check runs FIRST so a busy partition is reported as such even
    /// when the worker also happens to be at capacity — the two conditions have
    /// different remedies (wait for that workload vs. wait for any workload).
    fn try_admit(&mut self, partition: i32, cap: usize) -> Result<usize, Reject> {
        if self.partitions.contains_key(&partition) {
            return Err(Reject::PartitionBusy);
        }
        if self.partitions.len() >= cap {
            return Err(Reject::AtCapacity);
        }
        // Lowest free slot, so slot indices stay in [0, cap) and are reused rather than
        // growing without bound.
        let used: std::collections::HashSet<usize> = self.partitions.values().copied().collect();
        let slot = (0..cap)
            .find(|s| !used.contains(s))
            .ok_or(Reject::AtCapacity)?;
        self.partitions.insert(partition, slot);
        Ok(slot)
    }

    /// finish releases a partition once its workload has completed AND its offset has
    /// been committed.
    fn finish(&mut self, partition: i32) {
        self.partitions.remove(&partition);
    }

    fn len(&self) -> usize {
        self.partitions.len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }
}

/// run starts the bot-fleet worker loop.
/// It consumes workload assignments, executes each one, and exits on a
/// shutdown signal (SIGINT/Ctrl-C or SIGTERM from kubelet).
pub async fn run(mut config: Config) -> Result<()> {
    kafka::ensure_topics(
        &config.kafka_brokers,
        &[
            &config.workload_topic,
            &config.barrier_topic,
            &config.ready_topic,
            &config.orders_sent_topic,
        ],
    )
    .await?;

    let control_producer = kafka::control_producer(&config.kafka_brokers)?; // the one which sends ready signals back to bot fleet controller
    let telemetry_producer = kafka::telemetry_producer(&config.kafka_brokers)?; // the one which is responsible for sending orders.acked to kafka
    if let Some(n) = kafka::topic_partition_count(&telemetry_producer, &config.orders_sent_topic) {
        if n != config.orders_partitions {
            tracing::info!(
                env = config.orders_partitions,
                topic = n,
                "orders.sent partition count from metadata overrides ORDERS_PARTITIONS"
            );
        }
        config.orders_partitions = n;
    }
    let workload_consumer = kafka::consumer(
        &config.kafka_brokers,
        &config.consumer_group,
        &[&config.workload_topic],
        config.max_poll_interval,
    )?;

    info!(
        worker_id = %config.worker_id,
        workload_topic = %config.workload_topic,
        "bot worker started"
    );

    let cancel = CancelToken::new();
    {
        let cancel = cancel.clone(); // clone does not create a independent copy of the flag.
        tokio::spawn(async move {
            let mut sigterm =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(err) => {
                        error!(error = %err, "failed to install SIGTERM handler");
                        return;
                    }
                };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => info!("SIGINT received; shutting down"),
                _ = sigterm.recv() => info!("SIGTERM received; shutting down"),
            }
            cancel.cancel();
        });
    }

    // Workloads execute on spawned tasks rather than inline, so one worker can drive
    // several sessions at once. `in_flight` enforces one workload per partition plus a
    // global cap; `joinset` owns the running tasks and yields (partition, message) so
    // the completion handler can commit exactly that partition's offset.
    let cap = max_concurrent_workloads();
    let mut in_flight = InFlight::new();
    let mut joinset: tokio::task::JoinSet<(i32, kafka::KafkaMessage, bool)> =
        tokio::task::JoinSet::new();
    // task id -> partition, so a PANICKED task's partition can still be released. A
    // JoinError carries no payload, so without this side table a panic would leave the
    // partition reserved forever and silently stop that partition being served for the
    // pod's lifetime.
    let mut task_partitions: std::collections::HashMap<tokio::task::Id, i32> =
        std::collections::HashMap::new();
    info!(
        max_concurrent_workloads = cap,
        "workload execution concurrency"
    );

    loop {
        // At capacity, stop receiving and drain instead. Kafka redelivers anything we
        // have not committed, so declining to poll is backpressure, not loss — and it
        // is why nothing here ever drops a spec it cannot run.
        let can_accept = in_flight.len() < cap;

        tokio::select! {
            _ = cancel.cancelled() => {
                info!(in_flight = in_flight.len(), "shutdown requested; draining in-flight workloads");
                // Let running workloads finish and commit: aborting them would leave
                // their offsets uncommitted and duplicate the shard on restart. The
                // workloads observe the same CancelToken, so they wind themselves down.
                while let Some(joined) = joinset.join_next_with_id().await {
                    complete_workload(&workload_consumer, &mut in_flight, &mut task_partitions, joined);
                }
                return Ok(());
            }

            joined = joinset.join_next_with_id(), if !joinset.is_empty() => {
                if let Some(joined) = joined {
                    complete_workload(&workload_consumer, &mut in_flight, &mut task_partitions, joined);
                }
            }

            message = kafka::recv_message(&workload_consumer), if can_accept => {
                let message = message.context("read workload assignment")?;
                let Some(payload) = message.payload.as_deref() else {
                    kafka::commit_message(&workload_consumer, &message)
                        .context("commit null workload assignment")?;
                    continue;
                };
                let spec = match kafka::decode_workload(payload) {
                    Ok(spec) => spec,
                    Err(err) => {
                        warn!(error = %err, "skipping invalid workload assignment");
                        kafka::commit_message(&workload_consumer, &message)
                            .context("commit invalid workload assignment")?;
                        continue;
                    }
                };

                if is_spec_stale(spec.published_at_unix_ns, unix_nanos(), workload_spec_max_age_s()) {
                    let age_s = unix_nanos().saturating_sub(spec.published_at_unix_ns) / 1_000_000_000;
                    warn!(
                        session_id = %spec.session_id,
                        age_s,
                        "skipping stale workload assignment (published_at_unix_ns beyond WORKLOAD_SPEC_MAX_AGE_S)"
                    );
                    metrics::stale_workload_skipped();
                    kafka::commit_message(&workload_consumer, &message)
                        .context("commit stale workload assignment")?;
                    continue;
                }

                let partition = message.partition();
                match in_flight.try_admit(partition, cap) {
                    Ok(slot) => {
                        let session_id = spec.session_id.clone();
                        let cfg = config.clone();
                        let ctrl = control_producer.clone();
                        let tele = telemetry_producer.clone();
                        let tok = cancel.clone();
                        let handle = joinset.spawn(async move {
                            let ok = match run_workload(&cfg, &ctrl, &tele, spec, tok, slot).await {
                                Ok(()) => true,
                                Err(err) => {
                                    error!(error = %err, session_id = %session_id, "workload failed");
                                    false
                                }
                            };
                            (partition, message, ok)
                        });
                        task_partitions.insert(handle.id(), partition);
                        metrics::workloads_in_flight(in_flight.len());
                    }
                    Err(reason) => {
                        // Deliberately NOT committed: the spec stays on the partition
                        // and is redelivered. Reaching this arm means the group handed
                        // this pod a partition whose predecessor is still running,
                        // which after the controller's pre-scale gate should be rare.
                        warn!(
                            session_id = %spec.session_id,
                            partition,
                            in_flight = in_flight.len(),
                            reason = ?reason,
                            "workload not admitted; leaving uncommitted for redelivery"
                        );
                    }
                }
            }
        }
    }
}

/// complete_workload records the outcome of a finished workload, commits its
/// partition's offset, and frees the partition for the next spec.
///
/// Commit happens only after the workload has finished — at-least-once, matching the
/// previous inline behaviour. Committing at admission would be at-most-once and would
/// silently lose a shard whenever a pod died mid-run.
fn complete_workload(
    consumer: &kafka::KafkaConsumer,
    in_flight: &mut InFlight,
    task_partitions: &mut std::collections::HashMap<tokio::task::Id, i32>,
    joined: Result<(tokio::task::Id, (i32, kafka::KafkaMessage, bool)), tokio::task::JoinError>,
) {
    match joined {
        Ok((id, (partition, message, ok))) => {
            task_partitions.remove(&id);
            if ok {
                metrics::workload_ok();
            } else {
                metrics::workload_error();
            }
            // A commit failure is logged, not fatal: the offset stays where it was, so
            // the spec is redelivered and the staleness guard decides whether to run
            // it. Killing the worker here would strand every other in-flight workload.
            if let Err(err) = kafka::commit_message(consumer, &message) {
                error!(error = %err, partition, "commit handled workload assignment failed");
            }
            in_flight.finish(partition);
        }
        Err(err) => {
            // The spawned task panicked, so there is no message to commit — the spec
            // stays uncommitted and Kafka redelivers it, which is the behaviour we
            // want. But the partition MUST be released or this pod stops serving it
            // for the rest of its life; the id side table is what makes that possible.
            metrics::workload_error();
            let id = err.id();
            match task_partitions.remove(&id) {
                Some(partition) => {
                    in_flight.finish(partition);
                    error!(error = %err, partition, "workload task panicked; partition released, spec left for redelivery");
                }
                None => {
                    error!(error = %err, "workload task panicked with no tracked partition");
                }
            }
        }
    }
    metrics::workloads_in_flight(in_flight.len());
}

/// run_workload performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run_workload(
    config: &Config,
    control_producer: &KafkaProducer,
    telemetry_producer: &KafkaProducer,
    spec: WorkloadSpec,
    cancel: CancelToken,
    slot: usize,
) -> Result<()> {
    validate_spec(config, &spec)?;

    // The barrier group must be unique per CONCURRENT SLOT on this pod. The barrier
    // topic is broadcast — every workload must receive every barrier — but two
    // concurrent workloads sharing one group become COMPETING MEMBERS of it, and the
    // barrier partition can only be assigned to one of them. The loser never receives
    // the barrier, never fires, and its shard's load silently goes missing. Observed in
    // a 3-shard run: one pod prepared two shards, logged a single "barrier received",
    // and the session delivered 58% of its scenario.
    //
    // Keyed on a SLOT index rather than session_id deliberately. wait_for_barrier
    // already filters by session_id, so per-session groups are unnecessary — and they
    // would be created fresh every run and never deleted, leaking consumer groups on
    // the broker (the reason the group was made per-worker in the first place, commit
    // 0ed8b9e). Slot indices are bounded by MAX_CONCURRENT_WORKLOADS, so the set of
    // groups a pod ever creates is small and fully reused across runs.
    let barrier_group = barrier_group(&config.consumer_group, &config.worker_id, slot);
    let barrier_consumer = kafka::consumer(
        &config.kafka_brokers,
        &barrier_group,
        &[&config.barrier_topic],
        config.max_poll_interval,
    )?;

    info!(
        session_id = %spec.session_id,
        protocol = ?spec.protocol,
        task_count = spec.tasks.len(),
        worker_index = spec.worker_index,
        worker_count = spec.worker_count,
        "preparing workload"
    );
    metrics::tasks_assigned(spec.tasks.len());

    let connected = connect_tasks(&spec).await?;
    let connected_count = connected.len() as u32;
    metrics::tasks_connected(connected.len());

    let ready = ReadySignal {
        session_id: spec.session_id.clone(),
        submission_id: spec.submission_id.clone(),
        worker_id: config.worker_id.clone(),
        worker_index: spec.worker_index,
        worker_count: spec.worker_count,
        task_count: spec.tasks.len() as u32,
        connected_count,
        ready_at_unix_nanos: unix_nanos(),
    };
    kafka::publish_json(
        control_producer,
        &config.ready_topic,
        &kafka::ready_key(&ready),
        &ready,
    )
    .await?;
    info!(session_id = %spec.session_id, connected_count, "published ready signal");

    let barrier =
        kafka::wait_for_barrier(&barrier_consumer, &spec.session_id, BARRIER_WAIT).await?;
    let barrier_epoch_ns = barrier.target_epoch_unix_nanos;
    // Logged so a multi-shard run can be checked for a COMMON start epoch: the
    // controller publishes one barrier per session after full ready fan-in, so every
    // shard of a session — including shards on other pods — must report the same value
    // here. Distinct values would mean the scenario's shape was smeared across pods
    // rather than applied together.
    info!(
        session_id = %spec.session_id,
        worker_index = spec.worker_index,
        barrier_epoch_ns,
        "barrier received"
    );

    let telemetry = TelemetrySink::new(
        telemetry_producer.clone(),
        config.orders_sent_topic.clone(),
        spec.session_id.clone(),
        config.worker_id.clone(),
        config.telemetry_channel_capacity,
        config.telemetry_flush_interval,
        config.telemetry_batch_size,
        config.orders_partitions,
        spec.order_band,
    );

    // 1s send-health snapshot. Logs to the pod's own stdout (survives a dead
    // port-forward and is finer than the 15s Prometheus scrape), giving a
    // second-by-second timeline to align against the eBPF capture's decoded/
    // ringbuf_dropped log. The cliff diagnosis: orders_sent rate -> 0 with
    // write_block spiking and inflight pinned high == contestant stopped
    // draining; orders_sent rate steady while eBPF decoded craters == capture.
    let snapshot_session = spec.session_id.clone();
    let snapshot = tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut last_sent = metrics::orders_sent_value();
        let mut last_writes = metrics::writes_value();
        let start = unix_nanos();
        loop {
            ticker.tick().await;
            let now_sent = metrics::orders_sent_value();
            let rate = now_sent.saturating_sub(last_sent);
            last_sent = now_sent;
            let now_writes = metrics::writes_value();
            let writes = now_writes.saturating_sub(last_writes);
            last_writes = now_writes;
            let max_block_ms = metrics::take_max_write_block_ns() as f64 / 1e6;
            // avg_batch = orders per write_all this second; with max_batch these are
            // the t1-fidelity signals the task-count sweep keys on (1.0 = every
            // order got its own write and timestamp).
            let avg_batch = if writes > 0 {
                rate as f64 / writes as f64
            } else {
                0.0
            };
            info!(
                session_id = %snapshot_session,
                t_s = (unix_nanos().saturating_sub(start)) / 1_000_000_000,
                orders_sent_total = now_sent,
                send_rate_per_s = rate,
                inflight = metrics::inflight_value(),
                max_write_block_ms = max_block_ms,
                avg_batch = format!("{avg_batch:.2}").as_str(),
                max_batch = metrics::take_max_batch_size(),
                max_slip_ms = format!("{:.3}", metrics::take_max_slip_ns() as f64 / 1e6).as_str(),
                write_errors = metrics::write_errors_value(),
                "bot send snapshot"
            );
        }
    });

    let result = fire_workload(
        config,
        &spec,
        connected,
        barrier_epoch_ns,
        telemetry.clone(),
        cancel,
    )
    .await;
    snapshot.abort();
    telemetry.close().await?;
    result
}

/// validate_spec performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn validate_spec(config: &Config, spec: &WorkloadSpec) -> Result<()> {
    if spec.tasks.is_empty() {
        return Err(
            crate::errors::BotFleetError::ValidationError("tasks list is empty".into()).into(),
        );
    }
    if spec.tasks.len() > config.max_bots_per_worker {
        return Err(crate::errors::BotFleetError::ValidationError(format!(
            "task count {} exceeds config max_bots_per_worker {}",
            spec.tasks.len(),
            config.max_bots_per_worker
        ))
        .into());
    }
    for task in &spec.tasks {
        // target_rps == 0 is the max-rate sentinel (no pacer, send back-to-back); it is
        // a valid spec, not an error.
        if task.duration_ns == 0 {
            return Err(crate::errors::BotFleetError::ValidationError(format!(
                "task {} has duration_ns=0",
                task.task_id
            ))
            .into());
        }
    }
    if spec.worker_count == 0 || spec.worker_index >= spec.worker_count {
        return Err(crate::errors::BotFleetError::ValidationError(
            "invalid worker index/count".into(),
        )
        .into());
    }
    let worst_case_ns = worst_case_wall_time_ns(spec);
    let poll_ceiling_ns = config.max_poll_interval.as_nanos() as u64;
    if worst_case_ns >= poll_ceiling_ns {
        return Err(crate::errors::BotFleetError::ValidationError(format!(
            "worst-case wall time {worst_case_ns}ns (barrier wait + offset + duration + drain) \
             would meet or exceed max.poll.interval.ms ceiling {poll_ceiling_ns}ns; the \
             assignment offset commits only after the run, so Kafka would rebalance and \
             re-deliver mid-run (duplicate execution)"
        ))
        .into());
    }
    validate_identifier("session_id", &spec.session_id)?;
    validate_identifier("submission_id", &spec.submission_id)?;
    validate_identifier("fix_version", &spec.fix_version)?;
    Ok(())
}

/// barrier_group performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn barrier_group(consumer_group: &str, worker_id: &str, slot: usize) -> String {
    format!("{consumer_group}-barrier-{worker_id}-slot{slot}")
}

/// pacer_interval_ns computes the fixed inter-send interval for a paced task.
/// `target_rps == 0` is the max-rate sentinel: no pacer, so this returns `None` and
/// callers must send back-to-back instead of scheduling on a fixed cadence.
fn pacer_interval_ns(target_rps: u32) -> Option<u64> {
    if target_rps == 0 {
        None
    } else {
        Some(1_000_000_000_u64 / u64::from(target_rps))
    }
}

/// worst_case_wall_time_ns performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn worst_case_wall_time_ns(spec: &WorkloadSpec) -> u64 {
    let max_task_span_ns = spec
        .tasks
        .iter()
        .map(|t| t.start_offset_ns.saturating_add(t.duration_ns))
        .max()
        .unwrap_or(0);
    (BARRIER_WAIT.as_nanos() as u64)
        .saturating_add(max_task_span_ns)
        .saturating_add(RESPONSE_TIMEOUT_NS)
}

/// validate_identifier performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn validate_identifier(name: &str, value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));

    if !valid {
        return Err(crate::errors::BotFleetError::ValidationError(format!(
            "{name} contains unsupported characters"
        ))
        .into());
    }

    Ok(())
}

/// connect_tasks performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn connect_tasks(spec: &WorkloadSpec) -> Result<Vec<ConnectedTask>> {
    let targets = spec.resolved_targets();
    let mut set = JoinSet::new();
    let shared_spec = Arc::new(spec.clone());
    let shared_targets = Arc::new(targets);

    for task in &spec.tasks {
        let spec = Arc::clone(&shared_spec);
        let targets = Arc::clone(&shared_targets);
        let task = task.clone();
        set.spawn(async move {
            let target = resolve_task_target(&targets, &task);
            let addr = resolve_target(&spec.target_host, target.port).await?;
            let client = TargetClient::connect(&spec, target, addr).await?;
            Ok::<_, anyhow::Error>(ConnectedTask {
                task,
                target_host: spec.target_host.clone(),
                client,
            })
        });
    }

    let mut tasks = Vec::with_capacity(spec.tasks.len());
    while let Some(result) = set.join_next().await {
        match result.context("join task connect")? {
            Ok(t) => tasks.push(t),
            Err(err) => {
                metrics::connect_failure();
                warn!(error = %err, "task connect failed; dropping");
            }
        }
    }
    Ok(tasks)
}

/// resolve_task_target picks a task's connection target from the spec's
/// resolved targets table by `target_idx`, falling back to the first target
/// if the index is out of range (defensive against a malformed message).
fn resolve_task_target(targets: &[TargetSpec], task: &TaskSpec) -> TargetSpec {
    *targets.get(task.target_idx as usize).unwrap_or(&targets[0])
}

/// resolve_target performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn resolve_target(host: &str, port: u16) -> Result<SocketAddr> {
    let target = format!("{host}:{port}");
    let mut addrs = lookup_host(&target)
        .await
        .with_context(|| format!("resolve target host {target}"))?;
    addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("no addresses resolved for target host {target}"))
}

/// fire_workload performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn fire_workload(
    config: &Config,
    spec: &WorkloadSpec,
    tasks: Vec<ConnectedTask>,
    barrier_epoch_ns: u64,
    telemetry: TelemetrySink,
    cancel: CancelToken,
) -> Result<()> {
    let mut set = JoinSet::new();
    let write_timeout = Duration::from_millis(spec.write_timeout_ms);

    for ct in tasks {
        let session_id = spec.session_id.clone();
        let submission_id = spec.submission_id.clone();
        let worker_id = config.worker_id.clone();
        let fix_version = spec.fix_version.clone();
        let global_seed = spec.global_seed;
        let telemetry = telemetry.clone();
        let cancel = cancel.clone();
        let max_inflight = config.max_inflight_per_task;
        let write_batch = config.write_batch;
        set.spawn(async move {
            ct.run(
                &session_id,
                &submission_id,
                &worker_id,
                &fix_version,
                global_seed,
                barrier_epoch_ns,
                write_timeout,
                telemetry,
                cancel,
                max_inflight,
                write_batch,
            )
            .await
        });
    }

    let mut sent_total = 0u64;
    while let Some(result) = set.join_next().await {
        match result.context("join task send loop")? {
            Ok(sent) => sent_total += sent,
            Err(err) => {
                warn!(error = %err, "task send loop failed; dropped");
            }
        }
    }
    info!(session_id = %spec.session_id, sent = sent_total, "workload completed");
    Ok(())
}

/// ConnectedTask stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct ConnectedTask {
    task: TaskSpec,
    target_host: String,
    client: TargetClient,
}

#[derive(Clone)]
/// PendingOrder stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct PendingOrder {
    order_id: String,
    orig_order_id: String,
    target_send_ts_ns: u64,
    send_ts_ns: u64,
    barrier_epoch_ns: u64,
    price: u64,
    qty: u64,
    side: Side,
    payload_type: PayloadType,
    ord_type: OrdType,
    /// Self-match-prevention id this order was sent under, or `SMP_ID_NONE` when the
    /// task carries no SMP id. Captured here at send time so the telemetry event
    /// reports what was actually put on the wire rather than re-deriving it later.
    smp_id: u32,
}

type PendingMap = Arc<Mutex<HashMap<String, PendingOrder>>>;

// P3: watchdog expiry without a full-map scan per tick. Orders are inserted (and
// written) in send order within a single task's writer, so `send_ts_ns` — and
// therefore `deadline_ns = send_ts_ns + RESPONSE_TIMEOUT_NS` — is monotonically
// non-decreasing across pushes. That makes a FIFO queue sufficient: the watchdog
// only ever needs to pop from the front. Entries are pushed once a write succeeds
// (mirrors the map insert timing) and popped either when their deadline is due or
// on the task's final tick (`last_tick`), which must drain everything regardless
// of deadline — matching the prior `HashMap::retain` semantics where `last_tick`
// forced eviction of every remaining entry. An id may already be gone from the
// map (acked, or evicted by a previous pass) by the time it's popped; that's not
// an error, it's just skipped.
/// ExpiryEntry is the compact per-order record on the watchdog's timeout queue:
/// (deadline, task-local seq, order-kind letter) — 16 bytes vs ~50-60 for the
/// old (deadline, String order_id) tuple. The queue deliberately keeps entries
/// until their deadline even after an early ack (middle-removal from a VecDeque
/// would cost the O(due)-per-tick watchdog win), so it holds every order sent in
/// the last RESPONSE_TIMEOUT window — compactness is what makes that acceptable.
/// The full order id is rebuilt via fix::join_order_id only on eviction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExpiryEntry {
    deadline_ns: u64,
    seq: u32,
    kind: u8,
}

type ExpiryQueue = Arc<Mutex<VecDeque<ExpiryEntry>>>;

/// Removes each of `order_ids` from `pending` (if still present) under a single
/// lock and returns how many were actually removed. Used on the write-error
/// cleanup paths (FIX and REST/WS): a batch write can fail after the read/ack
/// loop has already removed (and `inflight_sub`'d) some of the same batch's
/// orders concurrently, so the caller must sub the gauge by this actual-removal
/// count, not the original batch size — otherwise already-acked entries get
/// subtracted twice and the shared inflight gauge drifts negative.
fn remove_batch_from_pending<'a, I>(pending: &PendingMap, order_ids: I) -> usize
where
    I: IntoIterator<Item = &'a str>,
{
    let mut map = pending.lock().expect("pending map poisoned");
    order_ids
        .into_iter()
        .filter(|id| map.remove(*id).is_some())
        .count()
}

/// Pops every queue entry that is due (`deadline_ns <= now_ns`), or all of them if
/// `last_tick`, preserving FIFO order. Pure and unit-testable independent of the
/// pending map / telemetry plumbing around it.
fn pop_due_expirations(
    queue: &mut VecDeque<ExpiryEntry>,
    now_ns: u64,
    last_tick: bool,
) -> Vec<ExpiryEntry> {
    let mut ids = Vec::new();
    while let Some(&entry) = queue.front() {
        if !last_tick && entry.deadline_ns > now_ns {
            break;
        }
        queue.pop_front();
        ids.push(entry);
    }
    ids
}

impl ConnectedTask {
    #[allow(clippy::too_many_arguments)]
    /// run performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn run(
        self,
        session_id: &str,
        submission_id: &str,
        worker_id: &str,
        fix_version: &str,
        global_seed: u64,
        barrier_epoch_ns: u64,
        write_timeout: Duration,
        telemetry: TelemetrySink,
        cancel: CancelToken,
        max_inflight: usize,
        write_batch: usize,
    ) -> Result<u64> {
        let ctx = TaskContext {
            session_id: session_id.to_string(),
            submission_id: submission_id.to_string(),
            worker_id: worker_id.to_string(),
            target_host: self.target_host,
            fix_version: fix_version.to_string(),
            global_seed,
            barrier_epoch_ns,
            write_timeout,
            telemetry,
            cancel,
            max_inflight,
            write_batch,
        };

        match self.client {
            TargetClient::Fix(fix) => run_fix_task(self.task, fix, ctx).await,
            TargetClient::Rest(stream) => run_rest_task(self.task, stream, ctx).await,
            TargetClient::Ws(ws) => run_ws_task(self.task, ws, ctx).await,
        }
    }
}

/// TaskContext stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct TaskContext {
    session_id: String,
    submission_id: String,
    worker_id: String,
    target_host: String,
    fix_version: String,
    global_seed: u64,
    barrier_epoch_ns: u64,
    write_timeout: Duration,
    telemetry: TelemetrySink,
    cancel: CancelToken,
    max_inflight: usize,
    write_batch: usize,
}

/// FixConnection stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct FixConnection {
    read_half: ReadHalf<TcpStream>,
    write_half: WriteHalf<TcpStream>,
}

/// run_fix_task performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run_fix_task(task: TaskSpec, fix: FixConnection, ctx: TaskContext) -> Result<u64> {
    let task_start_ns = ctx.barrier_epoch_ns.saturating_add(task.start_offset_ns);
    let task_end_ns = task_start_ns.saturating_add(task.duration_ns);
    let drain_end_ns = task_end_ns.saturating_add(RESPONSE_TIMEOUT_NS);

    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let notify: Arc<Notify> = Arc::new(Notify::new());
    let expiry: ExpiryQueue = Arc::new(Mutex::new(VecDeque::new()));

    let mut set: JoinSet<Result<u64>> = JoinSet::new();

    set.spawn(fix_write_loop(
        fix.write_half,
        task.clone(),
        pending.clone(),
        expiry.clone(),
        notify.clone(),
        ctx.session_id.clone(),
        ctx.target_host.clone(),
        ctx.fix_version.clone(),
        ctx.global_seed,
        task_start_ns,
        task_end_ns,
        ctx.write_timeout,
        ctx.cancel.clone(),
        ctx.max_inflight,
        ctx.write_batch,
    ));
    set.spawn(fix_read_loop(
        fix.read_half,
        task.task_id,
        pending.clone(),
        notify.clone(),
        ctx.telemetry.clone(),
        ctx.session_id.clone(),
        ctx.submission_id.clone(),
        ctx.worker_id.clone(),
        drain_end_ns,
    ));
    set.spawn(watchdog_loop(
        task.task_id,
        pending.clone(),
        expiry.clone(),
        notify.clone(),
        ctx.telemetry.clone(),
        ctx.session_id.clone(),
        ctx.submission_id.clone(),
        ctx.worker_id.clone(),
        drain_end_ns,
    ));

    let mut sent_total = 0u64;
    while let Some(result) = set.join_next().await {
        match result.context("join FIX sub-task")? {
            Ok(n) => sent_total += n,
            Err(err) => {
                warn!(task_id = task.task_id, error = %err, "FIX sub-task ended with error");
            }
        }
    }
    Ok(sent_total)
}

/// mix_from_task performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn mix_from_task(task: &TaskSpec) -> content::OrderMix {
    content::OrderMix {
        market_pct: task.market_pct,
        cancel_pct: task.cancel_pct,
        replace_pct: task.replace_pct,
    }
}

/// render_frame patches `action` into `cache`'s per-`FrameKind` template (P2: no
/// more `format!`-rebuilding the whole frame from scratch every order). `cache`
/// is created once per task (see `fix::TemplateCache`) and lives for the task's
/// lifetime; only ClOrdID-width rollovers and (for REST/WS) side/qty/price
/// digit-width changes force it to re-render a kind's template.
fn render_frame(cache: &mut fix::TemplateCache, action: &content::Action) -> fix::OrderFrame {
    use content::Action;
    match action {
        Action::NewLimit {
            seq,
            price,
            qty,
            side,
        } => cache.render_new(u64::from(*seq), *price, *qty, *side),
        Action::NewMarket { seq, qty, side } => cache.render_market(u64::from(*seq), *qty, *side),
        Action::Cancel {
            seq,
            orig_order_id,
            price,
            qty,
            side,
        } => cache.render_cancel(u64::from(*seq), orig_order_id, *price, *qty, *side),
        Action::Replace {
            seq,
            orig_order_id,
            price,
            qty,
            side,
        } => cache.render_replace(u64::from(*seq), orig_order_id, *price, *qty, *side),
    }
}

#[allow(clippy::too_many_arguments)]
/// fix_write_loop performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn fix_write_loop(
    mut write_half: WriteHalf<TcpStream>,
    task: TaskSpec,
    pending: PendingMap,
    expiry: ExpiryQueue,
    notify: Arc<Notify>,
    session_id: String,
    target_host: String,
    fix_version: String,
    global_seed: u64,
    task_start_ns: u64,
    task_end_ns: u64,
    // Intentionally unused: a per-write timeout is wrong for a load generator (see the write
    // site) — it turns sink backpressure into permanent task death and collapses aggregate load.
    _write_timeout: Duration,
    cancel: CancelToken,
    max_inflight: usize,
    batch_max: usize,
) -> Result<u64> {
    time::sleep_until(instant_from_unix_nanos(task_start_ns)).await;

    // batch_max (Config::write_batch, env BOT_WRITE_BATCH) coalesces up to N already-DUE
    // orders into a single write_all, so one write() syscall + one reactor round-trip
    // amortises across many orders — the dominant per-order cost found by profiling
    // (CPU-bound, ~42us/order, mostly the per-order write().await). Default 64; 1
    // reproduces legacy per-order sends. Pacing is unchanged: only orders past their
    // schedule are batched (catch-up), so an under-the-ceiling task still paces
    // normally and just writes batches of one.

    let max_rate = task.target_rps == 0;
    let interval_ns = pacer_interval_ns(task.target_rps).unwrap_or(0);
    let mut next_send_ns = task_start_ns;
    let barrier_epoch_ns = task_start_ns.saturating_sub(task.start_offset_ns);
    let mut generator = TaskGenerator::new(
        session_id.clone(),
        u64::from(task.task_id),
        task.profile,
        mix_from_task(&task),
        global_seed ^ u64::from(task.task_id),
    );
    let mut sent: u64 = 0;

    // One template per FrameKind, reused for the task's whole lifetime (P2).
    let mut template_cache = fix::TemplateCache::with_smp(
        Protocol::Fix,
        &fix_version,
        &session_id,
        &target_host,
        u64::from(task.task_id),
        task.smp_id_count,
    );

    // Reused across iterations to avoid per-batch allocation.
    let mut frames: Vec<OrderFrame> = Vec::with_capacity(batch_max);
    let mut targets: Vec<u64> = Vec::with_capacity(batch_max);
    let mut batch_buf: Vec<u8> = Vec::with_capacity(batch_max * 256);

    loop {
        if should_stop_sending(&cancel, task_end_ns) {
            break;
        }

        // In-flight backpressure: if too many orders are awaiting acks (contestant can't keep
        // up, or telemetry is stalled), wait for pending to drain — via acks or watchdog
        // eviction — before issuing more, instead of growing memory unbounded. Woken by
        // `notify` (signaled wherever the pending map shrinks) instead of polling every 1ms.
        while pending.lock().expect("pending map poisoned").len() >= max_inflight {
            if should_stop_sending(&cancel, task_end_ns) {
                return Ok(sent);
            }
            let notified = notify.notified();
            tokio::pin!(notified);
            if pending.lock().expect("pending map poisoned").len() < max_inflight {
                break;
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = cancel.cancelled() => return Ok(sent),
            }
        }

        // Pace: park only when genuinely ahead of the next due order. Max-rate mode
        // (target_rps == 0) has no schedule, so never parks here.
        if !max_rate && next_send_ns > unix_nanos() {
            tokio::select! {
                _ = time::sleep_until(instant_from_unix_nanos(next_send_ns)) => {}
                _ = cancel.cancelled() => break,
            }
        }

        // Collect every order that is now due (up to batch_max) into one buffer. In
        // max-rate mode every order is "due" immediately — fill the batch back-to-back.
        frames.clear();
        targets.clear();
        batch_buf.clear();
        let now_ns = unix_nanos();
        while frames.len() < batch_max && (max_rate || next_send_ns <= now_ns) {
            let action = generator.next();
            let mut frame = render_frame(&mut template_cache, &action);
            frame.patch_timestamp(unix_nanos());
            batch_buf.extend_from_slice(&frame.bytes);
            targets.push(next_send_ns);
            frames.push(frame);
            if !max_rate {
                next_send_ns = next_send_ns.saturating_add(interval_ns);
            }
        }
        if frames.is_empty() {
            continue;
        }
        let count = frames.len();

        // Insert the whole batch under a single lock. send_ts is provisionally the
        // batch's write-start time, not 0: TCP can deliver the batch's first frames
        // (and the peer can ack them) while write_all is still blocked on the rest —
        // an ack racing the write must not emit telemetry with send_ts=0. The
        // post-write patch below overwrites with the accurate timestamp for
        // everything still pending.
        let write_start_ns = unix_nanos();
        {
            let mut map = pending.lock().expect("pending map poisoned");
            for (frame, &target) in frames.iter().zip(targets.iter()) {
                map.insert(
                    frame.order_id.clone(),
                    PendingOrder {
                        order_id: frame.order_id.clone(),
                        orig_order_id: frame.orig_order_id.clone(),
                        target_send_ts_ns: target,
                        send_ts_ns: write_start_ns, // refined after write_all returns
                        barrier_epoch_ns,
                        price: frame.price,
                        qty: frame.qty,
                        side: frame.side,
                        payload_type: frame.payload_type,
                        ord_type: frame.ord_type,
                        smp_id: frame.smp_id,
                    },
                );
            }
        }
        metrics::inflight_add(count);

        // Block on the write rather than imposing a per-write timeout. When the contestant
        // can't drain fast enough its TCP receive window fills and write_all stalls; TCP flow
        // control then paces THIS task down to the contestant's real service rate, so aggregate
        // load plateaus at the sink's capacity instead of overshooting. A short timeout here did
        // the opposite: write_all isn't cancel-safe, so a timeout left a partial frame and forced
        // the task to exit — under sustained backpressure every loaded task exited at once and the
        // offered load collapsed to zero (the symptom we saw on ramp; a drain sink never fills the
        // window so it never tripped). Cancellation still tears the loop down promptly at session
        // end, and a genuinely dead peer surfaces as a write error below.
        // Third arm: the run's drain deadline. Backpressure from a slow-but-
        // draining peer is handled by blocking (see above); a peer that never
        // reads again must not wedge this task past the point where the run is
        // over regardless. Abandoning the write here pairs with the watchdog's
        // last-tick pending sweep, which accounts the batch as timed_out.
        let drain_end_ns = task_end_ns.saturating_add(RESPONSE_TIMEOUT_NS);
        let write_res = tokio::select! {
            res = write_half.write_all(&batch_buf) => res,
            _ = cancel.cancelled() => break,
            _ = time::sleep_until(instant_from_unix_nanos(drain_end_ns)) => {
                metrics::order_write_error(metrics::protocol_label(Protocol::Fix));
                warn!(task_id = task.task_id, "write still blocked at drain deadline; abandoning stalled peer");
                break;
            }
        };
        match write_res {
            Ok(()) => {
                let send_ts_ns = unix_nanos();
                metrics::orders_sent_by(metrics::protocol_label(Protocol::Fix), count);
                metrics::observe_write(
                    metrics::protocol_label(Protocol::Fix),
                    send_ts_ns.saturating_sub(write_start_ns),
                    count,
                );
                {
                    let mut map = pending.lock().expect("pending map poisoned");
                    for frame in frames.iter() {
                        if let Some(p) = map.get_mut(&frame.order_id) {
                            p.send_ts_ns = send_ts_ns;
                            // Max-rate mode has no schedule: target == send time by
                            // definition, so slip downstream is exactly zero.
                            if max_rate {
                                p.target_send_ts_ns = send_ts_ns;
                            }
                        }
                    }
                }
                {
                    let deadline_ns = send_ts_ns.saturating_add(RESPONSE_TIMEOUT_NS);
                    let mut queue = expiry.lock().expect("expiry queue poisoned");
                    for frame in frames.iter() {
                        if let Some((seq, kind)) = fix::split_order_id(&frame.order_id) {
                            queue.push_back(ExpiryEntry {
                                deadline_ns,
                                seq,
                                kind,
                            });
                        }
                        // A non-splitting id (impossible for generator output) simply
                        // gets no queue entry; the watchdog's last-tick pending sweep
                        // still accounts for it.
                    }
                }
                if !max_rate {
                    for &target in targets.iter() {
                        metrics::observe_slip(
                            metrics::protocol_label(Protocol::Fix),
                            send_ts_ns.saturating_sub(target),
                        );
                    }
                }
                sent += count as u64;
            }
            Err(err) => {
                let removed =
                    remove_batch_from_pending(&pending, frames.iter().map(|f| f.order_id.as_str()));
                metrics::inflight_sub(removed);
                metrics::order_write_error(metrics::protocol_label(Protocol::Fix));
                warn!(
                    task_id = task.task_id,
                    error = %err, "FIX batch write failed; task writer exiting"
                );
                return Ok(sent);
            }
        }
    }

    Ok(sent)
}

#[allow(clippy::too_many_arguments)]
/// fix_read_loop performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn fix_read_loop(
    mut read_half: ReadHalf<TcpStream>,
    task_id: u32,
    pending: PendingMap,
    notify: Arc<Notify>,
    telemetry: TelemetrySink,
    session_id: String,
    submission_id: String,
    worker_id: String,
    drain_end_ns: u64,
) -> Result<u64> {
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];

    loop {
        let now_ns = unix_nanos();
        if now_ns >= drain_end_ns {
            break;
        }
        let remaining = Duration::from_nanos(drain_end_ns - now_ns);

        let n = match time::timeout(remaining, read_half.read(&mut chunk)).await {
            Ok(Ok(0)) => {
                break;
            }
            Ok(Ok(n)) => n,
            Ok(Err(err)) => {
                warn!(task_id, error = %err, "FIX read failed; reader exiting");
                break;
            }
            Err(_) => break,
        };
        buf.extend_from_slice(&chunk[..n]);

        let (messages, consumed) = fix::parse_messages(&buf);
        for msg in messages {
            if msg.msg_type != b"8" {
                continue;
            }
            let Some(clord_id_bytes) = msg.clord_id else {
                continue;
            };
            let Ok(clord_id) = std::str::from_utf8(clord_id_bytes) else {
                continue;
            };

            let pending_order = {
                let mut map = pending.lock().expect("pending map poisoned");
                map.remove(clord_id)
            };
            let Some(p) = pending_order else { continue };
            metrics::inflight_sub(1);
            notify.notify_one();

            let recv_done_ts_ns = unix_nanos();
            telemetry
                .record(OrderSentEvent {
                    session_id: session_id.clone(),
                    submission_id: submission_id.clone(),
                    worker_id: worker_id.clone(),
                    task_id,
                    order_id: p.order_id,
                    smp_id: p.smp_id,
                    target_send_ts_ns: p.target_send_ts_ns,
                    send_ts_ns: p.send_ts_ns,
                    recv_done_ts_ns,
                    timed_out: false,
                    price: p.price,
                    qty: p.qty,
                    side: p.side,
                    payload_type: p.payload_type,
                    ord_type: p.ord_type,
                    orig_order_id: p.orig_order_id,
                    barrier_epoch_ns: p.barrier_epoch_ns,
                })
                .await;
        }

        if consumed > 0 {
            buf.drain(..consumed);
        }

        if buf.len() > 1_048_576 {
            warn!(task_id, "FIX read buffer overflow; resetting");
            buf.clear();
        }
    }

    Ok(0)
}

#[allow(clippy::too_many_arguments)]
/// Evicts due orders (or, on the task's final tick, everything left) without
/// scanning the whole pending map: pops only the front of the per-task expiry
/// queue, which is deadline-ordered because a task's writer inserts orders (and
/// pushes their deadlines) in send order. Ids already removed from `pending`
/// (acked by the read loop, or evicted by a prior pass) are silently skipped —
/// they're stale queue entries, not errors.
async fn watchdog_loop(
    task_id: u32,
    pending: PendingMap,
    expiry: ExpiryQueue,
    notify: Arc<Notify>,
    telemetry: TelemetrySink,
    session_id: String,
    submission_id: String,
    worker_id: String,
    drain_end_ns: u64,
) -> Result<u64> {
    const WATCHDOG_TICK: Duration = Duration::from_millis(250);

    loop {
        let now_ns = unix_nanos();
        let last_tick = now_ns >= drain_end_ns;

        let due = {
            let mut queue = expiry.lock().expect("expiry queue poisoned");
            pop_due_expirations(&mut queue, now_ns, last_tick)
        };
        let to_evict: Vec<PendingOrder> = {
            let mut map = pending.lock().expect("pending map poisoned");
            let mut evicted: Vec<PendingOrder> = due
                .into_iter()
                .filter_map(|e| {
                    let id = fix::join_order_id(&session_id, u64::from(task_id), e.seq, e.kind);
                    map.remove(&id)
                })
                .collect();
            if last_tick {
                // Close the books (#2): orders inserted before a write that never
                // completed have no expiry entry and are invisible to the queue
                // drain above. Anything still in the map at the final tick is by
                // definition unaccounted — sweep it so every offered order ends
                // matched or timed-out and the inflight gauge returns to zero.
                // A full-map scan is exactly what the per-tick watchdog avoids;
                // once, at shutdown, it is free.
                evicted.extend(map.drain().map(|(_, p)| p));
            }
            evicted
        };

        metrics::inflight_sub(to_evict.len());
        if !to_evict.is_empty() {
            notify.notify_one();
        }
        for p in to_evict {
            telemetry
                .record(OrderSentEvent {
                    session_id: session_id.clone(),
                    submission_id: submission_id.clone(),
                    worker_id: worker_id.clone(),
                    task_id,
                    order_id: p.order_id,
                    smp_id: p.smp_id,
                    target_send_ts_ns: p.target_send_ts_ns,
                    send_ts_ns: p.send_ts_ns,
                    recv_done_ts_ns: 0,
                    timed_out: true,
                    price: p.price,
                    qty: p.qty,
                    side: p.side,
                    payload_type: p.payload_type,
                    ord_type: p.ord_type,
                    orig_order_id: p.orig_order_id,
                    barrier_epoch_ns: p.barrier_epoch_ns,
                })
                .await;
        }

        if last_tick {
            break;
        }
        time::sleep(WATCHDOG_TICK).await;
    }

    Ok(0)
}

/// RwWriter enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
enum RwWriter {
    Rest(WriteHalf<TcpStream>),
    Ws(SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, WsMessage>),
}

impl RwWriter {
    /// protocol reports which wire representation this writer expects `render_frame`
    /// to build (P1: the frame carries exactly one payload, selected up-front).
    fn protocol(&self) -> Protocol {
        match self {
            Self::Rest(_) => Protocol::Rest,
            Self::Ws(_) => Protocol::Ws,
        }
    }

    /// write_batch mirrors the FIX write loop's coalescing (P3'): REST concatenates
    /// every frame into one buffer and issues a single `write_all` (HTTP/1.1
    /// pipelining — the peer must answer in request order, verified against the
    /// sample engines separately). WS keeps tungstenite owning the socket (see
    /// module notes on the deferred raw-frame path) but batches at the sink level:
    /// `feed` queues each frame without a syscall, and the trailing `flush` issues
    /// one write for the whole batch — same syscall-amortization shape as REST
    /// without bypassing tungstenite's framing/masking.
    async fn write_batch(
        &mut self,
        frames: &mut [OrderFrame],
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        match self {
            Self::Rest(w) => {
                concat_frames(frames, scratch);
                w.write_all(scratch).await.context("write REST batch")
            }
            Self::Ws(s) => {
                for frame in frames.iter_mut() {
                    let bytes = std::mem::take(&mut frame.bytes);
                    // TEXT, not Binary. RFC 6455 defines 0x1 as a UTF-8 text frame and 0x2 as
                    // arbitrary bytes; a JSON order is text, and every mainstream
                    // JSON-over-WebSocket exchange API sends it as such. Sending binary made
                    // the platform demand something no contestant would write: an engine that
                    // handled only 0x1 received every order, framed it correctly, and then
                    // silently discarded it — 90,496 orders sent, ZERO answered, while FIX and
                    // REST on that same engine scored 0.994 and 0.996. The platform's own
                    // reference engine got this "wrong", which is the clearest possible
                    // evidence that contestants would too.
                    debug_assert!(
                        std::str::from_utf8(&bytes).is_ok(),
                        "WS order payload must be UTF-8"
                    );
                    // SAFETY: the payload is UTF-8 by construction. JsonTemplate renders it
                    // with `format!` from `String`s (serde_json::to_string for the ClOrdID,
                    // ASCII literals for symbol/side/keys) and its in-place fast path patches
                    // only ASCII digits and the BUY/SELL literal. Validating here would cost
                    // a full scan of every order body on the hot path; the debug_assert above
                    // catches any future renderer that breaks the invariant.
                    let json = unsafe { String::from_utf8_unchecked(bytes) };
                    s.feed(WsMessage::Text(json))
                        .await
                        .context("feed WS order")?;
                }
                s.flush().await.context("flush WS batch")
            }
        }
    }
}

/// Concatenates each frame's bytes into `out` (cleared first), so REST's batch
/// write is a single `write_all` over N pipelined HTTP requests.
fn concat_frames(frames: &[OrderFrame], out: &mut Vec<u8>) {
    out.clear();
    for frame in frames {
        out.extend_from_slice(&frame.bytes);
    }
}

/// run_rest_task performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run_rest_task(task: TaskSpec, stream: TcpStream, ctx: TaskContext) -> Result<u64> {
    let (read_half, write_half) = tokio::io::split(stream);
    run_readwrite_task(
        task,
        RwWriter::Rest(write_half),
        ReadSource::Rest(read_half),
        ctx,
    )
    .await
}

/// run_ws_task performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run_ws_task(
    task: TaskSpec,
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ctx: TaskContext,
) -> Result<u64> {
    let (sink, stream) = ws.split();
    run_readwrite_task(task, RwWriter::Ws(sink), ReadSource::Ws(stream), ctx).await
}

/// ReadSource enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
enum ReadSource {
    Rest(ReadHalf<TcpStream>),
    Ws(SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>),
}

/// run_readwrite_task performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run_readwrite_task(
    task: TaskSpec,
    writer: RwWriter,
    reader: ReadSource,
    ctx: TaskContext,
) -> Result<u64> {
    let task_start_ns = ctx.barrier_epoch_ns.saturating_add(task.start_offset_ns);
    let task_end_ns = task_start_ns.saturating_add(task.duration_ns);
    let drain_end_ns = task_end_ns.saturating_add(RESPONSE_TIMEOUT_NS);

    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let notify: Arc<Notify> = Arc::new(Notify::new());
    let expiry: ExpiryQueue = Arc::new(Mutex::new(VecDeque::new()));
    let mut set: JoinSet<Result<u64>> = JoinSet::new();

    set.spawn(rw_write_loop(
        writer,
        task.clone(),
        pending.clone(),
        expiry.clone(),
        notify.clone(),
        ctx.session_id.clone(),
        ctx.target_host.clone(),
        ctx.fix_version.clone(),
        ctx.global_seed,
        task_start_ns,
        task_end_ns,
        ctx.write_timeout,
        ctx.cancel.clone(),
        ctx.max_inflight,
        ctx.write_batch,
    ));
    match reader {
        ReadSource::Rest(read_half) => set.spawn(rest_read_loop(
            read_half,
            task.task_id,
            pending.clone(),
            notify.clone(),
            ctx.telemetry.clone(),
            ctx.session_id.clone(),
            ctx.submission_id.clone(),
            ctx.worker_id.clone(),
            drain_end_ns,
        )),
        ReadSource::Ws(stream) => set.spawn(ws_read_loop(
            stream,
            task.task_id,
            pending.clone(),
            notify.clone(),
            ctx.telemetry.clone(),
            ctx.session_id.clone(),
            ctx.submission_id.clone(),
            ctx.worker_id.clone(),
            drain_end_ns,
        )),
    };
    set.spawn(watchdog_loop(
        task.task_id,
        pending.clone(),
        expiry.clone(),
        notify.clone(),
        ctx.telemetry.clone(),
        ctx.session_id.clone(),
        ctx.submission_id.clone(),
        ctx.worker_id.clone(),
        drain_end_ns,
    ));

    let mut sent_total = 0u64;
    while let Some(result) = set.join_next().await {
        match result.context("join REST/WS sub-task")? {
            Ok(n) => sent_total += n,
            Err(err) => {
                warn!(task_id = task.task_id, error = %err, "REST/WS sub-task ended with error");
            }
        }
    }
    Ok(sent_total)
}

#[allow(clippy::too_many_arguments)]
/// rw_write_loop mirrors `fix_write_loop`'s batching (P3'): coalesces up to
/// `batch_max` already-due backlog orders into one write. `target_send_ts_ns` is
/// still captured per order at the moment it becomes due, before any sleep or
/// write — the coordinated-omission contract is unchanged, only the write
/// syscall is amortized across the batch.
async fn rw_write_loop(
    mut writer: RwWriter,
    task: TaskSpec,
    pending: PendingMap,
    expiry: ExpiryQueue,
    notify: Arc<Notify>,
    session_id: String,
    target_host: String,
    fix_version: String,
    global_seed: u64,
    task_start_ns: u64,
    task_end_ns: u64,
    // Intentionally unused: a per-write timeout is wrong for a load generator (see the write
    // site) — it turns sink backpressure into permanent task death and collapses aggregate load.
    // Matches the FIX path; backpressure comes from the in-flight cap + blocking write instead.
    _write_timeout: Duration,
    cancel: CancelToken,
    max_inflight: usize,
    batch_max: usize,
) -> Result<u64> {
    time::sleep_until(instant_from_unix_nanos(task_start_ns)).await;

    let max_rate = task.target_rps == 0;
    let interval_ns = pacer_interval_ns(task.target_rps).unwrap_or(0);
    let mut next_send_ns = task_start_ns;
    let barrier_epoch_ns = task_start_ns.saturating_sub(task.start_offset_ns);
    let mut generator = TaskGenerator::new(
        session_id.clone(),
        u64::from(task.task_id),
        task.profile,
        mix_from_task(&task),
        global_seed ^ u64::from(task.task_id),
    );
    let mut sent: u64 = 0;

    // One template per FrameKind, reused for the task's whole lifetime (P2').
    let mut template_cache = fix::TemplateCache::with_smp(
        writer.protocol(),
        &fix_version,
        &session_id,
        &target_host,
        u64::from(task.task_id),
        task.smp_id_count,
    );

    let mut frames: Vec<OrderFrame> = Vec::with_capacity(batch_max);
    let mut targets: Vec<u64> = Vec::with_capacity(batch_max);
    let mut scratch: Vec<u8> = Vec::with_capacity(batch_max * 256);

    loop {
        if should_stop_sending(&cancel, task_end_ns) {
            break;
        }

        // In-flight backpressure (same as the FIX path): if too many orders are awaiting acks
        // (contestant can't keep up, or telemetry is stalled), wait for pending to drain — via
        // acks or watchdog eviction — before issuing more, instead of growing memory unbounded.
        // Woken by `notify` instead of polling every 1ms.
        while pending.lock().expect("pending map poisoned").len() >= max_inflight {
            if should_stop_sending(&cancel, task_end_ns) {
                return Ok(sent);
            }
            let notified = notify.notified();
            tokio::pin!(notified);
            if pending.lock().expect("pending map poisoned").len() < max_inflight {
                break;
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = cancel.cancelled() => return Ok(sent),
            }
        }

        // Pace: park only when genuinely ahead of the next due order. Max-rate mode
        // (target_rps == 0) has no schedule, so never parks here.
        if !max_rate && next_send_ns > unix_nanos() {
            tokio::select! {
                _ = time::sleep_until(instant_from_unix_nanos(next_send_ns)) => {}
                _ = cancel.cancelled() => break,
            }
        }

        // Collect every order that is now due (up to batch_max). target_send_ts_ns is
        // captured here, per order, exactly as before batching was added. In max-rate
        // mode every order is "due" immediately.
        frames.clear();
        targets.clear();
        let now_ns = unix_nanos();
        while frames.len() < batch_max && (max_rate || next_send_ns <= now_ns) {
            let action = generator.next();
            let frame = render_frame(&mut template_cache, &action);
            targets.push(next_send_ns);
            frames.push(frame);
            if !max_rate {
                next_send_ns = next_send_ns.saturating_add(interval_ns);
            }
        }
        if frames.is_empty() {
            continue;
        }
        let count = frames.len();

        // Same provisional write-start stamp as the FIX loop: acks can race a
        // blocked batch write, and telemetry must never carry send_ts=0.
        let write_start_ns = unix_nanos();
        {
            let mut map = pending.lock().expect("pending map poisoned");
            for (frame, &target) in frames.iter().zip(targets.iter()) {
                map.insert(
                    frame.order_id.clone(),
                    PendingOrder {
                        order_id: frame.order_id.clone(),
                        orig_order_id: frame.orig_order_id.clone(),
                        target_send_ts_ns: target,
                        send_ts_ns: write_start_ns, // refined after the batch write returns
                        barrier_epoch_ns,
                        price: frame.price,
                        qty: frame.qty,
                        side: frame.side,
                        payload_type: frame.payload_type,
                        ord_type: frame.ord_type,
                        smp_id: frame.smp_id,
                    },
                );
            }
        }
        metrics::inflight_add(count);

        // Block on the write rather than imposing a per-write timeout (same as the FIX path).
        // When the contestant can't drain fast enough its TCP receive window fills and the write
        // stalls; TCP flow control then paces THIS task down to the contestant's real service
        // rate, so aggregate load plateaus at the sink's capacity instead of overshooting. A short
        // per-write timeout did the opposite: it forced the writer to exit, so under sustained
        // backpressure every loaded task exited at once and the offered load collapsed to zero
        // (the ramp-collapse symptom). Cancellation still tears the loop down promptly at session
        // end, and a genuinely dead peer surfaces as a write error below.
        let protocol_label = metrics::protocol_label(writer.protocol());
        // Same drain-deadline bound as the FIX loop: a never-draining peer must
        // not wedge the task past run end; the watchdog's last-tick pending
        // sweep accounts the abandoned batch as timed_out.
        let drain_end_ns = task_end_ns.saturating_add(RESPONSE_TIMEOUT_NS);
        let write_res = tokio::select! {
            res = writer.write_batch(&mut frames, &mut scratch) => res,
            _ = cancel.cancelled() => break,
            _ = time::sleep_until(instant_from_unix_nanos(drain_end_ns)) => {
                metrics::order_write_error(protocol_label);
                warn!(task_id = task.task_id, "write still blocked at drain deadline; abandoning stalled peer");
                break;
            }
        };
        match write_res {
            Ok(()) => {
                let send_ts_ns = unix_nanos();
                metrics::orders_sent_by(protocol_label, count);
                metrics::observe_write(
                    protocol_label,
                    send_ts_ns.saturating_sub(write_start_ns),
                    count,
                );
                {
                    let mut map = pending.lock().expect("pending map poisoned");
                    for frame in frames.iter() {
                        if let Some(p) = map.get_mut(&frame.order_id) {
                            p.send_ts_ns = send_ts_ns;
                            // Max-rate mode has no schedule: target == send time by
                            // definition, so slip downstream is exactly zero.
                            if max_rate {
                                p.target_send_ts_ns = send_ts_ns;
                            }
                        }
                    }
                }
                {
                    let deadline_ns = send_ts_ns.saturating_add(RESPONSE_TIMEOUT_NS);
                    let mut queue = expiry.lock().expect("expiry queue poisoned");
                    for frame in frames.iter() {
                        if let Some((seq, kind)) = fix::split_order_id(&frame.order_id) {
                            queue.push_back(ExpiryEntry {
                                deadline_ns,
                                seq,
                                kind,
                            });
                        }
                        // A non-splitting id (impossible for generator output) simply
                        // gets no queue entry; the watchdog's last-tick pending sweep
                        // still accounts for it.
                    }
                }
                if !max_rate {
                    for &target in targets.iter() {
                        metrics::observe_slip(protocol_label, send_ts_ns.saturating_sub(target));
                    }
                }
                sent += count as u64;
            }
            Err(err) => {
                let removed =
                    remove_batch_from_pending(&pending, frames.iter().map(|f| f.order_id.as_str()));
                metrics::inflight_sub(removed);
                metrics::order_write_error(protocol_label);
                warn!(
                    task_id = task.task_id,
                    error = %err, "REST/WS batch write failed; writer exiting"
                );
                return Ok(sent);
            }
        }
    }

    Ok(sent)
}

#[allow(clippy::too_many_arguments)]
/// emit_response performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn emit_response(
    telemetry: &TelemetrySink,
    pending: &PendingMap,
    notify: &Notify,
    session_id: &str,
    submission_id: &str,
    worker_id: &str,
    task_id: u32,
    clord_id: &str,
) {
    let pending_order = {
        let mut map = pending.lock().expect("pending map poisoned");
        map.remove(clord_id)
    };
    let Some(p) = pending_order else { return };
    metrics::inflight_sub(1);
    notify.notify_one();
    let recv_done_ts_ns = unix_nanos();
    telemetry
        .record(OrderSentEvent {
            session_id: session_id.to_string(),
            submission_id: submission_id.to_string(),
            worker_id: worker_id.to_string(),
            task_id,
            order_id: p.order_id,
            smp_id: p.smp_id,
            target_send_ts_ns: p.target_send_ts_ns,
            send_ts_ns: p.send_ts_ns,
            recv_done_ts_ns,
            timed_out: false,
            price: p.price,
            qty: p.qty,
            side: p.side,
            payload_type: p.payload_type,
            ord_type: p.ord_type,
            orig_order_id: p.orig_order_id,
            barrier_epoch_ns: p.barrier_epoch_ns,
        })
        .await;
}

/// Cheap `"cl_ord_id"` key scan (P4), avoiding `serde_json::from_slice` + a
/// `Resp` alloc per response on the hot read-loop path. Mirrors
/// `services/ebpf-latency/src/parse.rs::json_string`: find the key, the colon,
/// the opening quote, then the closing quote. ClOrdID is a bot-generated,
/// unpadded `sess_bot_seq_[OR]` token (see fix.rs) that never contains a
/// backslash or a literal `"` in the values bot-fleet itself produces — but a
/// contestant's response is untrusted input, so if a `\` shows up before the
/// closing quote this falls back to full `serde_json` parsing instead of
/// guessing at escape handling.
fn clordid_from_json(body: &[u8]) -> Option<String> {
    const KEY: &[u8] = b"\"cl_ord_id\"";
    let key_pos = find_subslice(body, KEY)?;
    let rest = &body[key_pos + KEY.len()..];
    let colon = rest.iter().position(|&b| b == b':')?;
    let after_colon = &rest[colon + 1..];
    let open = after_colon.iter().position(|&b| b == b'"')?;
    let value_start = &after_colon[open + 1..];
    let close = value_start.iter().position(|&b| b == b'"')?;
    let value = &value_start[..close];

    if value.contains(&b'\\') {
        return clordid_from_json_serde(body);
    }
    std::str::from_utf8(value).ok().map(str::to_string)
}

fn clordid_from_json_serde(body: &[u8]) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Resp {
        cl_ord_id: Option<String>,
    }
    serde_json::from_slice::<Resp>(body)
        .ok()
        .and_then(|r| r.cl_ord_id)
}

/// next_http_response performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn next_http_response(buf: &[u8]) -> Option<(Option<String>, usize)> {
    let hdr_end = find_subslice(buf, b"\r\n\r\n")?;
    let body_start = hdr_end + 4;
    let headers = &buf[..hdr_end];
    if header_is_chunked(headers) {
        let rel = find_subslice(&buf[body_start..], b"0\r\n\r\n")?;
        let total = body_start + rel + 5;
        let chunk_body = dechunk_first(&buf[body_start..total]);
        return Some((clordid_from_json(&chunk_body), total));
    }
    let content_len = header_content_length(headers).unwrap_or(0);
    let total = body_start + content_len;
    if buf.len() < total {
        return None;
    }
    Some((clordid_from_json(&buf[body_start..total]), total))
}

/// find_subslice performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

/// header_content_length performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn header_content_length(headers: &[u8]) -> Option<usize> {
    let lower: Vec<u8> = headers.iter().map(u8::to_ascii_lowercase).collect();
    let i = find_subslice(&lower, b"content-length:")?;
    let rest = &headers[i + b"content-length:".len()..];
    let end = rest.iter().position(|&b| b == b'\r').unwrap_or(rest.len());
    std::str::from_utf8(&rest[..end]).ok()?.trim().parse().ok()
}

/// header_is_chunked performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn header_is_chunked(headers: &[u8]) -> bool {
    let lower: Vec<u8> = headers.iter().map(u8::to_ascii_lowercase).collect();
    match find_subslice(&lower, b"transfer-encoding:") {
        Some(i) => {
            let rest = &lower[i + b"transfer-encoding:".len()..];
            let end = rest.iter().position(|&b| b == b'\r').unwrap_or(rest.len());
            find_subslice(&rest[..end], b"chunked").is_some()
        }
        None => false,
    }
}

/// dechunk_first performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn dechunk_first(body: &[u8]) -> Vec<u8> {
    let Some(crlf) = find_subslice(body, b"\r\n") else {
        return Vec::new();
    };
    let size = std::str::from_utf8(&body[..crlf])
        .ok()
        .and_then(|s| usize::from_str_radix(s.trim(), 16).ok())
        .unwrap_or(0);
    let start = crlf + 2;
    let end = (start + size).min(body.len());
    body[start..end].to_vec()
}

#[allow(clippy::too_many_arguments)]
/// rest_read_loop performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn rest_read_loop(
    mut read_half: ReadHalf<TcpStream>,
    task_id: u32,
    pending: PendingMap,
    notify: Arc<Notify>,
    telemetry: TelemetrySink,
    session_id: String,
    submission_id: String,
    worker_id: String,
    drain_end_ns: u64,
) -> Result<u64> {
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];

    loop {
        let now_ns = unix_nanos();
        if now_ns >= drain_end_ns {
            break;
        }
        let remaining = Duration::from_nanos(drain_end_ns - now_ns);
        let n = match time::timeout(remaining, read_half.read(&mut chunk)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
            Ok(Err(err)) => {
                warn!(task_id, error = %err, "REST read failed; reader exiting");
                break;
            }
            Err(_) => break,
        };
        buf.extend_from_slice(&chunk[..n]);

        while let Some((clord, consumed)) = next_http_response(&buf) {
            if let Some(clord_id) = clord {
                emit_response(
                    &telemetry,
                    &pending,
                    &notify,
                    &session_id,
                    &submission_id,
                    &worker_id,
                    task_id,
                    &clord_id,
                )
                .await;
            }
            buf.drain(..consumed);
        }

        if buf.len() > 1_048_576 {
            warn!(task_id, "REST read buffer overflow; resetting");
            buf.clear();
        }
    }

    Ok(0)
}

#[allow(clippy::too_many_arguments)]
/// ws_read_loop performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn ws_read_loop(
    mut stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    task_id: u32,
    pending: PendingMap,
    notify: Arc<Notify>,
    telemetry: TelemetrySink,
    session_id: String,
    submission_id: String,
    worker_id: String,
    drain_end_ns: u64,
) -> Result<u64> {
    loop {
        let now_ns = unix_nanos();
        if now_ns >= drain_end_ns {
            break;
        }
        let remaining = Duration::from_nanos(drain_end_ns - now_ns);
        let msg = match time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(err))) => {
                warn!(task_id, error = %err, "WS read failed; reader exiting");
                break;
            }
            Ok(None) => break,
            Err(_) => break,
        };
        let body: Vec<u8> = match msg {
            WsMessage::Text(t) => t.into_bytes(),
            WsMessage::Binary(b) => b,
            _ => continue,
        };
        if let Some(clord_id) = clordid_from_json(&body) {
            emit_response(
                &telemetry,
                &pending,
                &notify,
                &session_id,
                &submission_id,
                &worker_id,
                task_id,
                &clord_id,
            )
            .await;
        }
    }

    Ok(0)
}

/// order_shape performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub(crate) fn order_shape(profile: BotProfile, seq: u32, rng: &mut SmallRng) -> (u64, u64, Side) {
    match profile {
        BotProfile::Hft => {
            let side = if seq % 2 == 0 { Side::Sell } else { Side::Buy };
            // Price straddles the 10_000 mid (both sides draw from the same band),
            // so a buy can land above a resting sell — and a sell below a resting
            // bid — and they cross and trade. This churns the reference order book
            // (filled orders leave it), keeping the validator's book bounded, and
            // produces realistic fills instead of a permanently two-sided book that
            // never matches. Orders far from the mid still rest; near it they cross.
            let offset = rng.gen_range(0i64..25) - 12; // -12..=+12 around the mid
            let price = (10_000_i64 + offset) as u64;
            (price, rng.gen_range(10..50), side)
        }
        BotProfile::Retail => {
            let side = if rng.gen_bool(0.5) {
                Side::Buy
            } else {
                Side::Sell
            };
            let drift = rng.gen_range(0..50);
            let price = match side {
                Side::Buy => 10_000 - drift,
                Side::Sell => 10_000 + drift,
            };
            (price, rng.gen_range(1..10), side)
        }
        BotProfile::Institutional => {
            let side = if rng.gen_bool(0.5) {
                Side::Buy
            } else {
                Side::Sell
            };
            let drift = rng.gen_range(20..100);
            let price = match side {
                Side::Buy => 10_000 - drift,
                Side::Sell => 10_000 + drift,
            };
            (price, rng.gen_range(100..500), side)
        }
    }
}

/// instant_from_unix_nanos performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn instant_from_unix_nanos(target_ns: u64) -> Instant {
    let now_ns = unix_nanos();
    if target_ns <= now_ns {
        Instant::now()
    } else {
        Instant::now() + Duration::from_nanos(target_ns - now_ns)
    }
}

/// TargetClient enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
enum TargetClient {
    Fix(FixConnection),
    Rest(TcpStream),
    Ws(WebSocketStream<MaybeTlsStream<TcpStream>>),
}

impl TargetClient {
    /// connect performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn connect(spec: &WorkloadSpec, target: TargetSpec, addr: SocketAddr) -> Result<Self> {
        let timeout = Duration::from_millis(spec.connect_timeout_ms);
        match target.protocol {
            Protocol::Fix => {
                let mut stream = time::timeout(timeout, TcpStream::connect(addr))
                    .await
                    .context("timed out connecting FIX bot")?
                    .context("connect FIX bot")?;
                stream.set_nodelay(true).context("set TCP_NODELAY")?;
                stream
                    .write_all(&fix::logon_frame(&spec.fix_version, 1))
                    .await
                    .context("send FIX logon")?;
                let (read_half, write_half) = tokio::io::split(stream);
                Ok(Self::Fix(FixConnection {
                    read_half,
                    write_half,
                }))
            }
            Protocol::Rest => {
                let stream = time::timeout(timeout, TcpStream::connect(addr))
                    .await
                    .context("timed out connecting REST bot")?
                    .context("connect REST bot")?;
                stream.set_nodelay(true).context("set TCP_NODELAY")?;
                Ok(Self::Rest(stream))
            }
            Protocol::Ws => {
                let url = format!("ws://{}:{}/", spec.target_host, target.port);
                let (ws, _) = time::timeout(timeout, connect_async(url))
                    .await
                    .context("timed out connecting WS bot")?
                    .context("connect WS bot")?;
                if let tokio_tungstenite::MaybeTlsStream::Plain(ref tcp) = ws.get_ref() {
                    let _ = tcp.set_nodelay(true);
                }
                Ok(Self::Ws(ws))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn pacer_interval_ns_zero_target_is_max_rate_sentinel() {
        assert_eq!(pacer_interval_ns(0), None);
    }

    #[test]
    /// Distinct partitions run concurrently — the whole point of the change. Two
    /// concurrent sessions hold distinct partition leases, so their shards must be
    /// admitted together rather than serialized.
    fn in_flight_admits_distinct_partitions_concurrently() {
        let mut f = InFlight::new();
        assert!(f.try_admit(3, 4).is_ok());
        assert!(f.try_admit(15, 4).is_ok());
        assert_eq!(f.len(), 2);
    }

    #[test]
    /// A second spec on a partition that is already executing must be rejected, not
    /// queued alongside: per-partition offset commits are only correct while at most
    /// one workload per partition is in flight.
    fn in_flight_serializes_same_partition() {
        let mut f = InFlight::new();
        assert!(f.try_admit(7, 4).is_ok());
        assert_eq!(f.try_admit(7, 4), Err(Reject::PartitionBusy));
        f.finish(7);
        assert!(
            f.try_admit(7, 4).is_ok(),
            "freed partition is admissible again"
        );
    }

    #[test]
    /// At the cap, further specs are rejected as AtCapacity. The caller must leave
    /// them uncommitted so Kafka redelivers them — backpressure, never data loss.
    fn in_flight_enforces_global_cap() {
        let mut f = InFlight::new();
        assert!(f.try_admit(0, 2).is_ok());
        assert!(f.try_admit(1, 2).is_ok());
        assert_eq!(f.try_admit(2, 2), Err(Reject::AtCapacity));
        f.finish(0);
        assert!(f.try_admit(2, 2).is_ok(), "capacity freed by completion");
    }

    #[test]
    /// A busy partition reports PartitionBusy even when the worker is also at
    /// capacity: the conditions have different remedies, so the distinction has to
    /// survive for the log line to be useful.
    fn in_flight_reports_partition_busy_before_capacity() {
        let mut f = InFlight::new();
        assert!(f.try_admit(5, 1).is_ok());
        assert_eq!(f.try_admit(5, 1), Err(Reject::PartitionBusy));
    }

    #[test]
    /// A cap of 1 reproduces the old serialized behaviour exactly, which is the
    /// escape hatch if a deployment turns out to be memory-bound.
    fn in_flight_cap_of_one_is_fully_serial() {
        let mut f = InFlight::new();
        assert!(f.try_admit(1, 1).is_ok());
        assert_eq!(f.try_admit(2, 1), Err(Reject::AtCapacity));
        f.finish(1);
        assert!(f.try_admit(2, 1).is_ok());
    }

    #[test]
    /// finish() on a partition that holds nothing is a no-op, so a completion handler
    /// that runs twice cannot corrupt the set or free a slot it does not own.
    fn in_flight_finish_is_idempotent() {
        let mut f = InFlight::new();
        f.try_admit(9, 2).expect("admit");
        f.finish(9);
        f.finish(9);
        assert!(f.is_empty());
        assert!(f.try_admit(9, 2).is_ok());
    }

    #[test]
    /// MAX_CONCURRENT_WORKLOADS=0 would admit nothing and wedge the loop forever, so
    /// it is promoted to 1.
    fn max_concurrent_workloads_never_zero() {
        std::env::set_var("MAX_CONCURRENT_WORKLOADS", "0");
        assert_eq!(max_concurrent_workloads(), 1);
        std::env::set_var("MAX_CONCURRENT_WORKLOADS", "5");
        assert_eq!(max_concurrent_workloads(), 5);
        std::env::set_var("MAX_CONCURRENT_WORKLOADS", "not-a-number");
        assert_eq!(max_concurrent_workloads(), DEFAULT_MAX_CONCURRENT_WORKLOADS);
        std::env::remove_var("MAX_CONCURRENT_WORKLOADS");
        assert_eq!(max_concurrent_workloads(), DEFAULT_MAX_CONCURRENT_WORKLOADS);
    }

    #[test]
    fn pacer_interval_ns_computes_fixed_period() {
        assert_eq!(pacer_interval_ns(1), Some(1_000_000_000));
        assert_eq!(pacer_interval_ns(1000), Some(1_000_000));
        assert_eq!(pacer_interval_ns(10), Some(100_000_000));
    }

    #[test]
    /// http_response_framed_by_content_length_and_clordid_extracted performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn http_response_framed_by_content_length_and_clordid_extracted() {
        let body = br#"{"cl_ord_id":"sess_1_2_O","exec_type":"2","fill_qty":5,"fill_price":10000}"#;
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            std::str::from_utf8(body).unwrap()
        );
        let (clord, consumed) = next_http_response(resp.as_bytes()).expect("complete response");
        assert_eq!(clord.as_deref(), Some("sess_1_2_O"));
        assert_eq!(consumed, resp.len());
    }

    #[test]
    /// http_two_pipelined_responses_drain_in_order performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn http_two_pipelined_responses_drain_in_order() {
        let one = "HTTP/1.1 200 OK\r\nContent-Length: 22\r\n\r\n{\"cl_ord_id\":\"ord-A\"}\r\n";
        let two = "HTTP/1.1 200 OK\r\nContent-Length: 22\r\n\r\n{\"cl_ord_id\":\"ord-B\"}\r\n";
        let mut buf = format!("{one}{two}").into_bytes();
        let (a, n1) = next_http_response(&buf).expect("first");
        assert_eq!(a.as_deref(), Some("ord-A"));
        buf.drain(..n1);
        let (b, _) = next_http_response(&buf).expect("second");
        assert_eq!(b.as_deref(), Some("ord-B"));
    }

    #[test]
    /// http_incomplete_response_returns_none performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn http_incomplete_response_returns_none() {
        let partial = "HTTP/1.1 200 OK\r\nContent-Length: 50\r\n\r\n{\"cl_ord_id\":";
        assert!(next_http_response(partial.as_bytes()).is_none());
    }

    #[test]
    /// http_chunked_response_clordid_extracted performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn http_chunked_response_clordid_extracted() {
        let json = "{\"cl_ord_id\":\"ord-C\"}";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
            json.len(),
            json
        );
        let (clord, _) = next_http_response(resp.as_bytes()).expect("chunked complete");
        assert_eq!(clord.as_deref(), Some("ord-C"));
    }

    #[test]
    /// ws_json_payload_clordid_extracted performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn ws_json_payload_clordid_extracted() {
        let payload = br#"{"cl_ord_id":"ord-ws-1","exec_type":"0"}"#;
        assert_eq!(clordid_from_json(payload).as_deref(), Some("ord-ws-1"));
        assert_eq!(clordid_from_json(b"not json"), None);
        assert_eq!(clordid_from_json(b"{\"other\":1}"), None);
    }

    #[test]
    /// clordid_key_scan_matches_full_json_field_ordering_and_whitespace covers the
    /// P4 cheap-parse path across a few response shapes a contestant might send.
    fn clordid_key_scan_matches_full_json_field_ordering_and_whitespace() {
        assert_eq!(
            clordid_from_json(br#"{"exec_type":"0","cl_ord_id":"ord-2"}"#).as_deref(),
            Some("ord-2")
        );
        assert_eq!(
            clordid_from_json(br#"{ "cl_ord_id" : "ord-3" , "exec_type":"0" }"#).as_deref(),
            Some("ord-3")
        );
        assert_eq!(
            clordid_from_json(br#"{"cl_ord_id":""}"#).as_deref(),
            Some("")
        );
    }

    #[test]
    /// clordid_falls_back_to_serde_when_the_value_contains_a_backslash ensures the
    /// key-scan fast path never mis-parses an escaped value; it defers to the exact
    /// serde_json behavior in that case instead of guessing at unescaping.
    fn clordid_falls_back_to_serde_when_the_value_contains_a_backslash() {
        let body = br#"{"cl_ord_id":"ord\"weird","exec_type":"0"}"#;
        assert_eq!(clordid_from_json(body).as_deref(), Some("ord\"weird"));
    }

    #[test]
    /// pop_due_expirations_evicts_only_due_entries_in_fifo_order verifies the P3
    /// expiry queue: due entries pop from the front, later (not-yet-due) entries
    /// stay queued, and `last_tick` forces every remaining entry out regardless of
    /// deadline (matching the prior `HashMap::retain(... || last_tick)` semantics).
    fn pop_due_expirations_evicts_only_due_entries_in_fifo_order() {
        fn e(deadline_ns: u64, seq: u32) -> ExpiryEntry {
            ExpiryEntry {
                deadline_ns,
                seq,
                kind: b'O',
            }
        }
        let mut queue: VecDeque<ExpiryEntry> = VecDeque::new();
        queue.push_back(e(100, 1));
        queue.push_back(e(200, 2));
        queue.push_back(e(300, 3));

        let due = pop_due_expirations(&mut queue, 200, false);
        assert_eq!(due, vec![e(100, 1), e(200, 2)]);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.front().unwrap().seq, 3);

        let due = pop_due_expirations(&mut queue, 0, false);
        assert!(due.is_empty(), "nothing due yet before last_tick");
        assert_eq!(queue.len(), 1);
    }

    #[test]
    /// pop_due_expirations_last_tick_drains_everything covers the final-tick flush.
    fn pop_due_expirations_last_tick_drains_everything() {
        let a = ExpiryEntry {
            deadline_ns: u64::MAX,
            seq: 1,
            kind: b'O',
        };
        let b = ExpiryEntry {
            deadline_ns: u64::MAX,
            seq: 2,
            kind: b'C',
        };
        let mut queue: VecDeque<ExpiryEntry> = VecDeque::new();
        queue.push_back(a);
        queue.push_back(b);

        let due = pop_due_expirations(&mut queue, 0, true);
        assert_eq!(due, vec![a, b]);
        assert!(queue.is_empty());
    }

    #[test]
    /// pop_due_expirations_skips_nothing_and_is_empty_on_empty_queue is the trivial
    /// boundary case.
    fn pop_due_expirations_on_empty_queue_returns_empty() {
        let mut queue: VecDeque<ExpiryEntry> = VecDeque::new();
        assert!(pop_due_expirations(&mut queue, u64::MAX, true).is_empty());
    }

    #[test]
    /// concat_frames_batches_bytes_in_order verifies the REST batch-write helper
    /// (P3') concatenates frame payloads in order with no separators, matching
    /// HTTP/1.1 pipelining semantics (back-to-back requests on one write).
    fn concat_frames_batches_bytes_in_order() {
        fn frame(bytes: &[u8]) -> OrderFrame {
            OrderFrame {
                order_id: "id".into(),
                orig_order_id: String::new(),
                price: 0,
                qty: 0,
                side: Side::Buy,
                bytes: bytes.to_vec(),
                tag52_offset: None,
                payload_type: PayloadType::New,
                ord_type: OrdType::Limit,
                smp_id: iicpc_schemas_rust::SMP_ID_NONE,
            }
        }
        let frames = vec![frame(b"AAA"), frame(b"BB"), frame(b"C")];
        let mut out = Vec::new();
        concat_frames(&frames, &mut out);
        assert_eq!(out, b"AAABBC");

        // Reused buffer must not retain a previous batch's bytes.
        let frames2 = vec![frame(b"Z")];
        concat_frames(&frames2, &mut out);
        assert_eq!(out, b"Z");
    }

    #[test]
    /// resolve_task_target_picks_target_idx_entry verifies §7.3 Shape A: a task
    /// resolves its connection target by target_idx into the spec's targets
    /// table, independent of the legacy single protocol/target_port fields.
    fn resolve_task_target_picks_target_idx_entry() {
        let targets = vec![
            TargetSpec {
                protocol: Protocol::Fix,
                port: 9898,
            },
            TargetSpec {
                protocol: Protocol::Rest,
                port: 8080,
            },
            TargetSpec {
                protocol: Protocol::Ws,
                port: 8080,
            },
        ];
        let mut task = valid_spec().tasks.remove(0);

        task.target_idx = 0;
        assert_eq!(resolve_task_target(&targets, &task), targets[0]);
        task.target_idx = 2;
        assert_eq!(resolve_task_target(&targets, &task), targets[2]);
    }

    #[test]
    /// resolve_task_target_falls_back_out_of_range guards against a malformed
    /// or stale target_idx (e.g. a controller bug) crashing the worker.
    fn resolve_task_target_falls_back_out_of_range() {
        let targets = vec![TargetSpec {
            protocol: Protocol::Fix,
            port: 9898,
        }];
        let mut task = valid_spec().tasks.remove(0);
        task.target_idx = 5;
        assert_eq!(resolve_task_target(&targets, &task), targets[0]);
    }

    #[test]
    /// stale_spec_is_skipped verifies a workload spec published far enough in
    /// the past (beyond max_age_s) is flagged stale — the regression case for
    /// the lease-reuse race in runner.go, where a controller's next session
    /// re-leases the same partition before a worker consumed the previous
    /// session's spec.
    fn stale_spec_is_skipped() {
        let max_age_s = DEFAULT_WORKLOAD_SPEC_MAX_AGE_S;
        let now_ns = 1_000_000_000_000u64;
        let published_at_ns = now_ns - (max_age_s + 60) * 1_000_000_000;
        assert!(is_spec_stale(published_at_ns, now_ns, max_age_s));
    }

    #[test]
    /// fresh_spec_is_not_stale verifies a recently-published spec (well under
    /// max_age_s) is not skipped.
    fn fresh_spec_is_not_stale() {
        let max_age_s = DEFAULT_WORKLOAD_SPEC_MAX_AGE_S;
        let now_ns = 1_000_000_000_000u64;
        let published_at_ns = now_ns - 5 * 1_000_000_000;
        assert!(!is_spec_stale(published_at_ns, now_ns, max_age_s));
    }

    #[test]
    /// unset_published_at_is_not_stale verifies published_at_unix_ns == 0
    /// (older/other producers that don't stamp this field) is NOT treated as
    /// stale — backward-compat requirement from the design, since "unset"
    /// must not mean "infinitely old".
    fn unset_published_at_is_not_stale() {
        let max_age_s = DEFAULT_WORKLOAD_SPEC_MAX_AGE_S;
        let now_ns = 1_000_000_000_000u64;
        assert!(!is_spec_stale(0, now_ns, max_age_s));
    }

    #[test]
    /// legacy_spec_resolves_single_target_from_protocol_and_port verifies a
    /// spec with an empty targets table (pre-Shape-A message) still resolves
    /// a valid single target for connect_tasks/TargetClient::connect.
    fn legacy_spec_resolves_single_target_from_protocol_and_port() {
        let spec = valid_spec();
        assert!(spec.targets.is_empty());
        let resolved = spec.resolved_targets();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].protocol, spec.protocol);
        assert_eq!(resolved[0].port, spec.target_port);
        let task = &spec.tasks[0];
        assert_eq!(resolve_task_target(&resolved, task), resolved[0]);
    }

    #[tokio::test]
    /// resolves_hostname_target performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn resolves_hostname_target() {
        let addr = resolve_target("localhost", 9876)
            .await
            .expect("localhost must resolve");
        assert_eq!(addr.port(), 9876);
        assert!(addr.ip().is_loopback(), "expected loopback, got {addr}");
    }

    #[tokio::test]
    /// resolves_ip_literal_target performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn resolves_ip_literal_target() {
        let addr = resolve_target("127.0.0.1", 8080)
            .await
            .expect("ip literal must resolve");
        assert_eq!(addr, "127.0.0.1:8080".parse::<SocketAddr>().unwrap());
    }

    #[tokio::test]
    /// cluster_dns_name_reaches_resolver performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn cluster_dns_name_reaches_resolver() {
        let err = resolve_target("algo-sess-123.sandbox.svc.cluster.local", 8080)
            .await
            .expect_err("unresolvable off-cluster");
        assert!(
            err.to_string().contains("resolve target host"),
            "expected a resolver error, got: {err}"
        );
    }

    /// valid_spec performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn valid_spec() -> WorkloadSpec {
        WorkloadSpec {
            session_id: "sess-1".into(),
            submission_id: "sub-1".into(),
            contestant_id: "team-1".into(),
            target_host: "127.0.0.1".into(),
            target_port: 8080,
            protocol: Protocol::Fix,
            targets: Vec::new(),
            worker_index: 0,
            worker_count: 1,
            global_seed: 42,
            fix_version: "FIX.4.2".into(),
            connect_timeout_ms: 1500,
            write_timeout_ms: 250,
            barrier_epoch_ns: 0,
            published_at_unix_ns: 0,
            order_band: iicpc_schemas_rust::ORDER_BAND_UNSET,
            tasks: vec![TaskSpec {
                task_id: 1,
                profile: BotProfile::Hft,
                target_rps: 10,
                start_offset_ns: 0,
                duration_ns: 1_000_000_000,
                market_pct: 0,
                cancel_pct: 0,
                replace_pct: 0,
                target_idx: 0,
                smp_id_count: 0,
            }],
        }
    }

    #[test]
    /// validate_spec_rejects_submission_id_with_wire_unsafe_chars performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn validate_spec_rejects_submission_id_with_wire_unsafe_chars() {
        let mut spec = valid_spec();
        spec.submission_id = "sub/1".into();

        let err = validate_spec(&Config::default(), &spec).expect_err("spec should be rejected");
        assert!(err.to_string().contains("submission_id"));
    }

    #[test]
    /// cancel_replace_frames_carry_orig_order_id_new_orders_empty performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn cancel_replace_frames_carry_orig_order_id_new_orders_empty() {
        use content::Action;

        let mut cache = fix::TemplateCache::new(Protocol::Fix, "FIX.4.2", "sess1", "host", 7);

        let new = Action::NewLimit {
            seq: 1,
            price: 10_000,
            qty: 5,
            side: Side::Buy,
        };
        let frame = render_frame(&mut cache, &new);
        assert_eq!(frame.orig_order_id, "", "new limit must have empty orig");

        let market = Action::NewMarket {
            seq: 2,
            qty: 5,
            side: Side::Buy,
        };
        let frame = render_frame(&mut cache, &market);
        assert_eq!(frame.orig_order_id, "", "market must have empty orig");

        let cancel = Action::Cancel {
            seq: 3,
            orig_order_id: "sess1_7_1_O".into(),
            price: 10_000,
            qty: 5,
            side: Side::Buy,
        };
        let frame = render_frame(&mut cache, &cancel);
        assert_eq!(frame.orig_order_id, "sess1_7_1_O");

        let replace = Action::Replace {
            seq: 4,
            orig_order_id: "sess1_7_1_O".into(),
            price: 10_001,
            qty: 5,
            side: Side::Buy,
        };
        let frame = render_frame(&mut cache, &replace);
        assert_eq!(frame.orig_order_id, "sess1_7_1_O");

        let pending = PendingOrder {
            order_id: frame.order_id.clone(),
            orig_order_id: frame.orig_order_id.clone(),
            target_send_ts_ns: 0,
            send_ts_ns: 0,
            barrier_epoch_ns: 0,
            price: frame.price,
            qty: frame.qty,
            side: frame.side,
            payload_type: frame.payload_type,
            ord_type: frame.ord_type,
            smp_id: frame.smp_id,
        };
        assert_eq!(pending.orig_order_id, "sess1_7_1_O");
    }

    #[test]
    /// The barrier group must satisfy TWO constraints that pull in opposite directions.
    ///
    /// 1. Stable across sessions (no session_id): a per-session group is created fresh
    ///    every run and never deleted, leaking consumer groups on the broker. This is
    ///    why the group was made per-worker originally, and wait_for_barrier already
    ///    filters by session_id so per-session groups buy nothing.
    /// 2. Distinct per CONCURRENT SLOT: the barrier topic is broadcast, so every
    ///    concurrent workload needs its own group. Two workloads sharing one group
    ///    become competing members and only one is assigned the barrier partition —
    ///    the other never fires and its shard's load silently disappears.
    ///
    /// A bounded slot index satisfies both: no session_id, and at most
    /// MAX_CONCURRENT_WORKLOADS groups per pod, reused across runs.
    fn barrier_group_is_stable_across_sessions_but_distinct_per_slot() {
        let slot0 = barrier_group("bot-fleet", "worker-7", 0);
        let slot1 = barrier_group("bot-fleet", "worker-7", 1);

        assert!(
            !slot0.contains("sess"),
            "barrier group must not embed session_id (leaks broker groups), got {slot0}"
        );
        assert_ne!(
            slot0, slot1,
            "concurrent slots must not share a barrier group, or one workload never receives the barrier"
        );
        assert_eq!(slot0, "bot-fleet-barrier-worker-7-slot0");
        assert_eq!(
            slot0,
            barrier_group("bot-fleet", "worker-7", 0),
            "the same slot must reuse the same group across runs"
        );
    }

    #[test]
    /// Slots are allocated from [0, cap) and REUSED on completion, so the number of
    /// broker-side barrier groups a pod ever creates stays bounded.
    fn in_flight_allocates_and_reuses_bounded_slots() {
        let mut f = InFlight::new();
        let s0 = f.try_admit(10, 2).expect("first admit");
        let s1 = f.try_admit(20, 2).expect("second admit");
        assert_ne!(s0, s1, "concurrent workloads must get distinct slots");
        assert!(s0 < 2 && s1 < 2, "slots must stay within [0, cap)");

        f.finish(10);
        let s2 = f.try_admit(30, 2).expect("admit after completion");
        assert_eq!(
            s2, s0,
            "a freed slot must be reused, not incremented past cap"
        );
    }

    #[tokio::test]
    /// cancelled_token_stops_send_loop performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn cancelled_token_stops_send_loop() {
        let cancel = CancelToken::new();
        assert!(!cancel.is_cancelled(), "fresh token is not cancelled");

        cancel.cancel();
        assert!(cancel.is_cancelled(), "cancelled token reports cancelled");

        assert!(
            should_stop_sending(&cancel, u64::MAX),
            "cancelled token must stop the send loop before the deadline"
        );
        let live = CancelToken::new();
        assert!(!should_stop_sending(&live, u64::MAX));
        assert!(should_stop_sending(&live, 0));

        tokio::time::timeout(Duration::from_secs(1), cancel.cancelled())
            .await
            .expect("cancelled() must resolve promptly on a cancelled token");
    }

    #[test]
    /// validate_spec_rejects_duration_that_would_breach_poll_interval performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn validate_spec_rejects_duration_that_would_breach_poll_interval() {
        let config = Config::default();

        let mut ok = valid_spec();
        ok.tasks[0].duration_ns = 1_000_000_000;
        validate_spec(&config, &ok).expect("short workload must be accepted");

        let mut over = valid_spec();
        over.tasks[0].duration_ns = config.max_poll_interval.as_nanos() as u64;
        let err =
            validate_spec(&config, &over).expect_err("over-ceiling workload must be rejected");
        assert!(
            err.to_string().contains("max.poll.interval.ms")
                || err.to_string().contains("poll interval"),
            "error should reference the poll-interval ceiling, got: {err}"
        );

        assert!(worst_case_wall_time_ns(&over) > over.tasks[0].duration_ns);
    }

    #[test]
    /// ready_key_groups_by_session_and_worker performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn ready_key_groups_by_session_and_worker() {
        let signal = ReadySignal {
            session_id: "sess-1".into(),
            submission_id: "sub-1".into(),
            worker_id: "worker-1".into(),
            worker_index: 0,
            worker_count: 1,
            task_count: 1,
            connected_count: 1,
            ready_at_unix_nanos: 123,
        };

        assert_eq!(kafka::ready_key(&signal), "sess-1:worker-1");
    }

    fn mk_pending_order(id: &str) -> PendingOrder {
        PendingOrder {
            order_id: id.to_string(),
            orig_order_id: String::new(),
            target_send_ts_ns: 0,
            send_ts_ns: 0,
            barrier_epoch_ns: 0,
            price: 0,
            qty: 0,
            side: Side::Buy,
            payload_type: PayloadType::New,
            ord_type: OrdType::Limit,
            smp_id: iicpc_schemas_rust::SMP_ID_NONE,
        }
    }

    fn new_pending_map() -> PendingMap {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[test]
    /// remove_batch_from_pending_counts_only_actual_removals is the direct
    /// regression test for the FIX/REST/WS write-error double-subtraction bug
    /// (worker.rs write-error cleanup paths): when some ids in the batch have
    /// already been removed by a concurrent ack, the returned count must
    /// reflect only what THIS call actually removed, not the batch size.
    fn remove_batch_from_pending_counts_only_actual_removals() {
        let pending = new_pending_map();
        for id in ["a", "b", "c", "d"] {
            pending
                .lock()
                .unwrap()
                .insert(id.to_string(), mk_pending_order(id));
        }
        // Simulate the read/ack loop already having removed "b" and "d" before
        // the write-error cleanup path runs over the whole original batch.
        pending.lock().unwrap().remove("b");
        pending.lock().unwrap().remove("d");

        let removed = remove_batch_from_pending(&pending, ["a", "b", "c", "d"].into_iter());

        assert_eq!(removed, 2, "only a and c were still present to remove");
        assert!(pending.lock().unwrap().is_empty());
    }

    #[test]
    /// inflight_gauge_fix_error_path_no_double_subtract_when_partial_acks_race
    /// reproduces the exact failure scenario: a batch is inserted (inflight_add),
    /// some entries are acked concurrently (each doing its own inflight_sub(1),
    /// mirroring fix_read_loop), and then the write-error cleanup path fires
    /// over the ORIGINAL batch ids. Using the actual-removed count (the fix)
    /// must land the gauge back at exactly the pre-insert baseline; using the
    /// original batch size (the bug) would double-subtract the already-acked
    /// entries and drift the gauge negative.
    fn inflight_gauge_fix_error_path_no_double_subtract_when_partial_acks_race() {
        let _g = metrics::INFLIGHT_GAUGE_TEST_LOCK.lock().unwrap();
        let pending = new_pending_map();
        let batch = ["o1", "o2", "o3", "o4"];
        let start = metrics::inflight_value();

        // Writer inserts the whole batch and adds to the gauge (mirrors
        // fix_write_loop's insert + inflight_add(count)).
        {
            let mut map = pending.lock().unwrap();
            for id in batch {
                map.insert(id.to_string(), mk_pending_order(id));
            }
        }
        metrics::inflight_add(batch.len());
        assert_eq!(metrics::inflight_value(), start + batch.len() as i64);

        // Read loop acks o1 and o3 before the write error fires (mirrors
        // fix_read_loop: remove + inflight_sub(1) per ack).
        for id in ["o1", "o3"] {
            let acked = pending.lock().unwrap().remove(id).is_some();
            assert!(acked);
            metrics::inflight_sub(1);
        }
        assert_eq!(metrics::inflight_value(), start + 2);

        // Write error cleanup now runs over the ORIGINAL batch ids. Only o2
        // and o4 are still present; the fix subs by that actual count.
        let removed = remove_batch_from_pending(&pending, batch.into_iter());
        assert_eq!(removed, 2, "o1 and o3 were already gone");
        metrics::inflight_sub(removed);

        assert_eq!(
            metrics::inflight_value(),
            start,
            "gauge must return to baseline, not drift negative from double-subtracting o1/o3"
        );
        assert!(pending.lock().unwrap().is_empty());
    }

    #[test]
    /// inflight_gauge_rest_ws_insert_then_ack_nets_to_baseline covers the
    /// REST/WS send+ack flow: rw_write_loop's insert pairs with
    /// inflight_add(count), and emit_response's ack pairs with
    /// inflight_sub(1) per order. Before this fix, rw_write_loop never called
    /// inflight_add and emit_response never called inflight_sub, so the
    /// shared gauge only ever decreased for REST/WS traffic.
    fn inflight_gauge_rest_ws_insert_then_ack_nets_to_baseline() {
        let _g = metrics::INFLIGHT_GAUGE_TEST_LOCK.lock().unwrap();
        let pending = new_pending_map();
        let batch = ["r1", "r2", "r3"];
        let start = metrics::inflight_value();

        // Mirrors rw_write_loop: insert batch, then inflight_add(count).
        {
            let mut map = pending.lock().unwrap();
            for id in batch {
                map.insert(id.to_string(), mk_pending_order(id));
            }
        }
        metrics::inflight_add(batch.len());
        assert_eq!(metrics::inflight_value(), start + batch.len() as i64);

        // Mirrors emit_response: remove + inflight_sub(1) per ack.
        for id in batch {
            let acked = pending.lock().unwrap().remove(id).is_some();
            assert!(acked);
            metrics::inflight_sub(1);
        }

        assert_eq!(
            metrics::inflight_value(),
            start,
            "REST/WS send+ack must return the gauge to its pre-insert value"
        );
        assert!(pending.lock().unwrap().is_empty());
    }

    #[test]
    /// inflight_gauge_rest_ws_insert_then_timeout_eviction_nets_to_baseline
    /// covers the REST/WS send+timeout flow: rw_write_loop's insert pairs
    /// with inflight_add(count), and watchdog_loop's eviction (shared across
    /// all protocols) pairs with inflight_sub(evicted.len()). Confirms the
    /// gauge never goes negative for a REST/WS task whose orders time out
    /// instead of being acked.
    fn inflight_gauge_rest_ws_insert_then_timeout_eviction_nets_to_baseline() {
        let _g = metrics::INFLIGHT_GAUGE_TEST_LOCK.lock().unwrap();
        let pending = new_pending_map();
        let batch = ["w1", "w2"];
        let start = metrics::inflight_value();

        {
            let mut map = pending.lock().unwrap();
            for id in batch {
                map.insert(id.to_string(), mk_pending_order(id));
            }
        }
        metrics::inflight_add(batch.len());
        assert_eq!(metrics::inflight_value(), start + batch.len() as i64);

        // Mirrors watchdog_loop: due ids removed from pending, then
        // inflight_sub(to_evict.len()) once for the whole evicted set.
        let evicted: Vec<PendingOrder> = {
            let mut map = pending.lock().unwrap();
            batch.iter().filter_map(|id| map.remove(*id)).collect()
        };
        metrics::inflight_sub(evicted.len());

        assert_eq!(
            metrics::inflight_value(),
            start,
            "REST/WS send+timeout must return the gauge to its pre-insert value"
        );
        assert!(pending.lock().unwrap().is_empty());
    }
}
