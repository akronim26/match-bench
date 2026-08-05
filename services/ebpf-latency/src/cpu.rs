//! Per-thread CPU sampling for the capture.
//!
//! Why this exists: the capture drops ring-buffer records under load
//! (`ringbuf_dropped` = 23,436 on one 2.06M-order run), and the obvious question — is it
//! CPU-bound or blocked on Kafka? — could not be answered from its own telemetry. Kafka was
//! ruled out by `producer_inflight` (24 while dropping, versus 20 on a clean run: a blocked
//! producer would show thousands). CPU could only be guessed at, because nothing exported
//! it: the process publishes no CPU metric and cAdvisor is not scraped here.
//!
//! Guessing is what this codebase has already paid for repeatedly, so this measures instead.
//! Per-THREAD rather than per-process is the whole point: everything after the ring buffer —
//! decode, reassembly, framing, matching — runs in a single tokio task, so "one thread pinned
//! at 100% while the process shows 1.4 cores" and "work spread evenly across threads" are
//! completely different problems with completely different fixes, and the process total
//! cannot tell them apart.

use std::{
    collections::HashMap,
    fs,
    time::{Duration, Instant},
};

/// One thread's CPU utilisation over the last sampling interval.
pub struct ThreadCpu {
    /// The kernel's comm for the thread (tokio names its workers, so these are readable).
    pub name: String,
    /// Percent of ONE core. 100.0 means this thread was runnable and scheduled the entire
    /// interval — i.e. it is saturated and is the bottleneck.
    pub percent: f64,
}

/// Samples /proc/self/task/<tid>/stat and reports per-thread CPU between calls.
pub struct CpuSampler {
    last_ticks: HashMap<u32, u64>,
    last_at: Option<Instant>,
    ticks_per_sec: f64,
}

impl CpuSampler {
    pub fn new() -> Self {
        // USER_HZ is 100 on every Linux this runs on, but read it rather than assume: a
        // wrong divisor here would silently scale every number in the investigation this
        // exists to serve.
        let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        Self {
            last_ticks: HashMap::new(),
            last_at: None,
            ticks_per_sec: if hz > 0 { hz as f64 } else { 100.0 },
        }
    }

    /// Returns per-thread utilisation since the previous call, plus the process total.
    /// The first call returns nothing: a delta needs two samples, and reporting cumulative
    /// CPU as if it were a rate would be exactly the kind of misnamed metric that has
    /// already cost this project days.
    pub fn sample(&mut self) -> (Vec<ThreadCpu>, f64) {
        let now = Instant::now();
        let mut current: HashMap<u32, (String, u64)> = HashMap::new();

        let Ok(entries) = fs::read_dir("/proc/self/task") else {
            return (Vec::new(), 0.0);
        };
        for entry in entries.flatten() {
            let Ok(tid) = entry.file_name().to_string_lossy().parse::<u32>() else {
                continue;
            };
            let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            let Some((name, ticks)) = parse_stat(&stat) else {
                continue;
            };
            current.insert(tid, (name, ticks));
        }

        let elapsed = match self.last_at {
            Some(prev) => now.duration_since(prev),
            None => Duration::ZERO,
        };

        // Refuse to divide by a too-short interval, and do NOT consume the baseline when
        // refusing — the next call then measures across the full gap instead.
        //
        // This is not defensive padding; without it the metric is pure noise. Sampling is
        // driven by a tokio interval whose default MissedTickBehavior is Burst, so under the
        // very load worth measuring the loop falls behind and then fires ticks back to back.
        // With /proc accounting quantised to 10ms (USER_HZ 100), an elapsed of ~1ms yields
        // either delta=0 -> "0%" or delta=1 tick -> "~1000%". Both were observed on real
        // runs: a max-rate session reported 0% in most samples and 970% in one, and the 970%
        // was very nearly taken at face value as "the capture needs ten cores".
        const MIN_INTERVAL: Duration = Duration::from_millis(250);
        if elapsed > Duration::ZERO && elapsed < MIN_INTERVAL {
            return (Vec::new(), 0.0);
        }
        let mut out = Vec::with_capacity(current.len());
        let mut total = 0.0;

        if elapsed > Duration::ZERO {
            let secs = elapsed.as_secs_f64();
            for (tid, (name, ticks)) in &current {
                let prev = self.last_ticks.get(tid).copied().unwrap_or(*ticks);
                let delta = ticks.saturating_sub(prev) as f64;
                let percent = (delta / self.ticks_per_sec) / secs * 100.0;
                total += percent;
                out.push(ThreadCpu {
                    name: name.clone(),
                    percent,
                });
            }
        }

        self.last_ticks = current.into_iter().map(|(k, (_, t))| (k, t)).collect();
        self.last_at = Some(now);
        out.sort_by(|a, b| b.percent.total_cmp(&a.percent));
        (out, total)
    }
}

/// Cumulative CFS throttling for this container, read from the cgroup.
pub struct Throttling {
    /// Number of periods in which the cgroup was throttled.
    pub nr_throttled: u64,
    /// Total time spent throttled, in microseconds.
    pub throttled_usec: u64,
}

/// Reads cgroup v2 `cpu.stat`, then falls back to v1.
///
/// This distinguishes the two things a high CPU reading cannot: "the capture needs more CPU
/// than exists" versus "the capture was held below its limit by CFS while CPU was
/// available". They have completely different fixes — shard the pipeline, versus raise a
/// limit or a request — and the capture container is deliberately Burstable
/// (requests 200m, limits 4), so under node pressure it can be squeezed toward its request.
///
/// The container has hit this before: the CPU limit was raised from 2 to 4 cores precisely
/// because "a 2-core cap CFS-throttled it -> ringbuf drops"
/// (`sandbox-orchestrator/internal/k8s/slot.go`). Without this counter that diagnosis has to
/// be rediscovered by hand every time.
pub fn read_throttling() -> Option<Throttling> {
    let text = fs::read_to_string(cpu_stat_path()?).ok()?;
    let mut nr_throttled = None;
    let mut throttled_usec = None;
    for line in text.lines() {
        let mut it = line.split_whitespace();
        let (Some(key), Some(val)) = (it.next(), it.next()) else {
            continue;
        };
        match key {
            "nr_throttled" => nr_throttled = val.parse().ok(),
            // v2 reports microseconds; v1 reports nanoseconds under a different key.
            "throttled_usec" => throttled_usec = val.parse().ok(),
            "throttled_time" => throttled_usec = val.parse::<u64>().ok().map(|ns| ns / 1_000),
            _ => {}
        }
    }
    Some(Throttling {
        nr_throttled: nr_throttled?,
        throttled_usec: throttled_usec.unwrap_or(0),
    })
}

/// Resolves THIS container's `cpu.stat`, not the host's.
///
/// `/sys/fs/cgroup/cpu.stat` is the wrong file here. The capture Job runs with
/// `HostPID: true` and without a private cgroup mount, so that path resolves to the host
/// ROOT cgroup: it was observed reporting `usage_usec` of 119,500 seconds (~33 hours of CPU
/// on a node up 34 hours) for a Job that had been alive for seconds, and `nr_throttled` of 0
/// — which the root cgroup always reports, because the root is never throttled. A throttling
/// metric wired to that file can only ever say "not throttled", which is worse than having
/// no metric at all.
///
/// `/proc/self/cgroup` gives the real path, e.g.
/// `0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod<uid>.slice/cri-containerd-<id>.scope`.
fn cpu_stat_path() -> Option<std::path::PathBuf> {
    if let Ok(text) = fs::read_to_string("/proc/self/cgroup") {
        for line in text.lines() {
            // cgroup v2 lines are "0::<path>"; v1 lines carry controller names instead.
            let rel = line
                .strip_prefix("0::")
                .or_else(|| line.split(':').nth(2).filter(|_| line.contains(":cpu,")))?;
            let candidate = std::path::Path::new("/sys/fs/cgroup")
                .join(rel.trim_start_matches('/'))
                .join("cpu.stat");
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }
    // Fall back only if the specific path could not be resolved, and accept that the value
    // may then be the host's.
    let root = std::path::PathBuf::from("/sys/fs/cgroup/cpu.stat");
    root.exists().then_some(root)
}

/// Extracts (comm, utime+stime) from a /proc/.../stat line.
///
/// Splitting on whitespace naively is wrong: field 2 is the comm in parentheses and may
/// itself contain spaces (and parentheses). Everything after the LAST ')' is positional,
/// which is the documented way to parse this file.
fn parse_stat(stat: &str) -> Option<(String, u64)> {
    let open = stat.find('(')?;
    let close = stat.rfind(')')?;
    let name = stat.get(open + 1..close)?.to_string();
    let rest: Vec<&str> = stat.get(close + 2..)?.split_whitespace().collect();
    // rest[0] is field 3 (state), so utime (field 14) is rest[11] and stime (15) is rest[12].
    let utime: u64 = rest.get(11)?.parse().ok()?;
    let stime: u64 = rest.get(12)?.parse().ok()?;
    Some((name, utime + stime))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A comm containing spaces and parentheses is legal and would break naive field
    /// splitting — which is how a CPU metric ends up attributing time to the wrong thread.
    #[test]
    fn parse_stat_handles_comm_with_spaces_and_parens() {
        let line = "42 (weird (name) here) S 1 1 1 0 -1 0 0 0 0 0 111 222 0 0 20 0 8 0 0";
        let (name, ticks) = parse_stat(line).expect("parses");
        assert_eq!(name, "weird (name) here");
        assert_eq!(ticks, 333, "utime + stime");
    }

    #[test]
    fn parse_stat_rejects_truncated_lines() {
        assert!(parse_stat("42 (short) S 1 2 3").is_none());
    }

    /// The first sample has no baseline to subtract, so it must report nothing rather than
    /// present cumulative CPU as an interval rate.
    #[test]
    fn first_sample_reports_no_rates() {
        let mut s = CpuSampler::new();
        let (threads, total) = s.sample();
        assert!(threads.is_empty(), "first sample must not report rates");
        assert_eq!(total, 0.0);
    }

    /// A second sample taken after a long enough gap must produce readable per-thread
    /// entries.
    #[test]
    fn second_sample_reports_threads() {
        let mut s = CpuSampler::new();
        let _ = s.sample();
        std::thread::sleep(Duration::from_millis(300));
        let (threads, _) = s.sample();
        assert!(!threads.is_empty(), "expected at least this thread");
        assert!(threads.iter().all(|t| t.percent >= 0.0));
    }

    /// THE regression test. Sampling is driven by a tokio interval with Burst missed-tick
    /// behaviour, so under load it fires ticks back to back and the gap between samples
    /// collapses to ~1ms. Against 10ms /proc accounting that produced either 0% or ~1000%
    /// from the same healthy process — and the 970% reading was nearly acted on as "the
    /// capture needs ten cores".
    ///
    /// A sub-interval sample must report NOTHING and must not consume the baseline, so the
    /// following sample still measures across the full elapsed gap.
    #[test]
    fn sub_interval_samples_are_suppressed_and_keep_their_baseline() {
        let mut s = CpuSampler::new();
        let _ = s.sample();

        std::thread::sleep(Duration::from_millis(20));
        let (threads, total) = s.sample();
        assert!(
            threads.is_empty() && total == 0.0,
            "a 20ms gap is below the floor and must report nothing, got {} thread(s)",
            threads.len()
        );

        // Baseline preserved: this sample spans the whole ~320ms, not just the last 300ms.
        std::thread::sleep(Duration::from_millis(300));
        let (threads, _) = s.sample();
        assert!(
            !threads.is_empty(),
            "after the floor is cleared the sampler must report again"
        );
        assert!(
            threads.iter().all(|t| t.percent <= 100.0 * 64.0),
            "a suppressed-then-resumed sample must not produce absurd percentages"
        );
    }
}
