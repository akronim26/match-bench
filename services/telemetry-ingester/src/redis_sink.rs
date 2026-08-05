//! This module implements redis sink behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use anyhow::{Context, Result};
use redis::aio::MultiplexedConnection;

use crate::aggregate::Snapshot;

const KEY_TTL_SECS: i64 = 3600;

/// RedisSink stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct RedisSink {
    conn: MultiplexedConnection,
}

impl RedisSink {
    /// connect performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub async fn connect(url: &str) -> Result<Self> {
        let client = redis::Client::open(url).context("open redis client")?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .context("connect redis")?;
        Ok(Self { conn })
    }

    /// write performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub async fn write(&self, snaps: &[Snapshot]) -> Result<()> {
        let mut conn = self.conn.clone();
        for s in snaps {
            if s.contestant_id.is_empty() {
                continue;
            }
            let key = redis_key(s);
            let fields: &[(&str, String)] = &[
                ("p50_ns", s.p50_ns.to_string()),
                ("p99_ns", s.p99_ns.to_string()),
                ("p999_ns", s.p999_ns.to_string()),
                ("tps_1s", s.tps_1s.to_string()),
                ("error_rate", s.error_rate.to_string()),
                ("wave_index", s.wave_index.to_string()),
                ("session_id", s.session_id.clone()),
                ("updated_at_ns", s.time_ns.to_string()),
            ];
            let live_key = live_key(s);
            let mut pipe = redis::pipe();
            pipe.hset_multiple(&key, fields)
                .ignore()
                .expire(&key, KEY_TTL_SECS)
                .ignore();
            pipe.query_async::<()>(&mut conn)
                .await
                .context("redis pipeline HSET/EXPIRE")?;

            // CAS the live pointer on wave_index so out-of-order flushes (HashMap
            // iteration order, replay after restart/rebalance, or multiple ingester
            // replicas sharing a session's partition band) cannot regress it to an
            // older wave. This GET-then-SET is not linearizable across concurrent
            // writers; it relies on the assumption (per session-band partitioning)
            // that there is at most one writer for a given session shard, which
            // makes the race window between GET and SET benign in practice.
            let current: Option<String> = redis::cmd("GET")
                .arg(&live_key)
                .query_async(&mut conn)
                .await
                .context("redis GET live pointer")?;
            let current_wave: i64 = current.and_then(|v| v.parse::<i64>().ok()).unwrap_or(-1);
            if (s.wave_index as i64) >= current_wave {
                let mut set_pipe = redis::pipe();
                set_pipe
                    .set(&live_key, s.wave_index.to_string())
                    .ignore()
                    .expire(&live_key, KEY_TTL_SECS)
                    .ignore();
                set_pipe
                    .query_async::<()>(&mut conn)
                    .await
                    .context("redis pipeline SET/EXPIRE live pointer")?;
            }
        }
        Ok(())
    }
}

/// redis_key performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn redis_key(s: &Snapshot) -> String {
    format!(
        "contestant:{}:{}:{}",
        s.contestant_id, s.session_id, s.wave_index
    )
}

fn live_key(s: &Snapshot) -> String {
    format!("live:{}:latest", s.session_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// snap performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn snap(contestant: &str, session: &str, wave: u32) -> Snapshot {
        Snapshot {
            time_ns: 1,
            session_id: session.into(),
            contestant_id: contestant.into(),
            wave_index: wave,
            p50_ns: 0,
            p90_ns: 0,
            p99_ns: 0,
            p999_ns: 0,
            rt_p50_ns: 0,
            rt_p90_ns: 0,
            rt_p99_ns: 0,
            tps_1s: 0.0,
            error_rate: 0.0,
            offered: 0,
            errors: 0,
            hdr_encoded: Vec::new(),
            rt_hdr_encoded: Vec::new(),
            slip_hdr_encoded: Vec::new(),
            match_hdr_encoded: Vec::new(),
        }
    }

    #[test]
    /// redis_key_disambiguates_session_and_wave performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn redis_key_disambiguates_session_and_wave() {
        let a = redis_key(&snap("c1", "S", 0));
        let b = redis_key(&snap("c1", "S", 1));
        assert_ne!(a, b, "different waves must not share a key");
        assert_eq!(
            redis_key(&snap("c1", "S", 0)),
            a,
            "stable for the same window"
        );
    }

    #[tokio::test]
    async fn write_sets_ttl_and_live_pointer() {
        let Ok(url) = std::env::var("REDIS_URL") else {
            eprintln!("skip write_sets_ttl_and_live_pointer: REDIS_URL unset");
            return;
        };
        let session = format!("utest-redis-{}", std::process::id());
        let s = snap("c1", &session, 3);
        let sink = RedisSink::connect(&url).await.expect("connect redis");
        sink.write(&[s.clone()]).await.expect("redis write");

        let client = redis::Client::open(url).expect("redis client");
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("redis conn");

        let hash_key = redis_key(&s);
        let hash_ttl: i64 = redis::cmd("TTL")
            .arg(&hash_key)
            .query_async(&mut conn)
            .await
            .expect("ttl contestant hash");
        assert!(hash_ttl > 0, "contestant hash key must have a TTL set");

        let live_key = live_key(&s);
        let live_value: String = redis::cmd("GET")
            .arg(&live_key)
            .query_async(&mut conn)
            .await
            .expect("get live pointer");
        assert_eq!(live_value, "3");

        let live_ttl: i64 = redis::cmd("TTL")
            .arg(&live_key)
            .query_async(&mut conn)
            .await
            .expect("ttl live pointer");
        assert!(live_ttl > 0, "live pointer key must have a TTL set");
    }

    #[tokio::test]
    async fn live_pointer_does_not_regress_on_out_of_order_flush() {
        let Ok(url) = std::env::var("REDIS_URL") else {
            eprintln!("skip live_pointer_does_not_regress_on_out_of_order_flush: REDIS_URL unset");
            return;
        };
        let session = format!("utest-redis-cas-{}", std::process::id());
        let newer = snap("c1", &session, 5);
        let older = snap("c1", &session, 2);
        let sink = RedisSink::connect(&url).await.expect("connect redis");

        // Write the newer wave first, then an older wave arrives late (e.g. replay
        // after restart, or reordering across a rebalance). The live pointer must
        // stay at the newer wave, not regress.
        sink.write(&[newer.clone()])
            .await
            .expect("redis write newer");
        sink.write(&[older.clone()])
            .await
            .expect("redis write older");

        let client = redis::Client::open(url).expect("redis client");
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("redis conn");

        let live_key = live_key(&newer);
        let live_value: String = redis::cmd("GET")
            .arg(&live_key)
            .query_async(&mut conn)
            .await
            .expect("get live pointer");
        assert_eq!(
            live_value, "5",
            "live pointer must remain at the newer wave, not regress to an older one"
        );
    }
}
