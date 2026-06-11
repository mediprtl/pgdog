use std::{
    ops::{Deref, DerefMut},
    time::{Duration, SystemTime},
};

use tokio::{
    select, spawn,
    time::{interval, sleep, timeout},
};
use tracing::{debug, error, trace};

use crate::net::DataRow;

use super::*;
use pgdog_postgres_types::Format;

use pgdog_stats::LsnStats as StatsLsnStats;
pub use pgdog_stats::replication::ReplicaLag;

static AURORA_DETECTION_QUERY: &str = "SELECT aurora_version()";

static LSN_QUERY: &str = "
SELECT
    pg_is_in_recovery() AS replica,
    CASE
        WHEN pg_is_in_recovery() THEN
            COALESCE(
                pg_last_wal_replay_lsn(),
                pg_last_wal_receive_lsn()
            )
        ELSE
            pg_current_wal_lsn()
    END AS lsn,
    CASE
        WHEN pg_is_in_recovery() THEN
            COALESCE(
                pg_last_wal_replay_lsn(),
                pg_last_wal_receive_lsn()
            ) - '0/0'::pg_lsn
        ELSE
            pg_current_wal_lsn() - '0/0'::pg_lsn
    END AS offset_bytes,
    CASE
        WHEN pg_is_in_recovery() THEN
            COALESCE(pg_last_xact_replay_timestamp(), now())
        ELSE
            now()
    END AS timestamp
";

static AURORA_LSN_QUERY: &str = "
SELECT
    pg_is_in_recovery() AS replica,
    '0/0'::pg_lsn AS lsn,
    0::bigint AS offset_bytes,
    now() AS timestamp
";

/// LSN information.
#[derive(Debug, Clone, Copy, Default)]
pub struct LsnStats {
    inner: StatsLsnStats,
    /// Replica WAL apply rate in bytes/sec, measured by the monitor from
    /// successive LSN samples. `None` until two samples exist, or when the
    /// replica isn't advancing (stalled), in which case no ETA can be derived.
    apply_rate: Option<f64>,
}

impl Deref for LsnStats {
    type Target = StatsLsnStats;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for LsnStats {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl From<StatsLsnStats> for LsnStats {
    fn from(value: StatsLsnStats) -> Self {
        Self {
            inner: value,
            apply_rate: None,
        }
    }
}

impl LsnStats {
    /// How old the stats are.
    pub fn lsn_age(&self, now: SystemTime) -> Duration {
        now.duration_since(self.fetched).unwrap_or_default()
    }

    /// Stats contain real data.
    pub fn valid(&self) -> bool {
        self.inner.valid()
    }

    /// Calculate replica lag.
    pub fn replica_lag(&self, primary: &LsnStats) -> ReplicaLag {
        let bytes = primary.lsn.lsn - self.lsn.lsn;
        let lag_ms = (primary.timestamp.to_naive_datetime() - self.timestamp.to_naive_datetime())
            .num_milliseconds()
            .clamp(0, i64::MAX);
        let lag = Duration::from_millis(lag_ms as u64);

        ReplicaLag {
            bytes,
            duration: lag,
        }
    }

    /// Estimated time for this replica to replay up to `min_lsn`, derived from
    /// the apply rate the monitor measured (gap / rate). Used to size a client's
    /// read-your-writes defer to the real deficit. Returns `None` when it can't
    /// be estimated: no rate sample yet, or the replica isn't advancing.
    pub fn eta_to(&self, min_lsn: i64) -> Option<Duration> {
        if !self.valid() {
            return None;
        }
        let gap = min_lsn - self.lsn.lsn;
        if gap <= 0 {
            return Some(Duration::ZERO);
        }
        let rate = self.apply_rate?;
        if rate <= 0.0 {
            return None;
        }
        // Clamp to a day so a pathologically distant floor (e.g. a synthetic
        // max-LSN) can't overflow Duration::from_secs_f64; clients clamp further.
        let secs = (gap as f64 / rate).min(86_400.0);
        Some(Duration::from_secs_f64(secs))
    }
}

impl LsnStats {
    fn from_row(value: DataRow, aurora: bool) -> Self {
        StatsLsnStats {
            replica: value.get(0, Format::Text).unwrap_or_default(),
            lsn: value.get(1, Format::Text).unwrap_or_default(),
            offset_bytes: value.get(2, Format::Text).unwrap_or_default(),
            timestamp: value.get(3, Format::Text).unwrap_or_default(),
            fetched: SystemTime::now(),
            aurora,
        }
        .into()
    }
}

/// LSN monitor loop.
pub(super) struct LsnMonitor {
    pool: Pool,
}

impl LsnMonitor {
    pub(super) fn run(pool: &Pool) {
        let monitor = Self { pool: pool.clone() };

        spawn(async move {
            monitor.spawn().await;
        });
    }

    async fn run_query(&self, conn: &mut Guard, query: &str) -> Option<DataRow> {
        match timeout(self.pool.config().lsn_check_timeout, conn.fetch_all(query)).await {
            Ok(Ok(rows)) => rows.into_iter().next(),
            Ok(Err(err)) => {
                error!("lsn monitor query error: {} [{}]", err, self.pool.addr());
                None
            }
            Err(_) => {
                error!("lsn monitor query timeout [{}]", self.pool.addr());
                None
            }
        }
    }

    async fn detect_aurora(&self, conn: &mut Guard) -> Option<bool> {
        match timeout(
            self.pool.config().lsn_check_timeout,
            conn.fetch_all::<DataRow>(AURORA_DETECTION_QUERY),
        )
        .await
        {
            Ok(Ok(_)) => {
                debug!("aurora detected [{}]", self.pool.addr());
                Some(true)
            }
            Ok(Err(crate::backend::Error::ExecutionError(_))) => Some(false),
            Ok(Err(err)) => {
                error!(
                    "lsn monitor aurora detection error: {} [{}]",
                    err,
                    self.pool.addr()
                );
                None
            }
            Err(_) => {
                error!(
                    "lsn monitor aurora detection timeout [{}]",
                    self.pool.addr()
                );
                None
            }
        }
    }

    async fn spawn(&self) {
        select! {
            _ = sleep(self.pool.config().lsn_check_delay) => {},
            _ = self.pool.comms().shutdown.notified() => { return; }
        }

        debug!("lsn monitor loop is running [{}]", self.pool.addr());

        let mut aurora_detected: Option<bool> = None;
        let mut interval = interval(self.pool.config().lsn_check_interval);
        // Previous sample, to measure the replica's WAL apply rate (bytes/sec)
        // across poll intervals so we can estimate catch-up ETAs.
        let mut prev: Option<LsnStats> = None;

        loop {
            select! {
                _ = interval.tick() => {},
                _ = self.pool.comms().shutdown.notified() => { break; }
            }

            let mut conn = match self.pool.get(&Request::default()).await {
                Ok(conn) => conn,
                Err(Error::Offline) => break,
                Err(err) => {
                    error!("lsn monitor checkout error: {} [{}]", err, self.pool.addr());
                    continue;
                }
            };

            if aurora_detected.is_none() {
                aurora_detected = self.detect_aurora(&mut conn).await;
            }

            let Some(aurora) = aurora_detected else {
                continue;
            };

            let query = if aurora { AURORA_LSN_QUERY } else { LSN_QUERY };

            if let Some(row) = self.run_query(&mut conn, query).await {
                drop(conn);
                let mut stats = LsnStats::from_row(row, aurora);
                // Apply rate = how many WAL bytes the replica replayed since the
                // last sample, per second. Only when both samples are real and
                // the replica advanced (a flat/declining sample leaves it None).
                if let Some(prev) = prev {
                    if prev.valid() && stats.valid() {
                        let dt = stats
                            .fetched
                            .duration_since(prev.fetched)
                            .unwrap_or_default()
                            .as_secs_f64();
                        let dlsn = (stats.lsn.lsn - prev.lsn.lsn) as f64;
                        if dt > 0.0 && dlsn > 0.0 {
                            stats.apply_rate = Some(dlsn / dt);
                        }
                    }
                }
                prev = Some(stats);
                *self.pool.inner().lsn_stats.write() = stats;
                trace!("lsn monitor stats updated [{}]", self.pool.addr());
            }
        }

        debug!("lsn monitor shutdown [{}]", self.pool.addr());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use pgdog_postgres_types::TimestampTz;
    use pgdog_stats::Lsn;

    #[test]
    fn test_aurora_stats_valid_with_zero_lsn() {
        let stats: LsnStats = StatsLsnStats {
            replica: true,
            lsn: Lsn::default(),
            offset_bytes: 0,
            timestamp: TimestampTz::default(),
            fetched: SystemTime::now(),
            aurora: true,
        }
        .into();

        assert!(
            stats.valid(),
            "Aurora stats should be valid even with zero LSN"
        );
    }

    #[test]
    fn test_non_aurora_stats_invalid_with_zero_lsn() {
        let stats: LsnStats = StatsLsnStats {
            replica: true,
            lsn: Lsn::default(),
            offset_bytes: 0,
            timestamp: TimestampTz::default(),
            fetched: SystemTime::now(),
            aurora: false,
        }
        .into();

        assert!(
            !stats.valid(),
            "Non-Aurora stats should be invalid with zero LSN"
        );
    }

    #[test]
    fn test_eta_to_uses_apply_rate() {
        let mut lsn = Lsn::default();
        lsn.lsn = 1000;
        let mut stats: LsnStats = StatsLsnStats {
            replica: true,
            lsn,
            offset_bytes: 1000,
            timestamp: TimestampTz::default(),
            fetched: SystemTime::now(),
            aurora: false,
        }
        .into();

        // No rate sample yet -> not estimable.
        assert!(stats.eta_to(1500).is_none());

        // 500 bytes still to replay at 100 bytes/sec -> ~5s.
        stats.apply_rate = Some(100.0);
        assert_eq!(stats.eta_to(1500).map(|d| d.as_secs()), Some(5));

        // Already at/past the floor -> zero.
        assert_eq!(stats.eta_to(1000), Some(Duration::ZERO));
        assert_eq!(stats.eta_to(500), Some(Duration::ZERO));

        // Stalled (rate 0) -> not estimable.
        stats.apply_rate = Some(0.0);
        assert!(stats.eta_to(1500).is_none());

        // Pathologically distant floor is clamped, not overflowed.
        stats.apply_rate = Some(1.0);
        assert!(stats.eta_to(i64::MAX).unwrap().as_secs() <= 86_400);
    }
}
