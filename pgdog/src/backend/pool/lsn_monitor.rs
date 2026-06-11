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
    END AS timestamp,
    CASE
        WHEN pg_is_in_recovery() THEN
            COALESCE(
                EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::bigint,
                0
            )
        ELSE
            0
    END AS replay_lag_seconds
";

static AURORA_LSN_QUERY: &str = "
SELECT
    pg_is_in_recovery() AS replica,
    '0/0'::pg_lsn AS lsn,
    0::bigint AS offset_bytes,
    now() AS timestamp,
    0::bigint AS replay_lag_seconds
";

/// LSN information.
#[derive(Debug, Clone, Copy, Default)]
pub struct LsnStats {
    inner: StatsLsnStats,
    /// Replica replication lag in whole seconds: `now() -
    /// pg_last_xact_replay_timestamp()`, computed DB-side (one clock, no
    /// pgdog<->DB skew or timezone handling). `0` on the primary, on Aurora, or
    /// when the replica has not replayed any transaction yet.
    replay_lag_seconds: i64,
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
            replay_lag_seconds: 0,
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

    /// Estimated time for this replica to replay up to `min_lsn`: its current
    /// replication lag in time (`now() - pg_last_xact_replay_timestamp()`,
    /// measured DB-side). Used to size a client's read-your-writes defer to the
    /// real deficit. This is a stable single-sample reading — unlike a
    /// bytes/apply-rate derivative, it can't be fooled by bursty WAL replay into
    /// reporting a near-zero rate (and so a runaway ETA). Since `min_lsn`
    /// committed after the replica's current frontier, the true time to reach it
    /// is at most this lag, so the estimate is safe-biased (slightly long for an
    /// already-old `min_lsn`). Returns `None` when stats are invalid, `ZERO` once
    /// the replica has reached the floor.
    pub fn eta_to(&self, min_lsn: i64) -> Option<Duration> {
        if !self.valid() {
            return None;
        }
        if self.lsn.lsn >= min_lsn {
            return Some(Duration::ZERO);
        }
        // Clamp to a day so a pathological lag can't overflow; clients clamp
        // further. Negative (clock wobble) floors at zero.
        let secs = self.replay_lag_seconds.clamp(0, 86_400) as u64;
        Some(Duration::from_secs(secs))
    }
}

impl LsnStats {
    fn from_row(value: DataRow, aurora: bool) -> Self {
        let mut stats: LsnStats = StatsLsnStats {
            replica: value.get(0, Format::Text).unwrap_or_default(),
            lsn: value.get(1, Format::Text).unwrap_or_default(),
            offset_bytes: value.get(2, Format::Text).unwrap_or_default(),
            timestamp: value.get(3, Format::Text).unwrap_or_default(),
            fetched: SystemTime::now(),
            aurora,
        }
        .into();
        stats.replay_lag_seconds = value.get(4, Format::Text).unwrap_or_default();
        stats
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
                let stats = LsnStats::from_row(row, aurora);
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
    fn test_eta_to_uses_replay_lag() {
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

        // Behind the floor: eta is the replica's replication lag in time.
        stats.replay_lag_seconds = 7;
        assert_eq!(stats.eta_to(1500).map(|d| d.as_secs()), Some(7));

        // Already at/past the floor -> zero, regardless of lag.
        assert_eq!(stats.eta_to(1000), Some(Duration::ZERO));
        assert_eq!(stats.eta_to(500), Some(Duration::ZERO));

        // Pathological lag is clamped to a day, not overflowed.
        stats.replay_lag_seconds = i64::MAX;
        assert!(stats.eta_to(1500).unwrap().as_secs() <= 86_400);

        // Negative lag (clock wobble) floors at zero.
        stats.replay_lag_seconds = -5;
        assert_eq!(stats.eta_to(1500), Some(Duration::ZERO));
    }
}
