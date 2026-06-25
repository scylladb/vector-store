/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::ops::Add;
use std::time::Duration;
use time::Date;
use time::OffsetDateTime;
use time::PrimitiveDateTime;
use time::Time;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::From)]
pub struct Timestamp(PrimitiveDateTime);

impl Timestamp {
    pub const UNIX_EPOCH: Timestamp = Timestamp(PrimitiveDateTime::new(
        match Date::from_ordinal_date(1970, 1) {
            Ok(date) => date,
            Err(_) => panic!("Invalid date for UNIX epoch"),
        },
        Time::MIDNIGHT,
    ));

    pub fn from_unix_timestamp(timestamp: u64) -> Self {
        Self::UNIX_EPOCH + Duration::from_secs(timestamp)
    }

    /// Returns the current time in UTC.
    pub fn now() -> Self {
        let now = OffsetDateTime::now_utc();
        Timestamp(PrimitiveDateTime::new(now.date(), now.time()))
    }

    /// Returns the amount of time elapsed from this timestamp until now.
    ///
    /// Returns [`Duration::ZERO`] when this timestamp lies in the future, which
    /// can happen due to clock skew between ScyllaDB and the vector store.
    pub fn elapsed(&self) -> Duration {
        Duration::try_from(Self::now().0 - self.0).unwrap_or(Duration::ZERO)
    }
}

impl Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        Timestamp(self.0 + rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elapsed_is_zero_for_future_timestamp() {
        let future = Timestamp::now() + Duration::from_secs(3600);
        assert_eq!(future.elapsed(), Duration::ZERO);
    }

    #[test]
    fn elapsed_is_positive_for_past_timestamp() {
        let past = Timestamp::from_unix_timestamp(0);
        assert!(past.elapsed() > Duration::ZERO);
    }
}
