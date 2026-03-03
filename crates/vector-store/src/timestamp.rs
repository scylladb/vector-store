/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::ops::Add;
use std::time::Duration;
use time::Date;
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
}

impl Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        Timestamp(self.0 + rhs)
    }
}
