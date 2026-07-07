/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::anyhow;
use std::mem;
use std::mem::MaybeUninit;
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
/// A type for storing timestamps of values.
///
/// Internally it is a number of 100-nanoseconds since the UNIX epoch (January 1, 1970).
/// The MSB is reserved for internal use, so the maximum timestamp value is 2^63 - 1 nanoseconds,
/// which corresponds to approximately 29 thousands years.
pub struct Timestamp(u64);

impl Timestamp {
    pub const MIN: Timestamp = Timestamp(0);
    pub const MAX: Timestamp = Timestamp(Self::MAX_100_NANOS);

    const MAX_100_NANOS: u64 = u64::MAX >> 1;

    const MAX_MICROS: u64 = Self::MAX_100_NANOS / 10;

    const MAX_MILLIS: u64 = Self::MAX_MICROS / 1_000;

    const MAX_SECONDS: u64 = Self::MAX_MILLIS / 1_000;

    /// Creates a new `Timestamp` from a number of 100-nanoseconds since the UNIX epoch.
    pub const fn from_100_nanos(timestamp: u64) -> Self {
        let timestamp = if timestamp > Self::MAX_100_NANOS {
            Self::MAX_100_NANOS
        } else {
            timestamp
        };
        Timestamp(timestamp)
    }

    /// Creates a new `Timestamp` from a number of microseconds since the UNIX epoch.
    pub const fn from_micros(timestamp: u64) -> Self {
        let timestamp = if timestamp > Self::MAX_MICROS {
            Self::MAX_MICROS
        } else {
            timestamp
        };
        Timestamp(timestamp * 10)
    }

    /// Creates a new `Timestamp` from a number of milliseconds since the UNIX epoch.
    pub const fn from_millis(timestamp: u64) -> Self {
        let timestamp = if timestamp > Self::MAX_MILLIS {
            Self::MAX_MILLIS
        } else {
            timestamp
        };
        Timestamp(timestamp * 10_000)
    }

    /// Creates a new `Timestamp` from a number of seconds since the UNIX epoch.
    pub const fn from_seconds(timestamp: u64) -> Self {
        let timestamp = if timestamp > Self::MAX_SECONDS {
            Self::MAX_SECONDS
        } else {
            timestamp
        };
        Timestamp(timestamp * 10_000_000)
    }

    /// Returns the current time in UTC.
    pub fn now() -> Self {
        let offset_100_ns =
            (OffsetDateTime::now_utc() - OffsetDateTime::UNIX_EPOCH).whole_nanoseconds() / 100;
        if offset_100_ns < 0 {
            Self::MIN
        } else if offset_100_ns > Self::MAX_100_NANOS as i128 {
            Self::MAX
        } else {
            Self(offset_100_ns as u64)
        }
    }

    /// Returns the amount of time elapsed from this timestamp until now.
    ///
    /// Returns [`Duration::ZERO`] when this timestamp lies in the future, which
    /// can happen due to clock skew between ScyllaDB and the vector store.
    pub fn elapsed(&self) -> Duration {
        let now = Self::now();
        if self.0 > now.0 {
            Duration::ZERO
        } else {
            Duration::from_nanos_u128((now.0 - self.0) as u128 * 100)
        }
    }
}

impl TryFrom<Uuid> for Timestamp {
    type Error = anyhow::Error;

    fn try_from(uuid: Uuid) -> anyhow::Result<Self> {
        let (seconds, nanos) = uuid
            .get_timestamp()
            .ok_or(anyhow!("UUID has no timestamp"))?
            .to_unix();
        Ok(Timestamp::from_100_nanos(
            Timestamp::from_seconds(seconds).0 + (nanos / 100) as u64,
        ))
    }
}

#[derive(Debug)]
/// A value with an associated Timestamp.
///
/// It stores the timestamp of the value, a flag indicating whether the value is tombstone, and
/// the value itself.
pub struct Timestamped<T> {
    timestamp: u64,
    value: MaybeUninit<T>,
}

impl<T> Timestamped<T> {
    const DELETED_FLAG: u64 = 1 << 63;
    const TIMESTAMP_MASK: u64 = !Self::DELETED_FLAG;

    /// Creates a new `Timestamped` value with the given timestamp and optional value.
    pub fn new(timestamp: Timestamp, value: Option<T>) -> Self {
        let mut timestamp = timestamp.0;

        let value = if let Some(value) = value {
            timestamp &= Self::TIMESTAMP_MASK;
            MaybeUninit::new(value)
        } else {
            timestamp |= Self::DELETED_FLAG;
            MaybeUninit::uninit()
        };

        Timestamped { timestamp, value }
    }

    /// Returns true if the value is valid (not a tombstone).
    pub const fn is_valid(&self) -> bool {
        (self.timestamp & Self::DELETED_FLAG) == 0
    }

    /// Returns true if the value is a tombstone (deleted).
    pub const fn is_tombstone(&self) -> bool {
        !self.is_valid()
    }

    /// Returns the timestamp of the value.
    pub const fn timestamp(&self) -> Timestamp {
        Timestamp(self.timestamp & Self::TIMESTAMP_MASK)
    }

    /// Returns a reference to the value if it is valid, or None if it is a tombstone.
    pub fn value(&self) -> Option<&T> {
        (self.is_valid()).then_some(
            // SAFETY: value is initialized if and only if the deleted flag is not set
            unsafe { self.value.assume_init_ref() },
        )
    }

    /// Consumes the `Timestamped` value and returns the inner value if it is valid, or None if it
    /// is a tombstone.
    pub fn into_value(mut self) -> Option<T> {
        if self.is_valid() {
            // We are moving the value out of self and mark self.value as delete to avoid dropping
            // uninitialized value in the Drop implementation
            let value = mem::replace(&mut self.value, MaybeUninit::uninit());
            self.timestamp |= Self::DELETED_FLAG;

            // SAFETY: value is initialized if and only if the deleted flag is not set.
            Some(unsafe { value.assume_init() })
        } else {
            None
        }
    }

    /// Sets the value and timestamp of the `Timestamped` value.
    pub fn set(&mut self, timestamp: Timestamp, value: Option<T>) {
        // We are setting a new value, so the old value will be dropped
        self.drop_if_valid();

        let mut timestamp = timestamp.0;

        let value = if let Some(value) = value {
            timestamp &= Self::TIMESTAMP_MASK;
            MaybeUninit::new(value)
        } else {
            timestamp |= Self::DELETED_FLAG;
            MaybeUninit::uninit()
        };

        self.timestamp = timestamp;
        self.value = value;
    }

    fn drop_if_valid(&mut self) {
        if self.is_valid() {
            // SAFETY: value is initialized if and only if the deleted flag is not set
            unsafe { self.value.assume_init_drop() };
        }
    }
}

impl<T: Clone> Clone for Timestamped<T> {
    fn clone(&self) -> Self {
        let value = if self.is_valid() {
            // SAFETY: value is initialized if and only if the deleted flag is not set
            MaybeUninit::new(unsafe { self.value.assume_init_ref().clone() })
        } else {
            MaybeUninit::uninit()
        };
        Timestamped {
            timestamp: self.timestamp,
            value,
        }
    }
}

impl<T: PartialEq> PartialEq for Timestamped<T> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.value() == other.value()
    }
}

impl<T: PartialEq> Eq for Timestamped<T> {}

impl<T> Drop for Timestamped<T> {
    fn drop(&mut self) {
        self.drop_if_valid();
    }
}

impl Timestamped<()> {
    /// Creates a new `Timestamped` value with the given timestamp and no value (tombstone).
    pub(crate) const fn new_tombstone(timestamp: Timestamp) -> Self {
        Self::new_valid_or_tombstone(timestamp, false)
    }

    /// Creates a new `Timestamped` value with the given timestamp and value (valid).
    pub(crate) const fn new_valid(timestamp: Timestamp) -> Self {
        Self::new_valid_or_tombstone(timestamp, true)
    }

    /// Converts the `Timestamped` value into an array of bytes in native endian order.
    pub(crate) const fn into_ne_bytes(self) -> [u8; 8] {
        // SAFETY: Timestamped<()> contains only timestamp integer and integers are plain old
        // datatypes so we can always transmute them to arrays of bytes
        unsafe { mem::transmute(self) }
    }

    /// Creates a new `Timestamped` value from an array of bytes in native endian order.
    pub(crate) const fn from_ne_bytes(bytes: [u8; 8]) -> Self {
        // SAFETY: Timestamped<()> contains only timestamp integer and integers are plain old
        // datatypes so we can always transmute them to arrays of bytes
        unsafe { mem::transmute(bytes) }
    }

    const fn new_valid_or_tombstone(timestamp: Timestamp, valid: bool) -> Self {
        let mut timestamp = timestamp.0;

        let value = if valid {
            timestamp &= Self::TIMESTAMP_MASK;
            MaybeUninit::new(())
        } else {
            timestamp |= Self::DELETED_FLAG;
            MaybeUninit::uninit()
        };

        Timestamped { timestamp, value }
    }
}

#[cfg(test)]
mod tests {
    use uuid::ContextV1;

    use super::*;

    #[test]
    fn timestamp() {
        let timestamp = Timestamp::from_100_nanos(45);
        assert_eq!(timestamp.0, 45);

        let timestamp = Timestamp::from_micros(1_000_000);
        assert_eq!(timestamp.0, 10_000_000);

        let timestamp = Timestamp::from_millis(1_000);
        assert_eq!(timestamp.0, 10_000_000);

        let timestamp = Timestamp::from_seconds(1);
        assert_eq!(timestamp.0, 10_000_000);

        let timestamp = Timestamp::from_100_nanos(u64::MAX);
        assert_eq!(timestamp.0, (1 << 63) - 1);

        let timestamp = Timestamp::from_micros(u64::MAX);
        assert_eq!(timestamp.0, (((1 << 63) - 1) / 10) * 10);

        let timestamp = Timestamp::from_millis(u64::MAX);
        assert_eq!(timestamp.0, (((1 << 63) - 1) / 10_000) * 10_000);

        let timestamp = Timestamp::from_seconds(u64::MAX);
        assert_eq!(timestamp.0, (((1 << 63) - 1) / 10_000_000) * 10_000_000);
    }

    #[test]
    fn timestamp_from_uuid() {
        let context = ContextV1::new_random();
        assert!(Timestamp::try_from(Uuid::new_v4()).is_err());

        let ts = uuid::timestamp::Timestamp::from_unix(&context, 0, 0);
        let uuid = Uuid::new_v1(ts, &[0; 6]);
        let timestamp = Timestamp::try_from(uuid).unwrap();
        assert_eq!(timestamp, Timestamp::from_micros(0));

        let ts = uuid::timestamp::Timestamp::from_unix(&context, 100, 0);
        let uuid = Uuid::new_v1(ts, &[0; 6]);
        let timestamp = Timestamp::try_from(uuid).unwrap();
        assert_eq!(timestamp, Timestamp::from_seconds(100));

        let ts = uuid::timestamp::Timestamp::from_unix(&context, 200, 100);
        let uuid = Uuid::new_v1(ts, &[0; 6]);
        let timestamp = Timestamp::try_from(uuid).unwrap();
        assert_eq!(timestamp, Timestamp::from_100_nanos(2_000_000_001));

        let ts = uuid::timestamp::Timestamp::from_unix(&context, 200, 1_000);
        let uuid = Uuid::new_v1(ts, &[0; 6]);
        let timestamp = Timestamp::try_from(uuid).unwrap();
        assert_eq!(timestamp, Timestamp::from_100_nanos(2_000_000_010));

        let ts = uuid::timestamp::Timestamp::from_unix(&context, u64::MAX, 0);
        let uuid = Uuid::new_v1(ts, &[0; 6]);
        let timestamp = Timestamp::try_from(uuid).unwrap();
        assert_eq!(timestamp, Timestamp::from_100_nanos(u64::MAX));
    }

    #[test]
    fn timestamped() {
        let timestamped = Timestamped::<()>::new(Timestamp::from_micros(100), None);
        assert!(!timestamped.is_valid());
        assert!(timestamped.is_tombstone());
        assert_eq!(timestamped.timestamp(), Timestamp::from_micros(100));
        assert_eq!(timestamped.value(), None);

        let mut timestamped = Timestamped::new(Timestamp::from_millis(1_000), Some("value"));
        assert!(timestamped.is_valid());
        assert_eq!(timestamped.timestamp(), Timestamp::from_millis(1_000));
        assert_eq!(timestamped.value(), Some(&"value"));

        timestamped.set(Timestamp::from_millis(2_000), None);
        assert!(timestamped.is_tombstone());
        assert_eq!(timestamped.timestamp(), Timestamp::from_millis(2_000));
        assert_eq!(timestamped.value(), None);

        timestamped.set(Timestamp::from_millis(3_000), Some("new value"));
        assert!(timestamped.is_valid());
        assert_eq!(timestamped.timestamp(), Timestamp::from_millis(3_000));
        assert_eq!(timestamped.value(), Some(&"new value"));
    }

    #[test]
    fn timestamped_ne_bytes() {
        let timestamped = Timestamped::new_tombstone(Timestamp::from_micros(100));
        let bytes = timestamped.into_ne_bytes();
        let timestamped = Timestamped::from_ne_bytes(bytes);
        assert!(!timestamped.is_valid());
        assert!(timestamped.is_tombstone());
        assert_eq!(timestamped.timestamp(), Timestamp::from_micros(100));
        assert_eq!(timestamped.value(), None);

        let timestamped = Timestamped::new_valid(Timestamp::from_millis(1_000));
        let bytes = timestamped.into_ne_bytes();
        let timestamped = Timestamped::from_ne_bytes(bytes);
        assert!(timestamped.is_valid());
        assert!(!timestamped.is_tombstone());
        assert_eq!(timestamped.timestamp(), Timestamp::from_millis(1_000));
        assert_eq!(timestamped.value(), Some(&()));
    }

    #[test]
    fn elapsed_is_zero_for_future_timestamp() {
        let future =
            Timestamp::from_100_nanos(Timestamp::now().0 + Timestamp::from_seconds(3600).0);
        assert_eq!(future.elapsed(), Duration::ZERO);
    }

    #[test]
    fn elapsed_is_positive_for_past_timestamp() {
        let past = Timestamp::MIN;
        assert!(past.elapsed() > Duration::ZERO);
    }
}
