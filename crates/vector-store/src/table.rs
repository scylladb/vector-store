/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::DbEmbedding;
use crate::IndexKey;
use crate::PartitionKey;
use crate::PrimaryKey;
use crate::Restriction;
use crate::Timestamp;
use crate::Vector;
use anyhow::anyhow;
use anyhow::bail;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlDate;
use scylla::value::CqlTime;
use scylla::value::CqlTimestamp;
use scylla::value::CqlTimeuuid;
use scylla::value::CqlValue;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::btree_map::Entry;
use std::mem;
use std::net::IpAddr;
use std::sync::Arc;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

/// Idx is a trait for types that can be used as an index in the column vectors.
trait Idx {
    fn idx(&self) -> usize;
}

mod primary_id {
    use super::*;

    /// PrimaryId is a unique identifier for a row in the table. It consists of an index and an
    /// epoch. The index is used to access the row in the column vectors, while the epoch is used
    /// to determine if the row has been updated.
    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        derive_more::AsRef,
        derive_more::From,
        derive_more::Into,
    )]
    pub struct PrimaryId(u64);

    const _: () = assert!(
        mem::size_of::<PrimaryId>() == mem::size_of::<usize>(),
        "PrimaryId should be the same size as usize"
    );

    impl PrimaryId {
        const EPOCH_SHIFT: usize = (mem::size_of::<u64>() - mem::size_of::<Epoch>()) * 8;
        const MAX: u64 = !((Epoch::MAX as u64) << Self::EPOCH_SHIFT);

        pub(super) fn try_new(idx: usize, epoch: Epoch) -> anyhow::Result<Self> {
            if idx as u64 > Self::MAX {
                bail!("PrimaryId is too large: {idx}");
            }
            Ok(Self(
                (*epoch.as_ref() as u64) << Self::EPOCH_SHIFT | idx as u64,
            ))
        }

        pub(super) fn new_epoch(mut self, epoch: Epoch) -> Self {
            self.0 &= Self::MAX;
            self.0 |= (epoch.0 as u64) << Self::EPOCH_SHIFT;
            self
        }

        pub(super) fn next_epoch(self) -> Self {
            self.new_epoch(self.epoch().next())
        }

        pub(super) fn epoch(&self) -> Epoch {
            Epoch((self.0 >> Self::EPOCH_SHIFT) as u16)
        }
    }

    impl Idx for PrimaryId {
        fn idx(&self) -> usize {
            (self.0 & Self::MAX) as usize
        }
    }

    /// Epoch is a counter that is incremented every time a row is updated. New values are
    /// cyclically assigned epochs, so we can have at most Epoch::MAX updates (around 65k), so
    /// assuming one change per millisecond, we have around 65 seconds for unique epochs.
    /// Vectors with old epochs must be updated within that time (it seems reasonable).
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::AsRef)]
    pub(super) struct Epoch(u16);

    impl Epoch {
        const MIN: u16 = 0;
        const MAX: u16 = u16::MAX;

        pub(super) fn new() -> Self {
            Self(Self::MIN)
        }

        pub(super) fn next(self) -> Self {
            if self.0 == Self::MAX {
                info!("Epoch overflow: all epochs are used, starting from the minimum epoch");
                Self(Self::MIN)
            } else {
                Self(self.0 + 1)
            }
        }
    }
}
use primary_id::Epoch;
pub use primary_id::PrimaryId;

mod partition_id {
    use super::*;

    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::From)]
    pub struct PartitionId(u64);

    const _: () = assert!(
        mem::size_of::<PartitionId>() == mem::size_of::<usize>(),
        "PartitionId should be the same size as usize"
    );

    impl PartitionId {
        const INDEX_ID_SHIFT: usize = (mem::size_of::<u64>() - mem::size_of::<IndexId>()) * 8;
        const MAX: u64 = !((IndexId::MAX as u64) << Self::INDEX_ID_SHIFT);

        pub(super) fn try_new(idx: usize, index_id: IndexId) -> anyhow::Result<Self> {
            if idx as u64 > Self::MAX {
                bail!("PartitionId is too large: {idx}");
            }
            Ok(Self(
                (*index_id.as_ref() as u64) << Self::INDEX_ID_SHIFT | idx as u64,
            ))
        }

        pub(super) fn global(index_id: IndexId) -> Self {
            Self((*index_id.as_ref() as u64) << Self::INDEX_ID_SHIFT)
        }

        pub(crate) fn index_id(&self) -> IndexId {
            IndexId((self.0 >> Self::INDEX_ID_SHIFT) as u16)
        }
    }

    impl Idx for PartitionId {
        fn idx(&self) -> usize {
            (self.0 & Self::MAX) as usize
        }
    }

    /// IndexId provides unique IndexIds for indexes in the table. We can have up to
    /// IndexId::MAX indexes (~65k), which seems reasonable for a single table.
    /// IndexId::MAX is used as a sentinel to mark that there are no more IndexIds
    /// available.
    #[derive(
        Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::AsRef, derive_more::From,
    )]
    pub(crate) struct IndexId(u16);

    impl IndexId {
        const MAX: u16 = u16::MAX;
    }

    #[derive(Debug)]
    pub(super) struct IndexIdGenerator {
        next: u16,
    }

    impl IndexIdGenerator {
        pub(super) fn new() -> Self {
            Self { next: 0 }
        }

        pub(super) fn next(&mut self) -> anyhow::Result<IndexId> {
            if self.next == IndexId::MAX {
                bail!("No more IndexIds available");
            }
            let index_id = IndexId(self.next);
            self.next += 1;
            Ok(index_id)
        }
    }
}
pub(crate) use partition_id::IndexId;
use partition_id::IndexIdGenerator;
pub use partition_id::PartitionId;

/// A newtype for partition size
#[derive(Clone, Copy, Debug, derive_more::From, derive_more::Into, derive_more::AsRef)]
struct PartitionSize(usize);

mod column_vec {
    use super::*;

    /// ColumnVec is a wrapper around Vec and generic index type. It is used to safely access columns by specific index types.
    #[derive(Debug)]
    pub(super) struct ColumnVec<I, T> {
        vec: Vec<T>,
        _index: std::marker::PhantomData<I>,
    }

    impl<I: Idx, T> ColumnVec<I, T> {
        pub(super) fn new() -> Self {
            Self {
                vec: Vec::new(),
                _index: std::marker::PhantomData,
            }
        }

        pub(super) fn resize_with(&mut self, size: usize, f: impl FnMut() -> T) {
            self.vec.resize_with(size, f);
        }

        pub(super) fn get(&self, idx: I) -> Option<&T> {
            self.vec.get(idx.idx())
        }

        pub(super) fn get_mut(&mut self, idx: I) -> Option<&mut T> {
            self.vec.get_mut(idx.idx())
        }

        pub(super) fn update(&mut self, idx: I, value: T) -> anyhow::Result<()> {
            *self
                .get_mut(idx)
                .ok_or_else(|| anyhow!("Index out of ColumnVec bounds"))? = value;
            Ok(())
        }
    }

    impl<T> ColumnVec<PrimaryId, ETValue<T>> {
        pub(super) fn update_epoch_timestamp(
            &mut self,
            primary_id: PrimaryId,
            timestamp: Timestamp,
        ) -> anyhow::Result<()> {
            self.get_mut(primary_id)
                .map(|value| {
                    value.update_epoch_timestamp(primary_id.epoch(), timestamp);
                })
                .ok_or_else(|| anyhow!("Index out of ColumnVec bounds"))
        }
    }
}
use column_vec::ColumnVec;

/// An enum that can store Epoch, Timestamp and optionally a value
#[derive(Debug)]
enum ETValue<T> {
    None(Epoch, Timestamp),
    Some(Epoch, Timestamp, T),
}

const _: () = assert!(
    mem::size_of::<ETValue<()>>() <= 2 * mem::size_of::<u64>(),
    "The impact of Epoch, Timestamp and enum in ColumnValue shouldn't be larger that 2 * u64"
);

impl<T> ETValue<T> {
    fn update_epoch_timestamp(&mut self, epoch: Epoch, timestamp: Timestamp) {
        match self {
            Self::None(value_epoch, value_timestamp)
            | Self::Some(value_epoch, value_timestamp, _) => {
                *value_epoch = epoch;
                *value_timestamp = timestamp;
            }
        }
    }
}

/// An enum that can store Timestamp and optionally a value
#[derive(Debug)]
enum TValue<T> {
    #[allow(dead_code)]
    None(Timestamp),
    #[allow(dead_code)]
    Some(Timestamp, T),
}
impl<T> TValue<T> {
    fn get(&self) -> Option<&T> {
        match self {
            Self::None(_) => None,
            Self::Some(_, value) => Some(value),
        }
    }
}

/// A newtype for defining the offset of the key column.
#[derive(Clone, Copy, Debug, derive_more::From, derive_more::Into)]
struct KeyOffset(usize);

/// An enum that represents a column in the table. It can be a column with values of a specific
/// type or a primary key column (as an offset in the primary key columns).
#[derive(Debug)]
enum Column {
    Ascii(ColumnVec<PrimaryId, TValue<String>>),
    BigInt(ColumnVec<PrimaryId, TValue<i64>>),
    Blob(ColumnVec<PrimaryId, TValue<Vec<u8>>>),
    Boolean(ColumnVec<PrimaryId, TValue<bool>>),
    Date(ColumnVec<PrimaryId, TValue<CqlDate>>),
    Double(ColumnVec<PrimaryId, TValue<f64>>),
    Float(ColumnVec<PrimaryId, TValue<f32>>),
    Inet(ColumnVec<PrimaryId, TValue<IpAddr>>),
    Int(ColumnVec<PrimaryId, TValue<i32>>),
    SmallInt(ColumnVec<PrimaryId, TValue<i16>>),
    Text(ColumnVec<PrimaryId, TValue<String>>),
    Time(ColumnVec<PrimaryId, TValue<CqlTime>>),
    Timestamp(ColumnVec<PrimaryId, TValue<CqlTimestamp>>),
    Timeuuid(ColumnVec<PrimaryId, TValue<CqlTimeuuid>>),
    TinyInt(ColumnVec<PrimaryId, TValue<i8>>),
    Uuid(ColumnVec<PrimaryId, TValue<Uuid>>),
    PrimaryKey(KeyOffset),
}

impl Column {
    fn new(native_type: &NativeType) -> anyhow::Result<Self> {
        Ok(match native_type {
            NativeType::Ascii => Self::Ascii(ColumnVec::new()),
            NativeType::BigInt => Self::BigInt(ColumnVec::new()),
            NativeType::Blob => Self::Blob(ColumnVec::new()),
            NativeType::Boolean => Self::Boolean(ColumnVec::new()),
            NativeType::Date => Self::Date(ColumnVec::new()),
            NativeType::Double => Self::Double(ColumnVec::new()),
            NativeType::Float => Self::Float(ColumnVec::new()),
            NativeType::Inet => Self::Inet(ColumnVec::new()),
            NativeType::Int => Self::Int(ColumnVec::new()),
            NativeType::SmallInt => Self::SmallInt(ColumnVec::new()),
            NativeType::Text => Self::Text(ColumnVec::new()),
            NativeType::Time => Self::Time(ColumnVec::new()),
            NativeType::Timestamp => Self::Timestamp(ColumnVec::new()),
            NativeType::Timeuuid => Self::Timeuuid(ColumnVec::new()),
            NativeType::TinyInt => Self::TinyInt(ColumnVec::new()),
            NativeType::Uuid => Self::Uuid(ColumnVec::new()),
            _ => bail!("Unsupported native type: {native_type:?}"),
        })
    }

    fn resize_with(&mut self, size: usize) {
        let timestamp = Timestamp::UNIX_EPOCH;
        match self {
            Self::Ascii(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::BigInt(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Blob(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Boolean(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Date(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Double(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Float(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Inet(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Int(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::SmallInt(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Text(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Time(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Timestamp(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Timeuuid(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::TinyInt(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::Uuid(vec) => vec.resize_with(size, || TValue::None(timestamp)),
            Self::PrimaryKey(_) => {}
        }
    }

    #[allow(dead_code)]
    fn insert_cqlvalue(
        &mut self,
        primary_id: PrimaryId,
        timestamp: Timestamp,
        value: CqlValue,
    ) -> anyhow::Result<()> {
        match self {
            Self::Ascii(vec) => {
                let CqlValue::Ascii(value) = value else {
                    bail!("Failed to convert value to Ascii");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::BigInt(vec) => {
                let CqlValue::BigInt(value) = value else {
                    bail!("Failed to convert value to BigInt");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Blob(vec) => {
                let CqlValue::Blob(value) = value else {
                    bail!("Failed to convert value to Blob");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Boolean(vec) => {
                let CqlValue::Boolean(value) = value else {
                    bail!("Failed to convert value to Boolean");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Date(vec) => {
                let CqlValue::Date(value) = value else {
                    bail!("Failed to convert value to Date");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Double(vec) => {
                let CqlValue::Double(value) = value else {
                    bail!("Failed to convert value to Double");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Float(vec) => {
                let CqlValue::Float(value) = value else {
                    bail!("Failed to convert value to Float");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Inet(vec) => {
                let CqlValue::Inet(value) = value else {
                    bail!("Failed to convert value to Inet");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Int(vec) => {
                let CqlValue::Int(value) = value else {
                    bail!("Failed to convert value to Int");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::SmallInt(vec) => {
                let CqlValue::SmallInt(value) = value else {
                    bail!("Failed to convert value to SmallInt");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Text(vec) => {
                let CqlValue::Text(value) = value else {
                    bail!("Failed to convert value to Text");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Time(vec) => {
                let CqlValue::Time(value) = value else {
                    bail!("Failed to convert value to Time");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Timestamp(vec) => {
                let CqlValue::Timestamp(value) = value else {
                    bail!("Failed to convert value to Timestamp");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Timeuuid(vec) => {
                let CqlValue::Timeuuid(value) = value else {
                    bail!("Failed to convert value to Timeuuid");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::TinyInt(vec) => {
                let CqlValue::TinyInt(value) = value else {
                    bail!("Failed to convert value to TinyInt");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::Uuid(vec) => {
                let CqlValue::Uuid(value) = value else {
                    bail!("Failed to convert value to Uuid");
                };
                vec.update(primary_id, TValue::Some(timestamp, value))
            }
            Self::PrimaryKey(_) => bail!("Cannot insert value into PrimaryKey column"),
        }
    }

    fn get(
        &self,
        primary_id: PrimaryId,
        primary_keys: &ColumnVec<PrimaryId, Option<PrimaryKey>>,
    ) -> Option<CqlValue> {
        match self {
            Self::Ascii(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Ascii),
            Self::BigInt(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::BigInt),
            Self::Blob(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Blob),
            Self::Boolean(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Boolean),
            Self::Date(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Date),
            Self::Double(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Double),
            Self::Float(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Float),
            Self::Inet(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Inet),
            Self::Int(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Int),
            Self::SmallInt(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::SmallInt),
            Self::Text(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Text),
            Self::Time(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Time),
            Self::Timestamp(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Timestamp),
            Self::Timeuuid(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Timeuuid),
            Self::TinyInt(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::TinyInt),
            Self::Uuid(vec) => vec
                .get(primary_id)
                .and_then(|val| val.get())
                .cloned()
                .map(CqlValue::Uuid),
            Self::PrimaryKey(key_offset) => primary_keys
                .get(primary_id)
                .and_then(|opt_key| opt_key.as_ref())
                .and_then(|key| key.get((*key_offset).into())),
        }
    }
}

/// A struct that stores free PrimaryIds. It is used to efficiently use ids of preallocated or
/// deleted rows.
#[derive(Debug)]
struct FreePrimaryIds(VecDeque<PrimaryId>);

impl FreePrimaryIds {
    fn take_id(&mut self) -> anyhow::Result<PrimaryId> {
        self.0
            .pop_front()
            .ok_or(anyhow!("Primary ids should be reserved"))
    }
}

/// A struct that stores free PartitionIds. It is used to efficiently use ids of preallocated or
/// deleted rows.
#[derive(Debug)]
struct FreePartitionIds(VecDeque<PartitionId>);

impl FreePartitionIds {
    fn take_id(&mut self) -> anyhow::Result<PartitionId> {
        self.0
            .pop_front()
            .ok_or(anyhow!("Partition ids should be reserved"))
    }

    fn return_id(&mut self, id: PartitionId) {
        self.0.push_back(id);
    }
}

/// An enum that represents the data of an index specific to the index type.
#[derive(Debug)]
enum IndexData {
    Global,
    Local {
        /// Column names for which the index is built. The order of column names is important, as it
        /// defines the order of values in the index key.
        key_columns: Arc<Vec<ColumnName>>,

        map: BTreeMap<PartitionKey, PartitionId>,
        free_ids: FreePartitionIds,
        keys: ColumnVec<PartitionId, Option<PartitionKey>>,
        ids: ColumnVec<PrimaryId, Option<PartitionId>>,

        sizes: ColumnVec<PartitionId, PartitionSize>,
    },
}

impl IndexData {
    /// Returns true if partition is empty
    fn remove_row(&mut self, primary_id: PrimaryId) -> bool {
        match self {
            Self::Global { .. } => false,
            Self::Local {
                ids,
                keys,
                sizes,
                map,
                free_ids,
                ..
            } => {
                let Some(Some(partition_id)) = ids.get_mut(primary_id).map(|id| id.take()) else {
                    return false;
                };

                let is_empty = sizes
                    .get_mut(partition_id)
                    .map(|size| {
                        if size.0 > 0 {
                            size.0 -= 1;
                        }
                        size.0 == 0
                    })
                    .unwrap_or(false);
                if is_empty && let Some(key) = keys.get_mut(partition_id).and_then(|key| key.take())
                {
                    map.remove(&key);
                    free_ids.return_id(partition_id);
                }
                is_empty
            }
        }
    }
}

/// A struct that represents an index in the table.
#[derive(Debug)]
struct Index {
    index_id: IndexId,

    data: IndexData,

    /// All column names that are used in this index (key columns + filtering columns)
    _available_columns: BTreeSet<ColumnName>,

    /// Timestamps of the last vector update
    vector_timestamps: ColumnVec<PrimaryId, ETValue<()>>,
}

impl Index {
    const INCREMENT_SIZE: usize = 1 << 8;

    fn new_global(
        index_id: IndexId,
        primary_key_columns: Arc<Vec<ColumnName>>,
        filtering_columns: &[ColumnName],
    ) -> Self {
        Self {
            index_id,
            data: IndexData::Global,
            _available_columns: primary_key_columns
                .iter()
                .chain(filtering_columns.iter())
                .cloned()
                .collect(),
            vector_timestamps: ColumnVec::new(),
        }
    }

    fn new_local(
        index_id: IndexId,
        key_columns: Arc<Vec<ColumnName>>,
        filtering_columns: &[ColumnName],
    ) -> Self {
        Self {
            index_id,
            _available_columns: key_columns
                .iter()
                .chain(filtering_columns.iter())
                .cloned()
                .collect(),
            vector_timestamps: ColumnVec::new(),
            data: IndexData::Local {
                key_columns,
                map: BTreeMap::new(),
                free_ids: FreePartitionIds(VecDeque::new()),
                keys: ColumnVec::new(),
                ids: ColumnVec::new(),
                sizes: ColumnVec::new(),
            },
        }
    }

    fn resize_primary_ids_with(&mut self, new_size: usize) {
        match &mut self.data {
            IndexData::Global => {}
            IndexData::Local { ids, .. } => ids.resize_with(new_size, || None),
        }
        self.vector_timestamps.resize_with(new_size, || {
            ETValue::None(Epoch::new(), Timestamp::UNIX_EPOCH)
        });
    }

    fn resize_partition_ids(&mut self) -> anyhow::Result<()> {
        let IndexData::Local {
            map,
            free_ids,
            keys,
            sizes,
            ..
        } = &mut self.data
        else {
            return Ok(());
        };
        if !free_ids.0.is_empty() {
            return Ok(());
        }
        let start = map.len();
        let end = start + Self::INCREMENT_SIZE;
        free_ids.0.reserve(Self::INCREMENT_SIZE);
        (start..end)
            .map(|id| PartitionId::try_new(id, self.index_id))
            .try_for_each(|id| {
                id.map(|id| {
                    free_ids.0.push_back(id);
                })
            })?;
        keys.resize_with(end, || None);
        sizes.resize_with(end, || PartitionSize(0));
        Ok(())
    }

    fn partition_id(&self, primary_id: PrimaryId) -> Option<PartitionId> {
        match &self.data {
            IndexData::Global => Some(PartitionId::global(self.index_id)),
            IndexData::Local { ids, .. } => ids.get(primary_id).copied().flatten(),
        }
    }

    fn partition_key(
        &self,
        primary_key_columns: &[ColumnName],
        primary_key: &PrimaryKey,
    ) -> Option<PartitionKey> {
        match &self.data {
            IndexData::Global => None,
            IndexData::Local { key_columns, .. } => key_columns
                .iter()
                .map(|name| {
                    primary_key_columns
                        .iter()
                        .position(|col_name| col_name == name)
                        .and_then(|idx| primary_key.get(idx))
                })
                .collect::<Option<PartitionKey>>(),
        }
    }

    fn add(
        &mut self,
        primary_id: PrimaryId,
        partition_key: PartitionKey,
    ) -> anyhow::Result<PartitionId> {
        match &mut self.data {
            IndexData::Global => bail!("Cannot add partition key to global index"),
            IndexData::Local {
                map,
                free_ids,
                keys,
                ids,
                sizes,
                ..
            } => match map.entry(partition_key.clone()) {
                Entry::Occupied(entry) => {
                    let partition_id = *entry.get();
                    let id = ids
                        .get_mut(primary_id)
                        .ok_or_else(|| anyhow!("PrimaryId index out of partition ids bounds"))?;
                    if let Some(current_id) = &id {
                        bail!(
                            "Partition key {partition_key:?} already exists for primary id {current_id:?}",
                        );
                    }
                    id.replace(partition_id);
                    sizes
                        .get_mut(partition_id)
                        .ok_or_else(|| anyhow!("PartitionId index out of partition sizes bounds"))?
                        .0 += 1;
                    Ok(partition_id)
                }
                Entry::Vacant(entry) => {
                    let partition_id = free_ids.take_id()?;
                    entry.insert(partition_id);
                    keys.get_mut(partition_id)
                        .ok_or_else(|| anyhow!("PartitionId index out of partition keys bounds"))?
                        .replace(partition_key);
                    ids.get_mut(primary_id)
                        .ok_or_else(|| anyhow!("PrimaryId index out of partition ids bounds"))?
                        .replace(partition_id);
                    sizes
                        .get_mut(partition_id)
                        .ok_or_else(|| anyhow!("PartitionId index out of partition sizes bounds"))?
                        .0 = 1;
                    Ok(partition_id)
                }
            },
        }
    }
}

/// A struct that represents a table in the database.
#[derive(Debug)]
pub struct Table {
    primary_key_columns: Arc<Vec<ColumnName>>,
    primary_ids: BTreeMap<PrimaryKey, PrimaryId>,
    free_primary_ids: FreePrimaryIds,
    primary_keys: ColumnVec<PrimaryId, Option<PrimaryKey>>,

    columns: BTreeMap<ColumnName, Column>,

    _index_id_generator: IndexIdGenerator,
    index_ids: BTreeMap<IndexKey, IndexId>,
    indexes: BTreeMap<IndexId, Index>,
}

impl Table {
    const INCREMENT_SIZE: usize = 1 << 10;

    pub(crate) fn new(
        index_key: IndexKey,
        primary_key_columns: Arc<Vec<ColumnName>>,
        partition_key_columns: Option<Arc<Vec<ColumnName>>>,
        filtering_columns: &[ColumnName],
        table_columns: Arc<HashMap<ColumnName, NativeType>>,
    ) -> anyhow::Result<Self> {
        let mut index_id_generator = IndexIdGenerator::new();
        let mut indexes = BTreeMap::new();
        let mut index_ids = BTreeMap::new();
        let index_id = index_id_generator.next()?;
        let index = if let Some(partition_key_columns) = partition_key_columns.as_ref() {
            Index::new_local(
                index_id,
                Arc::clone(partition_key_columns),
                filtering_columns,
            )
        } else {
            Index::new_global(
                index_id,
                Arc::clone(&primary_key_columns),
                filtering_columns,
            )
        };
        indexes.insert(index_id, index);
        index_ids.insert(index_key, index_id);
        let mut columns = primary_key_columns
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.clone(), Column::PrimaryKey(idx.into())))
            .collect::<BTreeMap<_, _>>();
        let column_with_values = partition_key_columns
            .as_ref()
            .map(|vec| vec.as_slice())
            .unwrap_or(&[])
            .iter()
            .chain(filtering_columns.iter())
            .filter(|name| !columns.contains_key(*name))
            .map(|name| {
                table_columns
                    .get(name)
                    .ok_or_else(|| anyhow::anyhow!("Column {name} not found in table columns"))
                    .and_then(Column::new)
                    .map(|column| (name.clone(), column))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        columns.extend(column_with_values);
        let mut table = Self {
            primary_ids: BTreeMap::new(),
            free_primary_ids: FreePrimaryIds(VecDeque::new()),
            primary_keys: ColumnVec::new(),
            primary_key_columns,
            columns,
            _index_id_generator: index_id_generator,
            index_ids,
            indexes,
        };
        table.reserve_primary_ids()?;
        table.reserve_partition_ids()?;
        Ok(table)
    }

    fn reserve_primary_ids(&mut self) -> anyhow::Result<()> {
        if !self.free_primary_ids.0.is_empty() {
            return Ok(());
        }
        let start = self.primary_ids.len();
        let end = start + Self::INCREMENT_SIZE;
        self.free_primary_ids.0.reserve(Self::INCREMENT_SIZE);
        (start..end)
            .map(|idx| PrimaryId::try_new(idx, Epoch::new()))
            .take_while(|id| id.is_ok())
            .for_each(|id| {
                self.free_primary_ids.0.push_back(id.unwrap());
            });
        if self.free_primary_ids.0.is_empty() {
            bail!("Failed to reserve vector ids: no more ids available");
        }
        let new_size = start + self.free_primary_ids.0.len();

        self.primary_keys.resize_with(new_size, || None);
        self.columns
            .iter_mut()
            .for_each(|(_, column)| column.resize_with(new_size));
        self.indexes
            .iter_mut()
            .for_each(|(_, index)| index.resize_primary_ids_with(new_size));
        Ok(())
    }

    fn reserve_partition_ids(&mut self) -> anyhow::Result<()> {
        self.indexes
            .iter_mut()
            .try_for_each(|(_, index)| index.resize_partition_ids())?;
        Ok(())
    }

    fn is_valid_primary_id(&self, partition_id: PartitionId, primary_id: PrimaryId) -> bool {
        self.indexes
            .get(&partition_id.index_id())
            .and_then(|index| match index.vector_timestamps.get(primary_id)? {
                ETValue::Some(epoch, _, _) => Some(*epoch),
                ETValue::None(_, _) => None,
            })
            .is_some_and(|epoch| epoch == primary_id.epoch())
    }
}

/// A trait that defines the add operation for the table.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait TableAdd {
    fn add(
        &mut self,
        index_key: &IndexKey,
        db_embedding: DbEmbedding,
    ) -> anyhow::Result<Vec<Operation>>;
}

impl TableAdd for Table {
    fn add(
        &mut self,
        _index_key: &IndexKey,
        db_embedding: DbEmbedding,
    ) -> anyhow::Result<Vec<Operation>> {
        self.reserve_primary_ids()?;
        self.reserve_partition_ids()?;

        let mut operations = vec![];

        let primary_key = db_embedding.primary_key;
        let vector = db_embedding.embedding;

        let row_map = &mut self.primary_ids;

        match row_map.entry(primary_key.clone()) {
            Entry::Occupied(entry) => {
                let primary_id = *entry.get();
                self.indexes.iter_mut().try_for_each(|(index_id, index)| {
                    let (epoch, timestamp, vector_already_exists) = match index
                        .vector_timestamps
                        .get(primary_id)
                    {
                        Some(ETValue::Some(epoch, timestamp, _)) => (*epoch, timestamp, true),
                        Some(ETValue::None(epoch, timestamp)) => (*epoch, timestamp, false),
                        None => {
                            bail!(
                                "Failed to update vector: \
                                missing vector timestamp for index_id {index_id:?} and primary_id {primary_id:?}"
                            );
                        }
                    };
                    if *timestamp >= db_embedding.timestamp {
                        return Ok(());
                    }
                    let primary_id = primary_id.new_epoch(epoch);
                    let partition_id = index.partition_id(primary_id).ok_or_else(|| {
                        anyhow!(
                            "Failed to update vector: \
                            missing partition id for index_id {index_id:?} and primary_id {primary_id:?}"
                        )
                    })?;
                    if let Some(vector) = &vector {
                        if vector_already_exists {
                            operations.push(Operation::RemoveBeforeAddVector {
                                primary_id,
                                partition_id,
                            });
                        }

                        let primary_id = primary_id.next_epoch();
                        let timestamp = db_embedding.timestamp;
                        operations.push(Operation::AddVector {
                            primary_id,
                            partition_id,
                            vector: vector.clone(),
                            is_update: vector_already_exists,
                        });
                        index
                            .vector_timestamps
                            .update_epoch_timestamp(primary_id, timestamp)?;
                    } else {
                        let epoch = primary_id.epoch().next();
                        index
                            .vector_timestamps
                            .update(primary_id, ETValue::None(epoch, *timestamp))?;
                        if vector_already_exists {
                            operations.push(Operation::RemoveVector {
                                primary_id,
                                partition_id,
                            });
                            if index.data.remove_row(primary_id) {
                                operations.push(Operation::RemovePartition { partition_id });
                            }
                        }
                    }
                    Ok(())
                })?;
                Ok(operations)
            }

            Entry::Vacant(entry) => {
                if let Some(vector) = &vector {
                    let primary_id = self.free_primary_ids.take_id()?;
                    entry.insert(primary_id);
                    self.primary_keys
                        .get_mut(primary_id)
                        .ok_or_else(|| anyhow!("PrimaryId index out of primary keys bounds"))?
                        .replace(primary_key.clone());
                    self.indexes.iter_mut().try_for_each(
                        |(index_id, index)| -> anyhow::Result<()> {
                            let partition_id = match index.data {
                                IndexData::Global => PartitionId::global(*index_id),
                                IndexData::Local { .. } => {
                                    let Some(partition_key) = index
                                        .partition_key(&self.primary_key_columns, &primary_key)
                                    else {
                                        return Ok(());
                                    };
                                    index.add(primary_id, partition_key)?
                                }
                            };
                            index.vector_timestamps.update(
                                primary_id,
                                ETValue::Some(primary_id.epoch(), db_embedding.timestamp, ()),
                            )?;
                            operations.push(Operation::AddVector {
                                primary_id,
                                partition_id,
                                vector: vector.clone(),
                                is_update: false,
                            });
                            Ok(())
                        },
                    )?;
                } else {
                    warn!("Added row with no vector, skipping vector addition");
                }
                Ok(operations)
            }
        }
    }
}

/// A trait that defines the search operations for the table.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait TableSearch {
    #[allow(clippy::needless_lifetimes)] // for mockall
    fn partition_id<'a>(
        &self,
        index_key: &IndexKey,
        restrictions: Option<&'a [Restriction]>,
    ) -> Option<PartitionId>;

    fn primary_key(&self, partition_id: PartitionId, primary_id: PrimaryId) -> Option<PrimaryKey>;

    fn is_valid_for(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        restriction: &Restriction,
    ) -> bool;
}

impl TableSearch for Table {
    fn partition_id(
        &self,
        index_key: &IndexKey,
        restrictions: Option<&[Restriction]>,
    ) -> Option<PartitionId> {
        let index_id = self.index_ids.get(index_key)?;
        let index = self.indexes.get(index_id)?;
        match &index.data {
            IndexData::Global => Some(PartitionId::global(*index_id)),
            IndexData::Local {
                key_columns, map, ..
            } => restrictions
                .and_then(|restrictions| {
                    partition_key_from_restrictions(key_columns.as_ref(), restrictions)
                })
                .and_then(|partition_key| map.get(&partition_key).copied()),
        }
    }

    fn primary_key(&self, partition_id: PartitionId, primary_id: PrimaryId) -> Option<PrimaryKey> {
        if !self.is_valid_primary_id(partition_id, primary_id) {
            return None;
        }
        self.primary_keys.get(primary_id).cloned().flatten()
    }

    fn is_valid_for(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        restriction: &Restriction,
    ) -> bool {
        if !self.is_valid_primary_id(partition_id, primary_id) {
            return false;
        }
        match restriction {
            Restriction::Eq { lhs, rhs } => {
                cql_cmp_single(&self.columns, &self.primary_keys, primary_id, lhs, rhs)
                    .is_some_and(|ord| ord.is_eq())
            }

            Restriction::In { lhs, rhs } => self
                .columns
                .get(lhs)
                .and_then(|column| column.get(primary_id, &self.primary_keys))
                .and_then(|value| {
                    rhs.iter()
                        .filter_map(|rhs| cql_cmp(&value, rhs))
                        .any(|ord| ord.is_eq())
                        .then_some(())
                })
                .is_some(),

            Restriction::Lt { lhs, rhs } => {
                cql_cmp_single(&self.columns, &self.primary_keys, primary_id, lhs, rhs)
                    .is_some_and(|ord| ord.is_lt())
            }

            Restriction::Lte { lhs, rhs } => {
                cql_cmp_single(&self.columns, &self.primary_keys, primary_id, lhs, rhs)
                    .is_some_and(|ord| ord.is_le())
            }

            Restriction::Gt { lhs, rhs } => {
                cql_cmp_single(&self.columns, &self.primary_keys, primary_id, lhs, rhs)
                    .is_some_and(|ord| ord.is_gt())
            }

            Restriction::Gte { lhs, rhs } => {
                cql_cmp_single(&self.columns, &self.primary_keys, primary_id, lhs, rhs)
                    .is_some_and(|ord| ord.is_ge())
            }

            Restriction::EqTuple { lhs, rhs } => lhs.iter().zip(rhs.iter()).all(|(lhs, rhs)| {
                cql_cmp_single(&self.columns, &self.primary_keys, primary_id, lhs, rhs)
                    .is_some_and(|ord| ord.is_eq())
            }),

            Restriction::InTuple { lhs, rhs } => {
                let lhs = lhs
                    .iter()
                    .map(|lhs| {
                        self.columns
                            .get(lhs)
                            .and_then(|column| column.get(primary_id, &self.primary_keys))
                    })
                    .collect::<Option<Vec<_>>>();
                lhs.and_then(|lhs| {
                    rhs.iter()
                        .any(|rhs| {
                            lhs.iter()
                                .zip(rhs.iter())
                                .all(|(lhs, rhs)| cql_cmp(lhs, rhs).is_some_and(|ord| ord.is_eq()))
                        })
                        .then_some(())
                })
                .is_some()
            }

            Restriction::LtTuple { lhs, rhs } => {
                cql_cmp_tuple(&self.columns, &self.primary_keys, primary_id, lhs, rhs)
                    .is_some_and(|ord| ord.is_lt())
            }

            Restriction::LteTuple { lhs, rhs } => {
                cql_cmp_tuple(&self.columns, &self.primary_keys, primary_id, lhs, rhs)
                    .is_some_and(|ord| ord.is_le())
            }

            Restriction::GtTuple { lhs, rhs } => {
                cql_cmp_tuple(&self.columns, &self.primary_keys, primary_id, lhs, rhs)
                    .is_some_and(|ord| ord.is_gt())
            }

            Restriction::GteTuple { lhs, rhs } => {
                cql_cmp_tuple(&self.columns, &self.primary_keys, primary_id, lhs, rhs)
                    .is_some_and(|ord| ord.is_ge())
            }
        }
    }
}

/// Construct a partition key from the given restrictions.
fn partition_key_from_restrictions(
    key_columns: &[ColumnName],
    restrictions: &[Restriction],
) -> Option<PartitionKey> {
    key_columns
        .iter()
        .map(|column| {
            restrictions
                .iter()
                .find_map(|restriction| match restriction {
                    Restriction::Eq { lhs, rhs } if lhs == column => Some(rhs.clone()),
                    _ => None,
                })
        })
        .collect::<Option<PartitionKey>>()
}

/// Compare two CqlValues, returning an Ordering if they are comparable.
/// Only Numeric, Text, Date, Time, and Timestamp types support comparison operators.
fn cql_cmp(lhs: &CqlValue, rhs: &CqlValue) -> Option<Ordering> {
    match (lhs, rhs) {
        // Numeric types
        (CqlValue::TinyInt(a), CqlValue::TinyInt(b)) => Some(a.cmp(b)),
        (CqlValue::SmallInt(a), CqlValue::SmallInt(b)) => Some(a.cmp(b)),
        (CqlValue::Int(a), CqlValue::Int(b)) => Some(a.cmp(b)),
        (CqlValue::BigInt(a), CqlValue::BigInt(b)) => Some(a.cmp(b)),
        (CqlValue::Float(a), CqlValue::Float(b)) => a.partial_cmp(b),
        (CqlValue::Double(a), CqlValue::Double(b)) => a.partial_cmp(b),
        (CqlValue::Counter(a), CqlValue::Counter(b)) => Some(a.0.cmp(&b.0)),
        // Text types
        (CqlValue::Text(a), CqlValue::Text(b)) => Some(a.cmp(b)),
        (CqlValue::Ascii(a), CqlValue::Ascii(b)) => Some(a.cmp(b)),
        // Date and Time types (access inner values directly)
        (CqlValue::Date(a), CqlValue::Date(b)) => Some(a.0.cmp(&b.0)),
        (CqlValue::Time(a), CqlValue::Time(b)) => Some(a.0.cmp(&b.0)),
        (CqlValue::Timestamp(a), CqlValue::Timestamp(b)) => Some(a.0.cmp(&b.0)),
        // Unsupported or mismatched types
        _ => None,
    }
}

/// Compare a column value for a given primary_id with a CqlValue.
fn cql_cmp_single(
    columns: &BTreeMap<ColumnName, Column>,
    primary_keys: &ColumnVec<PrimaryId, Option<PrimaryKey>>,
    primary_id: PrimaryId,
    lhs: &ColumnName,
    rhs: &CqlValue,
) -> Option<Ordering> {
    columns
        .get(lhs)
        .and_then(|column| column.get(primary_id, primary_keys))
        .and_then(|value| cql_cmp(&value, rhs))
}

/// Returns the ordering of the first non-equal pair, or None if all pairs are equal.
fn cql_cmp_tuple(
    columns: &BTreeMap<ColumnName, Column>,
    primary_keys: &ColumnVec<PrimaryId, Option<PrimaryKey>>,
    primary_id: PrimaryId,
    lhs: &[ColumnName],
    rhs: &[CqlValue],
) -> Option<Ordering> {
    lhs.iter()
        .zip(rhs.iter())
        .map(|(lhs, rhs)| cql_cmp_single(columns, primary_keys, primary_id, lhs, rhs))
        .find(|ord| ord.is_none() || ord.is_some_and(|ord| !ord.is_eq()))
        .or(Some(Some(Ordering::Equal)))
        .flatten()
}

#[derive(Clone, Debug)]
pub(crate) enum Operation {
    AddVector {
        primary_id: PrimaryId,
        partition_id: PartitionId,
        vector: Vector,
        is_update: bool,
    },
    RemoveBeforeAddVector {
        primary_id: PrimaryId,
        partition_id: PartitionId,
    },
    RemoveVector {
        primary_id: PrimaryId,
        partition_id: PartitionId,
    },
    RemovePartition {
        partition_id: PartitionId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow() {
        for partition_key_columns in [None, Some(Arc::new(vec!["pk".into()]))] {
            let index_key = IndexKey::new(&"ks".into(), &"idx".into());
            let mut table = Table::new(
                index_key.clone(),
                Arc::new(vec!["pk".into(), "ck".into()]),
                partition_key_columns.clone(),
                &[],
                Arc::new(
                    [
                        ("pk".into(), NativeType::Int),
                        ("ck".into(), NativeType::Int),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .unwrap();

            // insert first vector
            let operations = table
                .add(
                    &index_key,
                    DbEmbedding {
                        primary_key: [CqlValue::Int(1), CqlValue::Int(1)].into(),
                        embedding: Some(vec![0.1, 0.2, 0.3].into()),
                        timestamp: Timestamp::from_unix_timestamp(100),
                    },
                )
                .unwrap();
            assert_eq!(operations.len(), 1);
            let (primary_id11, partition_id11) = match operations.first().unwrap() {
                Operation::AddVector {
                    primary_id,
                    partition_id,
                    vector,
                    is_update: false,
                } => {
                    assert_eq!(vector, &vec![0.1, 0.2, 0.3].into());
                    (*primary_id, *partition_id)
                }
                _ => panic!("Expected AddVector operation"),
            };

            // insert second vector
            let operations = table
                .add(
                    &index_key,
                    DbEmbedding {
                        primary_key: [CqlValue::Int(1), CqlValue::Int(2)].into(),
                        embedding: Some(vec![0.2, 0.2, 0.3].into()),
                        timestamp: Timestamp::from_unix_timestamp(100),
                    },
                )
                .unwrap();
            assert_eq!(operations.len(), 1);
            let (primary_id21, partition_id21) = match operations.first().unwrap() {
                Operation::AddVector {
                    primary_id,
                    partition_id,
                    vector,
                    is_update: false,
                } => {
                    assert_eq!(vector, &vec![0.2, 0.2, 0.3].into());
                    (*primary_id, *partition_id)
                }
                _ => panic!("Expected AddVector operation"),
            };
            assert_ne!(primary_id11, primary_id21);
            assert_eq!(partition_id11, partition_id21);

            // insert third vector
            let operations = table
                .add(
                    &index_key,
                    DbEmbedding {
                        primary_key: [CqlValue::Int(1), CqlValue::Int(3)].into(),
                        embedding: Some(vec![0.3, 0.2, 0.3].into()),
                        timestamp: Timestamp::from_unix_timestamp(100),
                    },
                )
                .unwrap();
            assert_eq!(operations.len(), 1);
            let (primary_id31, partition_id31) = match operations.first().unwrap() {
                Operation::AddVector {
                    primary_id,
                    partition_id,
                    vector,
                    is_update: false,
                } => {
                    assert_eq!(vector, &vec![0.3, 0.2, 0.3].into());
                    (*primary_id, *partition_id)
                }
                _ => panic!("Expected AddVector operation"),
            };
            assert_ne!(primary_id11, primary_id31);
            assert_ne!(primary_id21, primary_id31);
            assert_eq!(partition_id11, partition_id31);
            assert_eq!(partition_id21, partition_id31);

            assert_eq!(
                table.primary_key(partition_id11, primary_id11).unwrap(),
                [CqlValue::Int(1), CqlValue::Int(1)].into()
            );
            assert_eq!(
                table.primary_key(partition_id11, primary_id21).unwrap(),
                [CqlValue::Int(1), CqlValue::Int(2)].into()
            );
            assert_eq!(
                table.primary_key(partition_id11, primary_id31).unwrap(),
                [CqlValue::Int(1), CqlValue::Int(3)].into()
            );

            assert!(table.is_valid_for(
                partition_id11,
                primary_id11,
                &Restriction::Eq {
                    lhs: "ck".into(),
                    rhs: CqlValue::Int(1)
                }
            ));
            assert!(table.is_valid_for(
                partition_id21,
                primary_id21,
                &Restriction::Eq {
                    lhs: "ck".into(),
                    rhs: CqlValue::Int(2)
                }
            ));
            assert!(table.is_valid_for(
                partition_id31,
                primary_id31,
                &Restriction::Eq {
                    lhs: "ck".into(),
                    rhs: CqlValue::Int(3)
                }
            ));

            // insert second vector with older timestamp - should not update the vector
            let operations = table
                .add(
                    &index_key,
                    DbEmbedding {
                        primary_key: [CqlValue::Int(1), CqlValue::Int(2)].into(),
                        embedding: Some(vec![0.2, 0.2, 0.3].into()),
                        timestamp: Timestamp::from_unix_timestamp(50),
                    },
                )
                .unwrap();
            assert_eq!(operations.len(), 0);

            // insert second vector with newer timestamp - should update the vector
            let operations = table
                .add(
                    &index_key,
                    DbEmbedding {
                        primary_key: [CqlValue::Int(1), CqlValue::Int(2)].into(),
                        embedding: Some(vec![0.5, 0.5, 0.3].into()),
                        timestamp: Timestamp::from_unix_timestamp(150),
                    },
                )
                .unwrap();
            assert_eq!(operations.len(), 2);
            let (primary_id22, partition_id22) = match operations.first().unwrap() {
                Operation::RemoveBeforeAddVector {
                    primary_id,
                    partition_id,
                } => (*primary_id, *partition_id),
                _ => panic!("Expected RemoveBeforeAddVector operation"),
            };
            assert_eq!(primary_id22, primary_id21);
            assert_eq!(partition_id22, partition_id21);
            let (primary_id22, partition_id22) = match operations.get(1).unwrap() {
                Operation::AddVector {
                    primary_id,
                    partition_id,
                    vector,
                    is_update: true,
                } => {
                    assert_eq!(vector, &vec![0.5, 0.5, 0.3].into());
                    (*primary_id, *partition_id)
                }
                _ => panic!("Expected AddVector operation"),
            };
            assert_ne!(primary_id22, primary_id21);
            assert_eq!(partition_id22, partition_id21);

            assert!(table.primary_key(partition_id21, primary_id21).is_none());
            assert_eq!(
                table.primary_key(partition_id22, primary_id22).unwrap(),
                [CqlValue::Int(1), CqlValue::Int(2)].into()
            );

            // remove first vector
            let operations = table
                .add(
                    &index_key,
                    DbEmbedding {
                        primary_key: [CqlValue::Int(1), CqlValue::Int(1)].into(),
                        embedding: None,
                        timestamp: Timestamp::from_unix_timestamp(200),
                    },
                )
                .unwrap();
            assert_eq!(operations.len(), 1);
            let (primary_id13, partition_id13) = match operations.first().unwrap() {
                Operation::RemoveVector {
                    primary_id,
                    partition_id,
                } => (*primary_id, *partition_id),
                _ => panic!("Expected RemoveVector operation"),
            };
            assert_eq!(primary_id13, primary_id11);
            assert_eq!(partition_id13, partition_id11);
            assert!(table.primary_key(partition_id13, primary_id13).is_none());

            // remove second vector
            let operations = table
                .add(
                    &index_key,
                    DbEmbedding {
                        primary_key: [CqlValue::Int(1), CqlValue::Int(2)].into(),
                        embedding: None,
                        timestamp: Timestamp::from_unix_timestamp(200),
                    },
                )
                .unwrap();
            assert_eq!(operations.len(), 1);
            let (primary_id23, partition_id23) = match operations.first().unwrap() {
                Operation::RemoveVector {
                    primary_id,
                    partition_id,
                } => (*primary_id, *partition_id),
                _ => panic!("Expected RemoveVector operation"),
            };
            assert_eq!(primary_id23, primary_id22);
            assert_eq!(partition_id23, partition_id22);
            assert!(table.primary_key(partition_id23, primary_id23).is_none());

            // remove third vector
            let operations = table
                .add(
                    &index_key,
                    DbEmbedding {
                        primary_key: [CqlValue::Int(1), CqlValue::Int(3)].into(),
                        embedding: None,
                        timestamp: Timestamp::from_unix_timestamp(200),
                    },
                )
                .unwrap();
            if partition_key_columns.is_none() {
                assert_eq!(operations.len(), 1);
            } else {
                assert_eq!(operations.len(), 2);
            }
            let (primary_id33, partition_id33) = match operations.first().unwrap() {
                Operation::RemoveVector {
                    primary_id,
                    partition_id,
                } => (*primary_id, *partition_id),
                _ => panic!("Expected RemoveVector operation"),
            };
            assert_eq!(primary_id33, primary_id31);
            assert_eq!(partition_id33, partition_id31);
            assert!(table.primary_key(partition_id33, primary_id33).is_none());
            if partition_key_columns.is_some() {
                let partition_id33 = match operations.get(1).unwrap() {
                    Operation::RemovePartition { partition_id } => *partition_id,
                    _ => panic!("Expected RemovePartition operation"),
                };
                assert_eq!(partition_id33, partition_id31);
            }
        }
    }

    #[test]
    fn cql_cmp_integers() {
        assert_eq!(
            cql_cmp(&CqlValue::Int(1), &CqlValue::Int(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cql_cmp(&CqlValue::Int(2), &CqlValue::Int(2)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            cql_cmp(&CqlValue::Int(3), &CqlValue::Int(2)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn cql_cmp_bigints() {
        assert_eq!(
            cql_cmp(&CqlValue::BigInt(100), &CqlValue::BigInt(200)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cql_cmp(&CqlValue::BigInt(-50), &CqlValue::BigInt(-50)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn cql_cmp_floats() {
        assert_eq!(
            cql_cmp(&CqlValue::Float(1.0), &CqlValue::Float(2.0)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cql_cmp(&CqlValue::Float(2.5), &CqlValue::Float(2.5)),
            Some(Ordering::Equal)
        );
        // NaN comparison returns None
        assert_eq!(
            cql_cmp(&CqlValue::Float(f32::NAN), &CqlValue::Float(1.0)),
            None
        );
    }

    #[test]
    fn cql_cmp_doubles() {
        assert_eq!(
            cql_cmp(&CqlValue::Double(1.0), &CqlValue::Double(2.0)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cql_cmp(&CqlValue::Double(f64::NAN), &CqlValue::Double(1.0)),
            None
        );
    }

    #[test]
    fn cql_cmp_text() {
        assert_eq!(
            cql_cmp(
                &CqlValue::Text("apple".to_string()),
                &CqlValue::Text("banana".to_string())
            ),
            Some(Ordering::Less)
        );
        assert_eq!(
            cql_cmp(
                &CqlValue::Text("same".to_string()),
                &CqlValue::Text("same".to_string())
            ),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn cql_cmp_ascii() {
        assert_eq!(
            cql_cmp(
                &CqlValue::Ascii("aaa".to_string()),
                &CqlValue::Ascii("bbb".to_string())
            ),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn cql_cmp_smallint_and_tinyint() {
        assert_eq!(
            cql_cmp(&CqlValue::SmallInt(10), &CqlValue::SmallInt(20)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cql_cmp(&CqlValue::TinyInt(5), &CqlValue::TinyInt(3)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn cql_cmp_mismatched_types_return_none() {
        assert_eq!(cql_cmp(&CqlValue::Int(1), &CqlValue::BigInt(1)), None);
        assert_eq!(
            cql_cmp(&CqlValue::Int(1), &CqlValue::Text("1".to_string())),
            None
        );
        assert_eq!(cql_cmp(&CqlValue::Float(1.0), &CqlValue::Double(1.0)), None);
    }

    #[test]
    fn cql_cmp_unsupported_types_return_none() {
        assert_eq!(
            cql_cmp(
                &CqlValue::Blob(vec![1, 2, 3]),
                &CqlValue::Blob(vec![1, 2, 3])
            ),
            None
        );
        assert_eq!(
            cql_cmp(&CqlValue::Boolean(true), &CqlValue::Boolean(false)),
            None
        );
    }

    #[test]
    fn cql_cmp_single_ints() {
        let columns = [("col".into(), Column::PrimaryKey(0.into()))]
            .into_iter()
            .collect();
        let mut primary_keys = ColumnVec::new();
        primary_keys.resize_with(1, || None);
        primary_keys
            .get_mut(PrimaryId::from(0))
            .unwrap()
            .replace([CqlValue::Int(10)].into());

        assert_eq!(
            cql_cmp_single(
                &columns,
                &primary_keys,
                0.into(),
                &"col".into(),
                &CqlValue::Int(15),
            ),
            Some(Ordering::Less)
        );
        assert_eq!(
            cql_cmp_single(
                &columns,
                &primary_keys,
                0.into(),
                &"col".into(),
                &CqlValue::Int(10),
            ),
            Some(Ordering::Equal)
        );
        assert_eq!(
            cql_cmp_single(
                &columns,
                &primary_keys,
                0.into(),
                &"col".into(),
                &CqlValue::Int(5),
            ),
            Some(Ordering::Greater)
        );
        assert_eq!(
            cql_cmp_single(
                &columns,
                &primary_keys,
                1.into(),
                &"col".into(),
                &CqlValue::Int(5),
            ),
            None
        );
        assert_eq!(
            cql_cmp_single(
                &columns,
                &primary_keys,
                0.into(),
                &"col1".into(),
                &CqlValue::Int(5),
            ),
            None
        );
    }

    #[test]
    fn cql_cmp_tuple_ints() {
        let columns = [
            ("col0".into(), Column::PrimaryKey(0.into())),
            ("col1".into(), Column::PrimaryKey(1.into())),
        ]
        .into_iter()
        .collect();
        let mut primary_keys = ColumnVec::new();
        primary_keys.resize_with(1, || None);
        primary_keys
            .get_mut(PrimaryId::from(0))
            .unwrap()
            .replace([CqlValue::Int(10), CqlValue::Int(20)].into());

        assert_eq!(
            cql_cmp_tuple(
                &columns,
                &primary_keys,
                0.into(),
                &["col0".into(), "col1".into()],
                &[CqlValue::Int(10), CqlValue::Int(25)],
            ),
            Some(Ordering::Less)
        );
        assert_eq!(
            cql_cmp_tuple(
                &columns,
                &primary_keys,
                0.into(),
                &["col0".into(), "col1".into()],
                &[CqlValue::Int(10), CqlValue::Int(15)],
            ),
            Some(Ordering::Greater)
        );
        assert_eq!(
            cql_cmp_tuple(
                &columns,
                &primary_keys,
                0.into(),
                &["col0".into(), "col1".into()],
                &[CqlValue::Int(10), CqlValue::Int(20)],
            ),
            Some(Ordering::Equal)
        );
        assert_eq!(
            cql_cmp_tuple(
                &columns,
                &primary_keys,
                1.into(),
                &["col0".into(), "col1".into()],
                &[CqlValue::Int(10), CqlValue::Int(15)],
            ),
            None
        );
        assert_eq!(
            cql_cmp_tuple(
                &columns,
                &primary_keys,
                0.into(),
                &["col0".into(), "col2".into()],
                &[CqlValue::Int(10), CqlValue::Int(15)],
            ),
            None
        );
    }
}
