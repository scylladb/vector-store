/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use std::num::NonZeroUsize;
use std::sync::Arc;

/// An extension trait for iterators that allows collecting into nonempty collections.
pub trait NonemptyIteratorExt: Iterator {
    /// Collects the iterator into a `NonemptyArc` if it is nonempty, otherwise returns `None`.
    fn collect_nonempty_arc(self) -> Option<NonemptyArc<Self::Item>>
    where
        Self: Sized,
    {
        NonemptyArc::new(self)
    }

    /// Collects the iterator into a `NonemptyBox` if it is nonempty, otherwise returns `None`.
    fn collect_nonempty_box(self) -> Option<NonemptyBox<Self::Item>>
    where
        Self: Sized,
    {
        NonemptyBox::new(self)
    }
}

impl<I: Iterator + ?Sized> NonemptyIteratorExt for I {}

macro_rules! nonempty_impl {
    ($ty:ident, $name:ident) => {
        /// A nonempty collection of values stored in a $ty.
        #[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
        pub struct $name<T>($ty<[T]>);

        impl<T> $name<T> {
            /// Creates a new nonempty collection from an iterator of values.
            pub fn new(values: impl IntoIterator<Item = impl Into<T>>) -> Option<Self> {
                let table: $ty<[T]> = values.into_iter().map(Into::into).collect();
                (!table.is_empty()).then_some(Self(table))
            }

            /// Returns the number of values in this key.
            pub fn len(&self) -> NonZeroUsize {
                NonZeroUsize::new(self.0.len()).expect("Nonempty collection should never be empty")
            }

            /// Returns a reference to the first value.
            pub fn first(&self) -> &T {
                &self.0[0]
            }

            /// Returns a reference to the specified index.
            pub fn get(&self, index: usize) -> Option<&T> {
                self.0.get(index)
            }

            /// Returns a slice of all values.
            pub fn as_slice(&self) -> &[T] {
                self.0.as_ref()
            }

            /// Iterate over all items in the collection.
            pub fn iter(&self) -> impl Iterator<Item = &T> {
                self.0.iter()
            }

            /// Returns true if the collection contains the specified value.
            pub fn contains(&self, value: &T) -> bool
            where
                T: PartialEq,
            {
                self.0.contains(value)
            }
        }
    };
}

nonempty_impl!(Arc, NonemptyArc);
nonempty_impl!(Box, NonemptyBox);

impl<T> IntoIterator for NonemptyBox<T> {
    type Item = T;
    type IntoIter = <Box<[T]> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> TryFrom<Box<[T]>> for NonemptyBox<T> {
    type Error = anyhow::Error;

    fn try_from(value: Box<[T]>) -> anyhow::Result<Self> {
        (!value.is_empty())
            .then_some(Self(value))
            .ok_or_else(|| anyhow::anyhow!("Cannot create NonemptyBox from empty Box"))
    }
}

impl<T> TryFrom<Arc<[T]>> for NonemptyArc<T> {
    type Error = anyhow::Error;

    fn try_from(value: Arc<[T]>) -> anyhow::Result<Self> {
        (!value.is_empty())
            .then_some(Self(value))
            .ok_or_else(|| anyhow::anyhow!("Cannot create NonemptyArc from empty Arc"))
    }
}

impl<T> NonemptyBox<T> {
    /// Consumes the NonemptyBox and returns the first value.
    pub fn take_first(self) -> T {
        self.0.into_iter().next().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;

    #[test]
    fn test_nonempty_arc() {
        let empty: Option<NonemptyArc<u32>> = iter::empty().collect_nonempty_arc();
        assert!(empty.is_none());
        let nonempty = iter::once(42).collect_nonempty_arc();
        assert!(nonempty.is_some());
        let nonempty = iter::once(42).chain(iter::once(43)).collect_nonempty_arc();
        assert!(nonempty.is_some());
    }

    #[test]
    fn test_nonempty_box() {
        let empty: Option<NonemptyBox<u32>> = iter::empty().collect_nonempty_box();
        assert!(empty.is_none());
        let nonempty = iter::once(42).collect_nonempty_box();
        assert!(nonempty.is_some());
        let nonempty = iter::once(42).chain(iter::once(43)).collect_nonempty_box();
        assert!(nonempty.is_some());
    }
}
