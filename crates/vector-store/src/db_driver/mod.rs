/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod scylla;

pub trait DbDriver: Clone + Send + Sync + 'static {}

pub fn new_scylla() -> impl DbDriver {
    scylla::new()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    pub(crate) struct UnimplementedDbDriver;

    impl DbDriver for UnimplementedDbDriver {
    }
}
