/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use std::net::Ipv4Addr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub enum Firewall {
    DropTraffic {
        ips: Vec<Ipv4Addr>,
        tx: oneshot::Sender<()>,
    },
    TurnOffRules {
        tx: oneshot::Sender<()>,
    },
}

pub trait FirewallExt {
    /// Drops traffic to the specified IP addresses.
    fn drop_traffic(&self, ips: Vec<Ipv4Addr>) -> impl Future<Output = ()>;

    /// Turn off all firewall rules.
    fn turn_off_rules(&self) -> impl Future<Output = ()>;
}

impl FirewallExt for mpsc::Sender<Firewall> {
    #[framed]
    async fn drop_traffic(&self, ips: Vec<Ipv4Addr>) {
        let (tx, rx) = oneshot::channel();
        self.send(Firewall::DropTraffic { ips, tx })
            .await
            .expect("FirewallExt::drop_traffic: internal actor should receive request");
        rx.await
            .expect("FirewallExt::drop_traffic: internal actor should send response")
    }

    #[framed]
    async fn turn_off_rules(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(Firewall::TurnOffRules { tx })
            .await
            .expect("FirewallExt::turn_off_rules: internal actor should receive request");
        rx.await
            .expect("FirewallExt::turn_off_rules: internal actor should send response")
    }
}
