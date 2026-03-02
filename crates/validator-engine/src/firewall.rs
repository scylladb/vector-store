/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::borrow::Cow;
use std::net::Ipv4Addr;

use async_backtrace::frame;
use async_backtrace::framed;
use nftables::batch::Batch;
use nftables::expr::Expression;
use nftables::expr::NamedExpression;
use nftables::expr::Payload;
use nftables::expr::PayloadField;
use nftables::helper;
use nftables::schema::Chain;
use nftables::schema::NfListObject;
use nftables::schema::Rule;
use nftables::schema::Table;
use nftables::stmt::Match;
use nftables::stmt::Operator;
use nftables::stmt::Statement;
use nftables::types::NfChainPolicy;
use nftables::types::NfChainType;
use nftables::types::NfFamily;
use nftables::types::NfHook;
use tokio::sync::mpsc;
use tracing::Instrument;
use tracing::debug;
use tracing::error;
use tracing::error_span;
use tracing::info;
use vector_search_validator_tests::Firewall;

const TABLE_NAME: &str = "validator_firewall";
const OUTPUT_CHAIN: &str = "output";
const INPUT_CHAIN: &str = "input";

#[framed]
pub(crate) async fn new() -> mpsc::Sender<Firewall> {
    let (tx, mut rx) = mpsc::channel(10);

    tokio::spawn(
        frame!(async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg).await;
            }

            debug!("finished");
        })
        .instrument(error_span!("firewall")),
    );

    tx
}

#[framed]
async fn process(msg: Firewall) {
    match msg {
        Firewall::DropTraffic { ips, tx } => {
            info!("Removing old rules (deleting table if exists)");
            // Silently ignore errors when deleting a non-existent table.
            let _ = delete_table();
            info!("Adding DROP rules for: {ips:?}");
            if let Err(err) = drop_traffic(&ips) {
                error!("Failed to add DROP rules: {err}");
            }
            log_rules();
            tx.send(())
                .expect("process Firewall::DropTraffic: failed to send a response");
        }

        Firewall::TurnOffRules { tx } => {
            info!("Removing all rules (deleting table)");
            if let Err(err) = delete_table() {
                error!("Failed to delete nftables table: {err}");
            }
            log_rules();
            tx.send(())
                .expect("process Firewall::TurnOffRules: failed to send a response");
        }
    }
}

fn drop_traffic(ips: &[Ipv4Addr]) -> anyhow::Result<()> {
    let mut batch = Batch::new();

    // Create the table.
    batch.add(NfListObject::Table(Table {
        family: NfFamily::IP,
        name: Cow::Borrowed(TABLE_NAME),
        handle: None,
    }));

    // Create OUTPUT chain (filter, hook output, priority 0, accept policy).
    batch.add(NfListObject::Chain(Chain {
        family: NfFamily::IP,
        table: Cow::Borrowed(TABLE_NAME),
        name: Cow::Borrowed(OUTPUT_CHAIN),
        _type: Some(NfChainType::Filter),
        hook: Some(NfHook::Output),
        prio: Some(0),
        policy: Some(NfChainPolicy::Accept),
        ..Default::default()
    }));

    // Create INPUT chain (filter, hook input, priority 0, accept policy).
    batch.add(NfListObject::Chain(Chain {
        family: NfFamily::IP,
        table: Cow::Borrowed(TABLE_NAME),
        name: Cow::Borrowed(INPUT_CHAIN),
        _type: Some(NfChainType::Filter),
        hook: Some(NfHook::Input),
        prio: Some(0),
        policy: Some(NfChainPolicy::Accept),
        ..Default::default()
    }));

    // For each IP, add DROP rules on both OUTPUT (daddr) and INPUT (saddr).
    for ip in ips {
        let ip_str = ip.to_string();

        // DROP outgoing traffic to this IP.
        batch.add(NfListObject::Rule(Rule {
            family: NfFamily::IP,
            table: Cow::Borrowed(TABLE_NAME),
            chain: Cow::Borrowed(OUTPUT_CHAIN),
            expr: Cow::Owned(vec![
                Statement::Match(Match {
                    left: Expression::Named(NamedExpression::Payload(Payload::PayloadField(
                        PayloadField {
                            protocol: Cow::Borrowed("ip"),
                            field: Cow::Borrowed("daddr"),
                        },
                    ))),
                    right: Expression::String(Cow::Owned(ip_str.clone())),
                    op: Operator::EQ,
                }),
                Statement::Drop(None),
            ]),
            ..Default::default()
        }));

        // DROP incoming traffic from this IP.
        batch.add(NfListObject::Rule(Rule {
            family: NfFamily::IP,
            table: Cow::Borrowed(TABLE_NAME),
            chain: Cow::Borrowed(INPUT_CHAIN),
            expr: Cow::Owned(vec![
                Statement::Match(Match {
                    left: Expression::Named(NamedExpression::Payload(Payload::PayloadField(
                        PayloadField {
                            protocol: Cow::Borrowed("ip"),
                            field: Cow::Borrowed("saddr"),
                        },
                    ))),
                    right: Expression::String(Cow::Owned(ip_str)),
                    op: Operator::EQ,
                }),
                Statement::Drop(None),
            ]),
            ..Default::default()
        }));
    }

    let nftables = batch.to_nftables();
    helper::apply_ruleset(&nftables).map_err(|e| anyhow::anyhow!("nftables apply failed: {e}"))?;
    Ok(())
}

fn delete_table() -> anyhow::Result<()> {
    let mut batch = Batch::new();
    batch.delete(NfListObject::Table(Table {
        family: NfFamily::IP,
        name: Cow::Borrowed(TABLE_NAME),
        handle: None,
    }));
    let nftables = batch.to_nftables();
    helper::apply_ruleset(&nftables)
        .map_err(|e| anyhow::anyhow!("nftables delete table failed: {e}"))?;
    Ok(())
}

fn log_rules() {
    match helper::get_current_ruleset() {
        Ok(ruleset) => {
            info!("Current nftables ruleset: {ruleset:?}");
        }
        Err(err) => {
            error!("Failed to get current nftables ruleset: {err}");
        }
    }
}
