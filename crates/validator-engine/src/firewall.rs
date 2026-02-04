/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::frame;
use async_backtrace::framed;
use neli::consts::nl::*;
use neli::consts::rtnl::*;
use neli::consts::socket::*;
use neli::nl::NlPayload;
use neli::nl::Nlmsghdr;
use neli::router::asynchronous::NlRouter;
use neli::rtnl::*;
use neli::types::RtBuffer;
use neli::utils::Groups;
use std::mem;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use tokio::sync::mpsc;
use tracing::Instrument;
use tracing::debug;
use tracing::error;
use tracing::error_span;
use tracing::info;
use vector_search_validator_tests::Firewall;

#[framed]
pub(crate) async fn new() -> mpsc::Sender<Firewall> {
    let (tx, mut rx) = mpsc::channel(10);

    tokio::spawn(
        frame!(async move {
            debug!("starting");

            let (socket, _) = NlRouter::connect(NlFamily::Route, None, Groups::empty())
                .await
                .unwrap();

            let mut disabled_ips = Vec::new();

            while let Some(msg) = rx.recv().await {
                process(msg, &socket, &mut disabled_ips).await;
            }

            debug!("finished");
        })
        .instrument(error_span!("firewall")),
    );

    tx
}

#[framed]
async fn process(msg: Firewall, socket: &NlRouter, disabled_ips: &mut Vec<Ipv4Addr>) {
    match msg {
        Firewall::DropTraffic { ips, tx } => {
            info!("Removing rules for: {disabled_ips:?}");
            turn_off_rules(socket, mem::take(disabled_ips)).await;
            *disabled_ips = ips;
            info!("Adding rules for: {disabled_ips:?}");
            drop_traffic(socket, disabled_ips).await;
            if let Err(err) = log_routes(socket).await {
                error!("Failed to list routes: {err}");
            }
            tx.send(())
                .expect("process Firewall::DropTraffic: failed to send a response");
        }

        Firewall::TurnOffRules { tx } => {
            info!("Removing rules for: {disabled_ips:?}");
            turn_off_rules(socket, mem::take(disabled_ips)).await;
            if let Err(err) = log_routes(socket).await {
                error!("Failed to list routes: {err}");
            }
            tx.send(())
                .expect("process Firewall::TurnOffRules: failed to send a response");
        }
    }
}

#[framed]
async fn drop_traffic(socket: &NlRouter, ips: &[Ipv4Addr]) {
    for ip in ips.iter() {
        let Err(err) = add_unreachable_route(socket, ip).await else {
            continue;
        };
        error!("Failed to add unreachable route for ip {ip}: {err}");
    }
}

#[framed]
async fn turn_off_rules(socket: &NlRouter, ips: Vec<Ipv4Addr>) {
    for ip in ips.into_iter() {
        let Err(err) = remove_unreachable_route(socket, ip).await else {
            continue;
        };
        error!("Failed to remove unreachable route for ip {ip}: {err}");
    }
}

async fn add_unreachable_route(socket: &NlRouter, ip: &Ipv4Addr) -> anyhow::Result<()> {
    let mut attrs = RtBuffer::new();
    attrs.push(
        RtattrBuilder::default()
            .rta_type(Rta::Dst)
            .rta_payload(ip.octets())
            .build()?,
    );
    let rtmsg = RtmsgBuilder::default()
        .rtm_family(RtAddrFamily::Inet)
        .rtm_dst_len(32)
        .rtm_src_len(0)
        .rtm_tos(0)
        .rtm_table(RtTable::Main)
        .rtm_protocol(Rtprot::Unspec)
        .rtm_scope(RtScope::Universe)
        .rtm_type(Rtn::Blackhole)
        .rtattrs(attrs)
        .build()?;
    socket
        .send::<Rtm, Rtmsg, NlTypeWrapper, Rtmsg>(
            Rtm::Newroute,
            NlmF::REQUEST | NlmF::CREATE | NlmF::REPLACE,
            NlPayload::Payload(rtmsg),
        )
        .await?;
    Ok(())
}

async fn remove_unreachable_route(socket: &NlRouter, ip: Ipv4Addr) -> anyhow::Result<()> {
    let mut attrs = RtBuffer::new();
    attrs.push(
        RtattrBuilder::default()
            .rta_type(Rta::Dst)
            .rta_payload(ip.octets())
            .build()?,
    );
    let rtmsg = RtmsgBuilder::default()
        .rtm_family(RtAddrFamily::Inet)
        .rtm_dst_len(32)
        .rtm_src_len(0)
        .rtm_tos(0)
        .rtm_table(RtTable::Main)
        .rtm_protocol(Rtprot::Unspec)
        .rtm_scope(RtScope::Universe)
        .rtm_type(Rtn::Blackhole)
        .rtattrs(attrs)
        .build()?;
    socket
        .send::<Rtm, Rtmsg, NlTypeWrapper, Rtmsg>(
            Rtm::Delroute,
            NlmF::REQUEST,
            NlPayload::Payload(rtmsg),
        )
        .await?;
    Ok(())
}

async fn log_routes(socket: &NlRouter) -> anyhow::Result<()> {
    let rtmsg = RtmsgBuilder::default()
        .rtm_family(RtAddrFamily::Inet)
        .rtm_dst_len(0)
        .rtm_src_len(0)
        .rtm_tos(0)
        .rtm_table(RtTable::Unspec)
        .rtm_protocol(Rtprot::Unspec)
        .rtm_scope(RtScope::Universe)
        .rtm_type(Rtn::Unspec)
        .build()?;
    let mut recv = socket
        .send::<Rtm, Rtmsg, NlTypeWrapper, Rtmsg>(
            Rtm::Getroute,
            NlmF::DUMP,
            NlPayload::Payload(rtmsg),
        )
        .await?;

    while let Some(rtm_result) = recv.next().await {
        let rtm = rtm_result?;
        if let NlTypeWrapper::Rtm(_) = rtm.nl_type() {
            parse_route_table(rtm)?;
        }
    }

    Ok(())
}

fn parse_route_table(rtm: Nlmsghdr<NlTypeWrapper, Rtmsg>) -> anyhow::Result<()> {
    if let Some(payload) = rtm.get_payload() {
        let mut dst = None;

        for attr in payload.rtattrs().iter() {
            fn to_addr(b: &[u8]) -> Option<IpAddr> {
                if let Ok(tup) = <&[u8; 4]>::try_from(b) {
                    Some(IpAddr::from(*tup))
                } else if let Ok(tup) = <&[u8; 16]>::try_from(b) {
                    Some(IpAddr::from(*tup))
                } else {
                    None
                }
            }

            if attr.rta_type() == &Rta::Dst {
                dst = to_addr(attr.rta_payload().as_ref())
            }
        }

        let dst = if let Some(dst) = dst {
            format!("{}/{} ", dst, payload.rtm_dst_len())
        } else {
            "default".to_string()
        };

        info!(
            "active route for {:?}: {dst}: {:?}",
            payload.rtm_table(),
            payload.rtm_type()
        );
    }
    Ok(())
}
