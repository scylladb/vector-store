/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::frame;
use async_backtrace::framed;
use hickory_server::proto::rr::DNSClass;
use hickory_server::proto::rr::LowerName;
use hickory_server::proto::rr::Name;
use hickory_server::proto::rr::RData;
use hickory_server::proto::rr::Record;
use hickory_server::proto::rr::RecordType;
use hickory_server::proto::rr::RrKey;
use hickory_server::proto::rr::rdata::a::A;
use hickory_server::proto::rr::rdata::soa::SOA;
use hickory_server::server::Server;
use hickory_server::store::in_memory::InMemoryZoneHandler;
use hickory_server::zone_handler::AxfrPolicy;
use hickory_server::zone_handler::Catalog;
use hickory_server::zone_handler::ZoneType;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub enum Dns {
    Version { tx: oneshot::Sender<String> },
    Domain { tx: oneshot::Sender<String> },
    Remove { name: String },
    Upsert { name: String, ip: Ipv4Addr },
}

pub trait DnsExt {
    /// Returns the version of the DNS server.
    fn version(&self) -> impl Future<Output = String>;

    /// Returns the domain name of the DNS server.
    fn domain(&self) -> impl Future<Output = String>;

    /// Remove an A DNS record with the given name.
    fn remove(&self, name: String) -> impl Future<Output = ()>;

    /// Upserts an A DNS record with the given name and IP address.
    fn upsert(&self, name: String, ip: Ipv4Addr) -> impl Future<Output = ()>;
}

impl DnsExt for mpsc::Sender<Dns> {
    #[framed]
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(Dns::Version { tx })
            .await
            .expect("DnsExt::version: internal actor should receive request");
        rx.await
            .expect("DnsExt::version: internal actor should send response")
    }

    #[framed]
    async fn domain(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(Dns::Domain { tx })
            .await
            .expect("DnsExt::domain: internal actor should receive request");
        rx.await
            .expect("DnsExt::domain: internal actor should send response")
    }

    #[framed]
    async fn remove(&self, name: String) {
        self.send(Dns::Remove { name })
            .await
            .expect("DnsExt::remove: internal actor should receive request");
    }

    #[framed]
    async fn upsert(&self, name: String, ip: Ipv4Addr) {
        self.send(Dns::Upsert { name, ip })
            .await
            .expect("DnsExt::upsert: internal actor should receive request");
    }
}
#[framed]
/// Starts the DNS server on the given IP address.
pub async fn new(ip: Ipv4Addr) -> mpsc::Sender<Dns> {
    assert!(ip.is_loopback(), "DNS server should listen on a localhost");

    let (tx, mut rx) = mpsc::channel(10);

    let mut state = State::new().await;

    let socket = UdpSocket::bind((ip, 53))
        .await
        .expect("dns: failed to bind UDP socket");

    let mut catalog = Catalog::new();
    catalog.upsert(
        LowerName::from_str(ZONE).unwrap(),
        vec![state.authority.clone()],
    );
    let mut server = Server::new(catalog);
    server.register_socket(socket);

    tokio::spawn(
        frame!(async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &mut state).await;
            }

            server
                .shutdown_gracefully()
                .await
                .expect("stop: failed to shutdown server gracefully");

            debug!("stopped");
        })
        .instrument(debug_span!("dns")),
    );

    tx
}

struct State {
    version: String,
    authority: Arc<InMemoryZoneHandler>,
    serial: u32,
}

const ZONE: &str = "validator.test.";
const TTL: u32 = 60;

impl State {
    #[framed]
    async fn new() -> Self {
        let version = format!("hickory-server-{}", hickory_server::version());

        let authority = Arc::new(InMemoryZoneHandler::empty(
            Name::from_str(ZONE).unwrap(),
            ZoneType::Primary,
            AxfrPolicy::Deny,
        ));
        let mut soa = Record::from_rdata(
            Name::from_str(ZONE).unwrap(),
            TTL,
            RData::SOA(SOA::new(
                Name::from_str(ZONE).unwrap(),
                Name::new(),
                0,
                0,
                0,
                0,
                0,
            )),
        );
        soa.dns_class = DNSClass::IN;
        authority.upsert(soa, 0).await;

        Self {
            version,
            authority,
            serial: 0,
        }
    }
}

#[framed]
async fn process(msg: Dns, state: &mut State) {
    match msg {
        Dns::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process Dns::Version: failed to send a response");
        }

        Dns::Domain { tx } => {
            tx.send(ZONE[..ZONE.len() - 1].to_string())
                .expect("process Dns::Domain: failed to send a response");
        }

        Dns::Remove { name } => {
            remove(name, state).await;
        }

        Dns::Upsert { name, ip } => {
            upsert(name, ip, state).await;
        }
    }
}

#[framed]
async fn remove(name: String, state: &mut State) {
    state.authority.records_mut().await.remove(&RrKey::new(
        LowerName::from_str(&format!("{name}.{ZONE}")).expect("remove: failed to parse name"),
        RecordType::A,
    ));
}

#[framed]
async fn upsert(name: String, ip: Ipv4Addr, state: &mut State) {
    let name = Name::from_str(&format!("{name}.{ZONE}")).expect("upsert: failed to parse name");

    let serial = state.serial;
    state.serial += 1;

    let octets = ip.octets();
    let mut record = Record::from_rdata(
        name,
        TTL,
        RData::A(A::new(octets[0], octets[1], octets[2], octets[3])),
    );
    record.dns_class = DNSClass::IN;

    state.authority.upsert(record, serial).await;
}
