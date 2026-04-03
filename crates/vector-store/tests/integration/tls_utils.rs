/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use rcgen::CertificateParams;
use rcgen::KeyPair;
use std::io::Write;
use std::sync::Once;
use tempfile::NamedTempFile;

static INIT: Once = Once::new();

pub(crate) fn create_temp_file<C: AsRef<[u8]>>(content: C) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(content.as_ref()).unwrap();
    file
}

pub(crate) fn generate_server_cert(addr: &core::net::SocketAddr) -> (NamedTempFile, NamedTempFile) {
    let params = CertificateParams::new(vec![addr.ip().to_string()]).unwrap();
    let key_pair = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    let cert_file = create_temp_file(cert.pem().as_bytes());
    let key_file = create_temp_file(key_pair.serialize_pem().as_bytes());
    (cert_file, key_file)
}

pub(crate) fn read_cert(cert_file: &NamedTempFile) -> reqwest::Certificate {
    reqwest::Certificate::from_pem(&std::fs::read(cert_file).unwrap()).unwrap()
}

pub(crate) fn init() {
    INIT.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("rustls install_default: failed to install rustls crypto provider");
    });
    crate::enable_tracing();
}
