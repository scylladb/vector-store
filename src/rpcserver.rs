/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    crate::{
        actor::{ActorHandle, MessageStop},
        engine::{Engine, EngineExt},
        index::IndexExt,
        Distance, Embeddings, IndexId, Key, Limit, RpcServerAddr,
    },
    bytes::{Buf, BufMut},
    seastar_rpc::{ConnectionId, Server, ServerRpc},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{
            mpsc::{self, Sender},
            watch,
        },
    },
    tracing::error,
};

pub(crate) enum RpcServer {
    Stop,
}

impl MessageStop for RpcServer {
    fn message_stop() -> Self {
        RpcServer::Stop
    }
}

pub(crate) async fn new(
    addr: RpcServerAddr,
    engine: Sender<Engine>,
) -> anyhow::Result<(Sender<RpcServer>, ActorHandle)> {
    let listener = TcpListener::bind(addr.0).await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    let (tx, mut rx) = mpsc::channel(10);
    let task = tokio::spawn(async move {
        let mut next_id = ConnectionId::from(0);
        let (stop_tx, stop_rx) = watch::channel(false);
        while !rx.is_closed() {
            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    let id = next_id;
                    next_id = ConnectionId::from(next_id.as_ref() + 1);
                    tokio::spawn(handle_connection(engine.clone(), stream, id, stop_rx.clone()));
                }
                Some(msg) = rx.recv() => {
                    match msg {
                        RpcServer::Stop => rx.close(),
                    }
                }
            }
        }
        stop_tx.send(true).unwrap_or_default();
    });
    Ok((tx, task))
}

async fn handle_connection(
    engine: Sender<Engine>,
    mut stream: TcpStream,
    id: ConnectionId,
    mut stop_rx: watch::Receiver<bool>,
) {
    const BUF_SIZE: usize = 1024;
    let mut buf = [0; BUF_SIZE];

    let Some(server) = handle_connection_negotiations(&mut stream, id, &mut buf, &mut stop_rx)
        .await
        .unwrap_or_else(|err| {
            error!("rpcserver::handle_connection: negotiations error: {err}");
            None
        })
    else {
        return;
    };
    handle_connection_rpc(engine, server, stream, &mut buf, stop_rx)
        .await
        .unwrap_or_else(|err| error!("rpcserver::handle_connection: rpc error: {err}"));
}

async fn handle_connection_negotiations(
    stream: &mut TcpStream,
    id: ConnectionId,
    buf: &mut [u8],
    stop_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<Option<ServerRpc>> {
    let mut server = Server::new(id);
    loop {
        tokio::select! {
            len = stream.read(buf) => {
                match len {
                    Ok(len) => {
                        if let Err(err) =
                            handle_negotiations(stream, &mut server, &buf[..len]).await {
                            error!("rpcserver::handle_connection_negotiations: processing error: {err}");
                            break;
                        }
                        server = match server.into_rpc() {
                            Ok(server) => return Ok(Some(server)),
                            Err(server) => server,
                        };
                    }
                    Err(err) => {
                        error!("rpcserver::handle_connection_negotiations: unable to read from client: {err}");
                        break;
                    }
                }
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow_and_update() {
                    break;
                }
            }
        }
    }
    Ok(None)
}

async fn handle_connection_rpc(
    engine: Sender<Engine>,
    mut server: ServerRpc,
    mut stream: TcpStream,
    buf: &mut [u8],
    mut stop_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            len = stream.read(buf) => {
                match len {
                    Ok(len) => {
                        if len == 0 {
                            break;
                        }
                        if let Err(err) =
                            handle_rpc(&engine, &mut stream, &mut server, &buf[..len]).await {
                            error!("rpcserver::handle_connection_rpc: processing error: {err}");
                            break;
                        }
                    }
                    Err(err) => {
                        error!("rpcserver::handle_connection_rpc: unable to read from client: {err}");
                        break;
                    }
                }
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow_and_update() {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn handle_negotiations(
    stream: &mut TcpStream,
    server: &mut Server,
    buf: &[u8],
) -> anyhow::Result<()> {
    server.handle_input(buf)?;
    while let Some(output) = server.poll_output() {
        stream.write_all(output.data()).await?;
    }
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct RpcRequest {
    index_id: IndexId,
    embeddings: Embeddings,
    limit: Limit,
}

#[derive(Debug, serde::Serialize)]
struct RpcResponse {
    keys: Vec<Key>,
    distances: Vec<Distance>,
}

async fn handle_rpc(
    engine: &Sender<Engine>,
    stream: &mut TcpStream,
    server: &mut ServerRpc,
    buf: &[u8],
) -> anyhow::Result<()> {
    server.handle_input(buf)?;
    while let Some(request) = server.poll_request() {
        let mut data = request.data();
        let len = data.get_u32_le() as usize;
        let req: RpcRequest = match serde_json::from_slice(&data.as_ref()[..len]) {
            Ok(req) => req,
            Err(err) => {
                server.handle_response(request.msg_id(), |buf| {
                    let answer = format!("deserialize error: {err}");
                    buf.put_u32_le(answer.as_bytes().len() as u32);
                    buf.put_slice(answer.as_bytes())
                })?;
                continue;
            }
        };
        let Some(index) = engine.get_index(req.index_id).await else {
            server.handle_response(request.msg_id(), |buf| {
                let answer = b"index not found";
                buf.put_u32_le(answer.len() as u32);
                buf.put_slice(answer);
            })?;
            continue;
        };
        match index.ann(req.embeddings, req.limit).await {
            Err(err) => {
                server.handle_response(request.msg_id(), |buf| {
                    let answer = format!("ann error: {err}");
                    buf.put_u32_le(answer.as_bytes().len() as u32);
                    buf.put_slice(answer.as_bytes());
                })?;
            }
            Ok((keys, distances)) => {
                let data = serde_json::to_vec(&RpcResponse { keys, distances })?;
                server.handle_response(request.msg_id(), |buf| {
                    buf.put_u32_le(data.len() as u32);
                    buf.put_slice(&data);
                })?;
            }
        }
    }
    while let Some(output) = server.poll_output() {
        stream.write_all(output.data()).await?;
    }
    Ok(())
}
