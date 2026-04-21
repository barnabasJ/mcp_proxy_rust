use anyhow::{Context, Result, anyhow};
use clap::Parser;
use futures::StreamExt;
use rmcp::{
    model::{ClientJsonRpcMessage, ErrorCode, ProtocolVersion, ServerJsonRpcMessage},
    transport::{StreamableHttpClientTransport, Transport, sse_client::SseClientTransport},
};
use std::env;
use tokio::io::{Stdin, Stdout};
use tokio::time::Duration;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, error, info};
use tracing_subscriber::FmtSubscriber;

// Modules
mod cache;
mod cli;
mod core;
mod state;

use crate::cli::Args;
use crate::core::flush_buffer_with_errors;
use crate::state::{AppState, ProxyState};

// Custom Error Codes (Keep here or move to common/state? Keeping here for now)
const DISCONNECTED_ERROR_CODE: ErrorCode = ErrorCode(-32010);
const TRANSPORT_SEND_ERROR_CODE: ErrorCode = ErrorCode(-32011);

enum SseClientType {
    Sse(SseClientTransport<reqwest::Client>),
    Streamable(StreamableHttpClientTransport<reqwest::Client>),
}

impl SseClientType {
    async fn send(
        &mut self,
        item: ClientJsonRpcMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        match self {
            SseClientType::Sse(transport) => transport.send(item).await.map_err(|e| e.into()),
            SseClientType::Streamable(transport) => {
                transport.send(item).await.map_err(|e| e.into())
            }
        }
    }

    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        match self {
            SseClientType::Sse(transport) => transport.receive().await,
            SseClientType::Streamable(transport) => transport.receive().await,
        }
    }
}

type StdinCodec = rmcp::transport::async_rw::JsonRpcMessageCodec<ClientJsonRpcMessage>;
type StdoutCodec = rmcp::transport::async_rw::JsonRpcMessageCodec<ServerJsonRpcMessage>;
type StdinStream = FramedRead<Stdin, StdinCodec>;
type StdoutSink = FramedWrite<Stdout, StdoutCodec>;

// --- Main Function ---
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let log_level = if args.debug {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_writer(std::io::stderr)
        .finish();

    tracing::subscriber::set_global_default(subscriber).context("Failed to set up logging")?;

    // Get the SSE URL from args or environment
    let sse_url = match args.sse_url {
          Some(url) => url,
          None => env::var("SSE_URL").context(
              "Either the URL must be passed as the first argument or the SSE_URL environment variable must be set",
          )?,
      };

    debug!("Starting MCP proxy with URL: {}", sse_url);
    debug!("Max disconnected time: {:?}s", args.max_disconnected_time);

    // Parse protocol version override if provided
    let override_protocol_version = if let Some(version_str) = args.override_protocol_version {
        let protocol_version = match version_str.as_str() {
            "2024-11-05" => ProtocolVersion::V_2024_11_05,
            "2025-03-26" => ProtocolVersion::V_2025_03_26,
            _ => {
                return Err(anyhow!(
                    "Unsupported protocol version: {}. Supported versions are: 2024-11-05, 2025-03-26",
                    version_str
                ));
            }
        };
        Some(protocol_version)
    } else {
        None
    };

    // Set up communication channels
    let (reconnect_tx, mut reconnect_rx) = tokio::sync::mpsc::channel(10);
    let (timer_tx, mut timer_rx) = tokio::sync::mpsc::channel(10);

    // Initialize application state. The proxy starts in `Connecting`, with
    // no live transport — the stdio loop comes up immediately so Claude's
    // `initialize` can be answered locally, and the backend connection is
    // established in the background via the existing reconnect channel.
    let mut app_state = AppState::new(
        sse_url.clone(),
        args.max_disconnected_time,
        override_protocol_version,
        args.tools.as_deref(),
    );
    app_state.reconnect_tx = Some(reconnect_tx.clone());
    app_state.timer_tx = Some(timer_tx.clone());
    app_state.state = ProxyState::Connecting;
    app_state.transport_valid = false;

    let mut transport: Option<SseClientType> = None;

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let mut stdin_stream: StdinStream = FramedRead::new(stdin, StdinCodec::default());
    let mut stdout_sink: StdoutSink = FramedWrite::new(stdout, StdoutCodec::default());

    // Kick off the initial backend-connection attempt immediately by firing
    // the reconnect channel ourselves. The retry/backoff loop inside
    // `handle_reconnect_signal` then handles every subsequent attempt.
    info!("Scheduling initial connection to {}...", sse_url);
    if let Err(e) = reconnect_tx.try_send(()) {
        error!("Failed to schedule initial reconnect: {}", e);
    }

    // Grace window: if the backend is already up, give the initial connect
    // a short head start over any incoming client traffic so that requests
    // pass through live instead of being answered from the (possibly stale)
    // offline cache. If the backend is down, we fall out of the grace window
    // quickly and carry on in offline mode.
    //
    // The window is intentionally bounded — the whole point of going
    // non-blocking was to let Claude's `initialize` handshake beat its
    // timeout even when the backend is very slow or absent.
    const INITIAL_GRACE: Duration = Duration::from_millis(1500);
    let grace_deadline = tokio::time::Instant::now() + INITIAL_GRACE;
    while transport.is_none() && tokio::time::Instant::now() < grace_deadline {
        tokio::select! {
            Some(_) = reconnect_rx.recv() => {
                if let Some(new_transport) = app_state.handle_reconnect_signal(&mut stdout_sink).await? {
                    transport = Some(new_transport);
                }
            }
            _ = tokio::time::sleep_until(grace_deadline) => break,
        }
    }
    if transport.is_some() {
        debug!("Initial connection established within grace window.");
    } else {
        debug!("Initial connection did not complete within grace window; continuing in offline mode.");
    }

    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(1));

    // Main event loop
    loop {
        tokio::select! {
            biased;
            // Messages from stdin
            msg = stdin_stream.next() => {
                if !app_state.handle_stdin_message(msg, &mut transport, &mut stdout_sink).await? {
                    break;
                }
            }
            // Messages from the SSE/streamable-http backend (only when a
            // transport exists; `transport_valid` is false until connected).
            result = async {
                match transport.as_mut() {
                    Some(t) => t.receive().await,
                    None => std::future::pending().await,
                }
            }, if app_state.transport_valid && transport.is_some() => {
                let Some(ref mut t) = transport else { unreachable!() };
                if !app_state.handle_sse_message(result, t, &mut stdout_sink).await? {
                    break;
                }
            }
            // Reconnect signal — fires for both the initial connect and every
            // subsequent reconnect.
            Some(_) = reconnect_rx.recv() => {
                if let Some(new_transport) = app_state.handle_reconnect_signal(&mut stdout_sink).await? {
                    transport = Some(new_transport);
                }
                if app_state.disconnected_too_long() {
                    error!("Giving up after failed reconnection attempts and exceeding max disconnected time.");
                    if !app_state.in_buf.is_empty() && app_state.buf_mode == state::BufferMode::Store {
                        flush_buffer_with_errors(&mut app_state, &mut stdout_sink).await?;
                    }
                    break;
                }
            }
            Some(_) = timer_rx.recv() => app_state.handle_timer_signal(&mut stdout_sink).await?,
            _ = heartbeat_interval.tick() => {
                if let Some(ref mut t) = transport {
                    app_state.handle_heartbeat_tick(t).await?;
                }
            }
            else => break,
        }
    }

    info!("Proxy terminated");
    Ok(())
}
