use crate::cache;
use crate::core::{
    flush_buffer_with_errors, generate_id, initiate_post_reconnect_handshake,
    process_buffered_messages, process_client_request, reply_disconnected,
    send_heartbeat_if_needed, try_reconnect,
};
use crate::{SseClientType, StdoutSink};
use anyhow::Result;
use futures::SinkExt;
use rmcp::model::{
    ClientJsonRpcMessage, ClientNotification, ClientRequest, EmptyResult, Implementation,
    InitializeResult, InitializedNotification, InitializedNotificationMethod, ListToolsResult,
    ProtocolVersion, RequestId, ServerCapabilities, ServerJsonRpcMessage, ServerResult,
    ToolsCapability,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Reasons why a reconnection attempt might fail.
#[derive(Debug)]
pub enum ReconnectFailureReason {
    TimeoutExceeded,
    ConnectionFailed(anyhow::Error),
}

/// Buffer mode for message handling during disconnection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferMode {
    Store,
    Fail,
}

/// Proxy state to track connection and message handling
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyState {
    Connecting,
    Connected,
    Disconnected,
    WaitingForClientInit,
    WaitingForServerInit(RequestId),
    WaitingForServerInitHidden(RequestId),
    WaitingForClientInitialized,
}

/// Application state for the proxy
#[derive(Debug)]
pub struct AppState {
    /// URL of the SSE server
    pub url: String,
    /// Maximum time to try reconnecting in seconds (None = infinity)
    pub max_disconnected_time: Option<u64>,
    /// Override protocol version
    pub override_protocol_version: Option<ProtocolVersion>,
    /// When we were disconnected
    pub disconnected_since: Option<Instant>,
    /// Current state of the application
    pub state: ProxyState,
    /// Number of connection attempts
    pub connect_tries: u32,
    /// The initialization message (for reconnection)
    pub init_message: Option<ClientJsonRpcMessage>,
    /// Map of generated IDs to original IDs (Client -> Server flow)
    pub id_map: HashMap<String, RequestId>,
    /// Buffer for holding messages during reconnection
    pub in_buf: Vec<ClientJsonRpcMessage>,
    /// Buffer mode (store or fail)
    pub buf_mode: BufferMode,
    /// Whether a flush timer is in progress
    pub flush_timer_active: bool,
    /// Channel sender for reconnect events
    pub reconnect_tx: Option<Sender<()>>,
    /// Channel sender for timer events
    pub timer_tx: Option<Sender<()>>,
    /// Whether reconnect is already scheduled
    pub reconnect_scheduled: bool,
    /// Whether the transport is still valid
    pub transport_valid: bool,
    /// Time of last heartbeat check
    pub last_heartbeat: Instant,
    /// Cached `tools/list` result, loaded from disk (or `--tools`) at startup
    /// and refreshed whenever the backend returns a fresh `tools/list`
    /// response. Used to answer client `tools/list` requests when the
    /// backend is unreachable.
    pub tools_cache: Option<ListToolsResult>,
}

impl AppState {
    pub fn new(
        url: String,
        max_disconnected_time: Option<u64>,
        override_protocol_version: Option<ProtocolVersion>,
        inline_tools_json: Option<&str>,
    ) -> Self {
        // Priority: URL-keyed on-disk cache > inline --tools JSON > None. The
        // on-disk cache is refreshed every time the backend responds, so it
        // is always at least as fresh as the inline seed. `--tools` exists to
        // bootstrap a URL that has never been reached (fresh worktree, new
        // port, first run on a new machine), not to override observed tools.
        let tools_cache = cache::load(&url)
            .or_else(|| inline_tools_json.and_then(cache::parse_inline));
        Self {
            tools_cache,
            url,
            max_disconnected_time,
            override_protocol_version,
            disconnected_since: None,
            state: ProxyState::Connecting,
            connect_tries: 0,
            init_message: None,
            id_map: HashMap::new(),
            in_buf: Vec::new(),
            buf_mode: BufferMode::Store,
            flush_timer_active: false,
            reconnect_tx: None,
            timer_tx: None,
            reconnect_scheduled: false,
            transport_valid: false,
            last_heartbeat: Instant::now(),
        }
    }

    pub fn connected(&mut self) {
        self.state = ProxyState::Connected;
        self.connect_tries = 0;
        self.disconnected_since = None;
        self.buf_mode = BufferMode::Store;
        self.reconnect_scheduled = false;
        self.transport_valid = true;
        self.last_heartbeat = Instant::now();
    }

    pub fn disconnected(&mut self) {
        if self.state != ProxyState::Disconnected {
            debug!("State changing to disconnected");
            self.state = ProxyState::Disconnected;
            self.disconnected_since = Some(Instant::now());
            self.buf_mode = BufferMode::Store;
            self.transport_valid = false;
        }
        self.connect_tries += 1;
    }

    pub fn disconnected_too_long(&self) -> bool {
        match (self.max_disconnected_time, self.disconnected_since) {
            (Some(max_time), Some(since)) => since.elapsed().as_secs() > max_time,
            _ => false,
        }
    }

    pub fn get_backoff_duration(&self) -> Duration {
        let clamped_tries = std::cmp::min(self.connect_tries, 3);
        let seconds = 2u64.pow(clamped_tries);
        Duration::from_secs(seconds)
    }

    pub fn schedule_reconnect(&mut self) {
        if !self.reconnect_scheduled {
            if let Some(tx) = &self.reconnect_tx {
                let tx_clone = tx.clone();
                let backoff = self.get_backoff_duration();
                debug!("Scheduling reconnect in {}s", backoff.as_secs());
                tokio::spawn(async move {
                    sleep(backoff).await;
                    let _ = tx_clone.send(()).await;
                });
                self.reconnect_scheduled = true;
            }
        } else {
            debug!("Reconnect already scheduled, skipping");
        }
    }

    pub fn schedule_flush_timer(&mut self) {
        if !self.flush_timer_active {
            if let Some(tx) = &self.timer_tx {
                debug!("Scheduling flush timer for 20s");
                self.flush_timer_active = true;
                let tx_clone = tx.clone();
                tokio::spawn(async move {
                    sleep(Duration::from_secs(20)).await;
                    let _ = tx_clone.send(()).await;
                });
            }
        } else {
            debug!("Flush timer already active, skipping");
        }
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    /// Handles common logic for fatal transport errors:
    /// Sets state to disconnected and schedules timer/reconnect.
    pub fn handle_fatal_transport_error(&mut self) {
        if self.state != ProxyState::Disconnected {
            self.disconnected();
            self.schedule_flush_timer();
            self.schedule_reconnect();
        }
    }

    /// Handles messages received from stdin.
    /// Returns Ok(true) to continue processing, Ok(false) to break the main loop.
    pub(crate) async fn handle_stdin_message(
        &mut self,
        msg: Option<
            Result<ClientJsonRpcMessage, rmcp::transport::async_rw::JsonRpcMessageCodecError>,
        >,
        transport: &mut Option<SseClientType>,
        stdout_sink: &mut StdoutSink,
    ) -> Result<bool> {
        match msg {
            Some(Ok(message)) => {
                process_client_request(message, self, transport, stdout_sink).await?;
                Ok(true)
            }
            Some(Err(e)) => {
                error!("Error reading from stdin: {}", e);
                Ok(false)
            }
            None => {
                info!("Stdin stream ended.");
                Ok(false)
            }
        }
    }

    /// Handles messages received from the SSE transport.
    /// Returns Ok(true) to continue processing, Ok(false) to break the main loop.
    pub(crate) async fn handle_sse_message(
        &mut self,
        result: Option<ServerJsonRpcMessage>,
        transport: &mut SseClientType,
        stdout_sink: &mut StdoutSink,
    ) -> Result<bool> {
        debug!("Received SSE message: {:?}", result);
        match result {
            Some(mut message) => {
                self.update_heartbeat();

                // --- Handle Server-Initiated Request ---
                if let ServerJsonRpcMessage::Request(mut req) = message {
                    let server_id = req.id.clone();
                    let proxy_id_str = generate_id();
                    let proxy_id = RequestId::String(proxy_id_str.clone().into());
                    debug!(
                        "Mapping server request ID {} to proxy ID {}",
                        server_id, proxy_id
                    );
                    self.id_map.insert(proxy_id_str, server_id);
                    req.id = proxy_id;
                    message = ServerJsonRpcMessage::Request(req);
                    // Now fall through to forward the modified request
                }
                // --- End Server-Initiated Request Handling ---
                else {
                    match self.map_server_response_error_id(message) {
                        Some(mapped_message) => message = mapped_message,
                        None => return Ok(true), // Skip forwarding this message
                    }
                    // --- Handle Initialization Response --- (Only for Response/Error)
                    let is_init_response = match &message {
                        ServerJsonRpcMessage::Response(response) => match self.state {
                            ProxyState::WaitingForServerInit(ref init_request_id) => {
                                *init_request_id == response.id
                            }
                            ProxyState::WaitingForServerInitHidden(ref init_request_id) => {
                                *init_request_id == response.id
                            }
                            _ => false,
                        },
                        // Don't treat errors related to init ID as special init responses
                        _ => false,
                    };

                    debug!(
                        "Handling initialization response - state: {:?}, message: {:?}, is_init_response: {}",
                        self.state, message, is_init_response
                    );

                    if is_init_response {
                        let was_hidden =
                            matches!(self.state, ProxyState::WaitingForServerInitHidden(_));
                        if was_hidden {
                            self.connected();
                            debug!("Reconnection successful, received hidden init response");
                            let initialized_notification = ClientJsonRpcMessage::notification(
                                ClientNotification::InitializedNotification(
                                    InitializedNotification {
                                        method: InitializedNotificationMethod,
                                        extensions: rmcp::model::Extensions::default(),
                                    },
                                ),
                            );
                            if let Err(e) = transport.send(initialized_notification).await {
                                error!(
                                    "Error sending initialized notification post-reconnect: {}",
                                    e
                                );
                                self.handle_fatal_transport_error();
                            } else {
                                process_buffered_messages(self, transport, stdout_sink).await?;
                            }
                            return Ok(true); // Don't forward the init response
                        } else {
                            debug!(
                                "Initial connection successful, received init response. Waiting for client initialized."
                            );
                            self.state = ProxyState::WaitingForClientInitialized;
                            message = self.maybe_overwrite_protocol_version(message);
                        }
                    }
                    // --- End Initialization Response Handling ---
                }

                // Intercept `tools/list` responses on their way to the client
                // so subsequent sessions can serve them from disk even when
                // the backend isn't up yet.
                self.maybe_cache_tools_result(&message);

                // Forward the (potentially modified) message to stdout
                // This now handles mapped server requests, mapped responses/errors, and notifications
                debug!("Forwarding from SSE to stdout: {:?}", message);
                if let Err(e) = stdout_sink.send(message).await {
                    error!("Error writing to stdout: {}", e);
                    return Ok(false);
                }

                Ok(true)
            }
            None => {
                debug!("SSE stream ended (Fatal error in transport) - trying to reconnect");
                self.handle_fatal_transport_error();
                Ok(true)
            }
        }
    }

    pub(crate) async fn maybe_handle_message_while_disconnected(
        &mut self,
        message: ClientJsonRpcMessage,
        stdout_sink: &mut StdoutSink,
    ) -> Result<()> {
        // "Offline" covers both `Connecting` (backend has never come up yet)
        // and `Disconnected` (backend was up and died). Message handling is
        // identical in both cases — respond to init/ping/tools.list locally
        // and buffer everything else until the backend arrives.
        if !matches!(
            self.state,
            ProxyState::Connecting | ProxyState::Disconnected
        ) {
            return Err(anyhow::anyhow!("Not offline"));
        }

        match message {
            ClientJsonRpcMessage::Request(req) => match &req.request {
                ClientRequest::PingRequest(_) => {
                    debug!(
                        "Received Ping request while offline, replying directly: {:?}",
                        req.id
                    );
                    let response = ServerJsonRpcMessage::response(
                        ServerResult::EmptyResult(EmptyResult {}),
                        req.id.clone(),
                    );
                    if let Err(e) = stdout_sink.send(response).await {
                        error!("Error sending direct ping response to stdout: {}", e);
                    }
                }
                ClientRequest::InitializeRequest(_) => {
                    debug!(
                        "Received Initialize while offline, replying locally: {:?}",
                        req.id
                    );
                    // Remember the full init message so that when the backend
                    // eventually connects, the hidden-init flow can replay it
                    // (see `initiate_post_reconnect_handshake`).
                    let stored = ClientJsonRpcMessage::Request(req.clone());
                    if self.init_message.is_none() {
                        self.init_message = Some(stored);
                    }
                    let response = local_initialize_response(
                        req.id.clone(),
                        &self.override_protocol_version,
                    );
                    if let Err(e) = stdout_sink.send(response).await {
                        error!("Error sending local Initialize response: {}", e);
                    }
                    // Stay in `Connecting`/`Disconnected` — the client's
                    // subsequent `initialized` notification and any tool
                    // calls keep flowing through the offline path until the
                    // backend transport becomes available.
                }
                ClientRequest::ListToolsRequest(_) => match self.tools_cache.clone() {
                    Some(cached) => {
                        debug!(
                            "Serving tools/list from cache while offline: {:?} ({} tools)",
                            req.id,
                            cached.tools.len()
                        );
                        let response = ServerJsonRpcMessage::response(
                            ServerResult::ListToolsResult(cached),
                            req.id.clone(),
                        );
                        if let Err(e) = stdout_sink.send(response).await {
                            error!("Error sending cached tools/list response: {}", e);
                        }
                    }
                    None => {
                        debug!(
                            "Serving empty tools/list while offline (no cache): {:?}",
                            req.id
                        );
                        let response = ServerJsonRpcMessage::response(
                            ServerResult::ListToolsResult(ListToolsResult {
                                tools: Vec::new(),
                                next_cursor: None,
                            }),
                            req.id.clone(),
                        );
                        if let Err(e) = stdout_sink.send(response).await {
                            error!("Error sending empty tools/list response: {}", e);
                        }
                    }
                },
                _ => {
                    if self.buf_mode == BufferMode::Store {
                        debug!("Buffering request for later retry");
                        self.in_buf.push(ClientJsonRpcMessage::Request(req));
                    } else {
                        reply_disconnected(&req.id, stdout_sink).await?;
                    }
                }
            },
            ClientJsonRpcMessage::Notification(notif) => {
                if matches!(
                    notif.notification,
                    ClientNotification::InitializedNotification(_)
                ) {
                    debug!(
                        "Consumed initialized notification while offline; \
                         will replay via hidden handshake when backend connects."
                    );
                } else if self.buf_mode == BufferMode::Store {
                    debug!("Buffering notification for later retry");
                    self.in_buf.push(ClientJsonRpcMessage::Notification(notif));
                }
            }
            other => {
                if self.buf_mode == BufferMode::Store {
                    debug!("Buffering client message for later retry");
                    self.in_buf.push(other);
                }
            }
        }

        Ok(())
    }

    /// Handles the reconnect signal.
    /// Returns the potentially new transport if reconnection was successful.
    /// Fires for both the initial connection and every subsequent reconnect
    /// — so `Connecting` and `Disconnected` are both valid entry states.
    pub(crate) async fn handle_reconnect_signal(
        &mut self,
        stdout_sink: &mut StdoutSink,
    ) -> Result<Option<SseClientType>> {
        debug!("Received reconnect signal");
        self.reconnect_scheduled = false;

        if !matches!(
            self.state,
            ProxyState::Connecting | ProxyState::Disconnected
        ) {
            return Ok(None);
        }

        match try_reconnect(self).await {
            Ok(new_transport) => {
                self.transport_valid = true;

                // The handshake re-uses the client's stored init_message (if any).
                // On initial connect with no client-init-yet, there's nothing to
                // replay — the client's eventual `initialize` will flow through
                // normally once the transport is set.
                let mut transport_opt = Some(new_transport);
                if self.init_message.is_some() {
                    match initiate_post_reconnect_handshake(self, &mut transport_opt, stdout_sink)
                        .await
                    {
                        Ok(true) => Ok(transport_opt),
                        Ok(false) => Ok(None),
                        Err(e) => Err(e),
                    }
                } else {
                    // No client-init yet: skip the hidden handshake, just mark
                    // transport ready. We'll transition through the normal
                    // pass-through init path when the client sends `initialize`.
                    self.state = ProxyState::WaitingForClientInit;
                    Ok(transport_opt)
                }
            }
            Err(reason) => {
                self.connect_tries += 1;
                match reason {
                    ReconnectFailureReason::TimeoutExceeded => {
                        error!(
                            "Reconnect attempt {} failed: Timeout exceeded",
                            self.connect_tries
                        );
                        info!("Disconnected too long, flushing buffer.");
                        flush_buffer_with_errors(self, stdout_sink).await?;
                    }
                    ReconnectFailureReason::ConnectionFailed(e) => {
                        error!(
                            "Reconnect attempt {} failed: Connection error: {}",
                            self.connect_tries, e
                        );
                        if !self.disconnected_too_long() {
                            self.schedule_reconnect();
                        } else {
                            info!("Disconnected too long after failed connect, flushing buffer.");
                            flush_buffer_with_errors(self, stdout_sink).await?;
                        }
                    }
                }
                Ok(None)
            }
        }
    }

    /// Handles the flush timer signal.
    pub(crate) async fn handle_timer_signal(&mut self, stdout_sink: &mut StdoutSink) -> Result<()> {
        debug!("Received flush timer signal");
        self.flush_timer_active = false;
        if self.state == ProxyState::Disconnected {
            info!("Still disconnected after 20 seconds, flushing buffer with errors");
            flush_buffer_with_errors(self, stdout_sink).await?;
        }
        Ok(())
    }

    /// Handles the heartbeat interval tick.
    pub(crate) async fn handle_heartbeat_tick(
        &mut self,
        transport: &mut SseClientType,
    ) -> Result<()> {
        if self.state == ProxyState::Connected {
            let check_result = send_heartbeat_if_needed(self, transport).await;
            match check_result {
                Some(true) => {
                    self.update_heartbeat();
                }
                Some(false) => {
                    self.handle_fatal_transport_error();
                }
                None => {}
            }
        }
        Ok(())
    }

    // --- ID Mapping Helpers ---
    fn lookup_and_remove_original_id(&mut self, current_id: &RequestId) -> Option<RequestId> {
        let lookup_key = current_id.to_string();
        self.id_map.remove(&lookup_key)
    }

    // Add the client mapping logic here
    pub(crate) fn map_client_response_error_id(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> Option<ClientJsonRpcMessage> {
        let (id_to_check, is_response_or_error) = match &message {
            ClientJsonRpcMessage::Response(res) => (Some(res.id.clone()), true),
            ClientJsonRpcMessage::Error(err) => (Some(err.id.clone()), true),
            _ => (None, false), // Requests or Notifications are not mapped back this way
        };

        if is_response_or_error {
            if let Some(current_id) = id_to_check {
                if let Some(original_id) = self.lookup_and_remove_original_id(&current_id) {
                    debug!(
                        "Mapping client message ID {} back to original server ID: {}",
                        current_id, original_id
                    );
                    return Some(match message {
                        ClientJsonRpcMessage::Response(mut res) => {
                            res.id = original_id;
                            ClientJsonRpcMessage::Response(res)
                        }
                        ClientJsonRpcMessage::Error(mut err) => {
                            err.id = original_id;
                            ClientJsonRpcMessage::Error(err)
                        }
                        _ => message, // Should not happen
                    });
                } else {
                    // ID not found, return None to prevent forwarding
                    warn!(
                        "Received client response/error with unknown ID: {}, skipping forwarding.",
                        current_id
                    );
                    return None;
                }
            } else {
                // Error message has no ID (should not happen for JSON-RPC errors)
                warn!("Received client error message without an ID, skipping forwarding.");
                return None;
            }
        }
        // Not a response/error, return Some(original_message)
        Some(message)
    }

    /// Checks if a server message (Response or Error) has an ID corresponding
    /// to a mapped client request ID. If found, replaces the message's ID with
    /// the original client ID and returns `Some` modified message.
    /// Otherwise, returns `None`.
    // Renamed from try_map_server_message_id and made private
    pub(crate) fn map_server_response_error_id(
        &mut self,
        message: ServerJsonRpcMessage,
    ) -> Option<ServerJsonRpcMessage> {
        let (id_to_check, is_response_or_error) = match &message {
            ServerJsonRpcMessage::Response(res) => (Some(res.id.clone()), true),
            ServerJsonRpcMessage::Error(err) => (Some(err.id.clone()), true),
            _ => (None, false), // Notifications or Requests are not mapped back
        };

        if is_response_or_error {
            if let Some(current_id) = id_to_check {
                if let Some(original_id) = self.lookup_and_remove_original_id(&current_id) {
                    debug!(
                        "Mapping server message ID {} back to original client ID: {}",
                        current_id, original_id
                    );
                    return Some(match message {
                        ServerJsonRpcMessage::Response(mut res) => {
                            res.id = original_id;
                            ServerJsonRpcMessage::Response(res)
                        }
                        ServerJsonRpcMessage::Error(mut err) => {
                            err.id = original_id;
                            ServerJsonRpcMessage::Error(err)
                        }
                        _ => message, // Should not happen due to is_response_or_error check
                    });
                } else {
                    // ID not found, return None to prevent forwarding
                    warn!(
                        "Received server response/error with unknown ID: {}, skipping forwarding.",
                        current_id
                    );
                    return None;
                }
            } else {
                // Error message has no ID (should not happen for JSON-RPC errors)
                warn!("Received server error message without an ID, skipping forwarding.");
                return None;
            }
        }
        // Not a response/error, return Some(original_message)
        Some(message)
    }

    fn maybe_overwrite_protocol_version(
        &mut self,
        message: ServerJsonRpcMessage,
    ) -> ServerJsonRpcMessage {
        if let Some(protocol_version) = &self.override_protocol_version {
            match message {
                ServerJsonRpcMessage::Response(mut resp) => {
                    if let ServerResult::InitializeResult(mut initialize_result) = resp.result {
                        initialize_result.protocol_version = protocol_version.clone();
                        resp.result = ServerResult::InitializeResult(initialize_result);
                        return ServerJsonRpcMessage::Response(resp);
                    }
                    ServerJsonRpcMessage::Response(resp)
                }
                other => other,
            }
        } else {
            message
        }
    }

    /// If the incoming server message is a `tools/list` response, save it to
    /// disk so future sessions can answer the client before the backend is up.
    pub(crate) fn maybe_cache_tools_result(&mut self, message: &ServerJsonRpcMessage) {
        if let ServerJsonRpcMessage::Response(resp) = message
            && let ServerResult::ListToolsResult(tools) = &resp.result
        {
            debug!(
                "Caching tools/list response to disk ({} tools)",
                tools.tools.len()
            );
            cache::save(&self.url, tools);
            self.tools_cache = Some(tools.clone());
        }
    }
}

/// Build an `initialize` response the proxy can send without a live backend.
///
/// We advertise `tools.listChanged: true` so well-behaved clients know to
/// refresh on the eventual `notifications/tools/list_changed` emission, even
/// though Claude Code 2.1.x currently has a known bug where it doesn't honor
/// it for late-discovered tools. The real load-bearing piece is the disk
/// cache that backs subsequent `tools/list` calls.
fn local_initialize_response(
    id: RequestId,
    override_protocol_version: &Option<ProtocolVersion>,
) -> ServerJsonRpcMessage {
    let protocol_version = override_protocol_version
        .clone()
        .unwrap_or(ProtocolVersion::V_2025_03_26);
    let capabilities = ServerCapabilities {
        tools: Some(ToolsCapability {
            list_changed: Some(true),
        }),
        ..Default::default()
    };
    let server_info = Implementation {
        name: "mcp-proxy-cached".into(),
        title: None,
        version: env!("CARGO_PKG_VERSION").into(),
        icons: None,
        website_url: None,
    };
    ServerJsonRpcMessage::response(
        ServerResult::InitializeResult(InitializeResult {
            protocol_version,
            capabilities,
            server_info,
            instructions: Some(
                "Proxy responding before backend has connected. \
                 tools/list is served from disk cache; tool calls buffer until the backend is ready."
                    .into(),
            ),
        }),
        id,
    )
}
