use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// The URL of the SSE endpoint to connect to
    #[arg(value_name = "URL")]
    pub sse_url: Option<String>,

    /// Enable debug logging
    #[arg(long)]
    pub debug: bool,

    /// Maximum time to try reconnecting in seconds
    #[arg(long)]
    pub max_disconnected_time: Option<u64>,

    /// Initial retry interval in seconds. Default is 5 seconds
    #[arg(long, default_value = "5")]
    pub initial_retry_interval: u64,

    #[arg(long)]
    /// Override the protocol version returned to the client
    pub override_protocol_version: Option<String>,

    /// Inline JSON describing the tools the proxy should advertise before
    /// the backend has connected. Accepts either a full `ListToolsResult`
    /// (`{"tools":[...]}`) or a bare tools array (`[...]`). Intended for
    /// inline use in MCP client configs such as `.mcp.json` — no separate
    /// file needed. Takes precedence over the URL-keyed on-disk cache; the
    /// on-disk cache is still refreshed whenever the backend returns a live
    /// `tools/list` response.
    #[arg(long, value_name = "JSON")]
    pub tools: Option<String>,
}
