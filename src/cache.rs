//! Persistent cache for `tools/list` responses, keyed by backend URL.
//!
//! Clients like Claude Code freeze their tool catalog at the moment they
//! receive the first `tools/list` response, and there are known client-side
//! bugs (see `anthropics/claude-code#4118`) where late-arriving
//! `notifications/tools/list_changed` does not refresh that frozen catalog
//! for the active conversation. So if the backend isn't up at session start,
//! the client never sees any tools — even after the backend comes up.
//!
//! The cache sidesteps that by persisting the last successful `tools/list`
//! response to disk. On next startup, the proxy can answer the client's
//! `tools/list` request immediately from the cache, without waiting for the
//! backend.

use rmcp::model::{ListToolsResult, Tool};
use std::fs;
use std::path::PathBuf;
use tracing::{debug, info, warn};

/// Directory where tool caches are stored. Returns `None` if no suitable
/// directory is available, in which case caching is silently disabled.
fn cache_dir() -> Option<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME") {
        return Some(PathBuf::from(xdg).join("mcp-proxy"));
    }
    if cfg!(target_os = "macos") {
        if let Some(home) = std::env::var_os("HOME") {
            return Some(PathBuf::from(home).join("Library/Caches/mcp-proxy"));
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        return Some(PathBuf::from(home).join(".cache/mcp-proxy"));
    }
    None
}

/// Filesystem-safe filename derived from the backend URL.
///
/// We keep the filename human-readable rather than hashing — URLs are short
/// enough in practice that collisions aren't a concern, and a readable name
/// makes it easier to inspect or clear individual caches by hand.
fn filename_for(url: &str) -> String {
    let mut out = String::with_capacity(url.len() + 6);
    for ch in url.chars() {
        match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => out.push(ch),
            _ => out.push('_'),
        }
    }
    out.push_str(".tools.json");
    out
}

/// Load a previously-cached `tools/list` response for `url`, if any.
pub fn load(url: &str) -> Option<ListToolsResult> {
    let path = cache_dir()?.join(filename_for(url));
    let bytes = fs::read(&path).ok()?;
    match serde_json::from_slice::<ListToolsResult>(&bytes) {
        Ok(result) => {
            debug!(
                "Loaded tools cache from {:?} ({} tools)",
                path,
                result.tools.len()
            );
            Some(result)
        }
        Err(e) => {
            warn!("Failed to parse tools cache {:?}: {}", path, e);
            None
        }
    }
}

/// Parse an inline JSON value — either a full `ListToolsResult`
/// (`{"tools":[...]}`) or a bare tools array (`[...]`) — into a
/// `ListToolsResult` usable as the proxy's offline tool advertisement.
///
/// Kept lenient on shape since this is fed from hand-maintained `.mcp.json`
/// args; failing loud via a warning is better than failing silent.
pub fn parse_inline(json: &str) -> Option<ListToolsResult> {
    match serde_json::from_str::<ListToolsResult>(json) {
        Ok(result) => {
            info!(
                "Parsed inline --tools as ListToolsResult ({} tools)",
                result.tools.len()
            );
            return Some(result);
        }
        Err(full_err) => match serde_json::from_str::<Vec<Tool>>(json) {
            Ok(tools) => {
                info!("Parsed inline --tools as tools array ({} tools)", tools.len());
                return Some(ListToolsResult {
                    tools,
                    next_cursor: None,
                });
            }
            Err(arr_err) => {
                warn!(
                    "Failed to parse --tools as ListToolsResult ({}) or tools array ({})",
                    full_err, arr_err
                );
            }
        },
    }
    None
}

/// Persist `result` to disk for `url`. Errors are logged but never propagated
/// — caching is a best-effort optimisation, never a correctness requirement.
pub fn save(url: &str, result: &ListToolsResult) {
    let Some(dir) = cache_dir() else {
        debug!("No cache dir available, skipping tools cache save");
        return;
    };
    if let Err(e) = fs::create_dir_all(&dir) {
        warn!("Failed to create cache dir {:?}: {}", dir, e);
        return;
    }
    let path = dir.join(filename_for(url));
    let bytes = match serde_json::to_vec_pretty(result) {
        Ok(b) => b,
        Err(e) => {
            warn!("Failed to serialize tools cache: {}", e);
            return;
        }
    };
    if let Err(e) = fs::write(&path, bytes) {
        warn!("Failed to write tools cache to {:?}: {}", path, e);
    } else {
        debug!(
            "Wrote tools cache to {:?} ({} tools)",
            path,
            result.tools.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_is_safe_and_stable() {
        let f = filename_for("http://localhost:9712/tidewave/mcp");
        assert!(f.ends_with(".tools.json"));
        assert!(
            f.chars()
                .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-'))
        );
        // Same URL → same filename.
        assert_eq!(f, filename_for("http://localhost:9712/tidewave/mcp"));
        // Different URL → different filename.
        assert_ne!(f, filename_for("http://localhost:4000/tidewave/mcp"));
    }
}
