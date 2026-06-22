//! The ce-fn wire protocol: invocation envelopes and the pubsub trigger event.
//!
//! Invocation is HTTP-shaped but rides CE's authenticated `AppRequest`/reply primitive (the same
//! one swarm uses for `rdev/exec`), not a node RPC. A caller sends an [`InvokeRequest`] to the
//! host running the function on the [`INVOKE_TOPIC`] topic; the function runtime answers with an
//! [`InvokeResponse`]. Triggers reuse CE pubsub: a [`TriggerEvent`] published on a watched topic
//! cold-spawns one invocation per event.

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

/// The `AppRequest`/pubsub topic carrying function invocations. The function name is in the body
/// (not the topic) so a single runtime endpoint can host many functions.
pub const INVOKE_TOPIC: &str = "ce-fn/invoke";

/// An HTTP-style invocation of a named function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvokeRequest {
    /// Which function to run on the receiving host.
    pub function: String,
    /// Optional capability chain (hex token) authorizing the caller to invoke. Empty = none.
    #[serde(default)]
    pub caps: String,
    /// Request payload (opaque bytes), hex-encoded on the wire.
    #[serde(default)]
    pub payload_hex: String,
    /// Optional content-type hint for the payload (informational; the handler decides).
    #[serde(default)]
    pub content_type: Option<String>,
}

impl InvokeRequest {
    /// Build an invoke request for `function` with raw `payload` bytes.
    pub fn new(function: impl Into<String>, payload: &[u8]) -> Self {
        InvokeRequest {
            function: function.into(),
            caps: String::new(),
            payload_hex: hex::encode(payload),
            content_type: None,
        }
    }

    /// Attach a capability token authorizing the invocation.
    pub fn with_caps(mut self, caps: impl Into<String>) -> Self {
        self.caps = caps.into();
        self
    }

    /// Attach a content-type hint.
    pub fn with_content_type(mut self, ct: impl Into<String>) -> Self {
        self.content_type = Some(ct.into());
        self
    }

    /// Decode the request payload bytes.
    pub fn payload(&self) -> Result<Vec<u8>> {
        hex::decode(&self.payload_hex).map_err(|e| anyhow!("bad payload hex: {e}"))
    }

    /// Serialize to the bytes carried in an `AppRequest`.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Parse from `AppRequest` bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!("malformed invoke request: {e}"))
    }
}

/// The function runtime's reply to an [`InvokeRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvokeResponse {
    /// True if the handler ran and exited 0; false on dispatch error or non-zero exit.
    pub ok: bool,
    /// Handler exit code (0 on success); absent if it never started.
    #[serde(default)]
    pub exit_code: Option<i64>,
    /// Response body the handler produced (its stdout / output), hex-encoded.
    #[serde(default)]
    pub output_hex: String,
    /// Diagnostic / stderr text on failure.
    #[serde(default)]
    pub error: Option<String>,
}

impl InvokeResponse {
    /// A success carrying `output` bytes.
    pub fn success(output: &[u8]) -> Self {
        InvokeResponse {
            ok: true,
            exit_code: Some(0),
            output_hex: hex::encode(output),
            error: None,
        }
    }

    /// A failure carrying an error message.
    pub fn failure(error: impl Into<String>) -> Self {
        InvokeResponse {
            ok: false,
            exit_code: None,
            output_hex: String::new(),
            error: Some(error.into()),
        }
    }

    /// Decode the response output bytes.
    pub fn output(&self) -> Result<Vec<u8>> {
        hex::decode(&self.output_hex).map_err(|e| anyhow!("bad output hex: {e}"))
    }

    /// Serialize to reply bytes.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Parse from reply bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!("malformed invoke response: {e}"))
    }
}

/// An event delivered on a watched pubsub topic that should trigger a function invocation. Apps
/// (e.g. `ce-storage` on object upload) publish these; the [`crate::FnClient`] trigger loop maps
/// each into an [`InvokeRequest`] for the bound function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerEvent {
    /// The topic that produced the event (for the handler's context).
    #[serde(default)]
    pub topic: String,
    /// Opaque event data passed through as the invocation payload, hex-encoded.
    #[serde(default)]
    pub data_hex: String,
}

impl TriggerEvent {
    /// Build a trigger event for `topic` carrying `data`.
    pub fn new(topic: impl Into<String>, data: &[u8]) -> Self {
        TriggerEvent { topic: topic.into(), data_hex: hex::encode(data) }
    }

    /// The event data bytes.
    pub fn data(&self) -> Result<Vec<u8>> {
        hex::decode(&self.data_hex).map_err(|e| anyhow!("bad event data hex: {e}"))
    }

    /// Serialize for publishing.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Parse a received event. If the bytes are not a `TriggerEvent`, treat the whole payload as
    /// raw event data (so a function can be triggered by any topic, not just ce-fn-aware ones).
    pub fn decode_lenient(topic: &str, bytes: &[u8]) -> Self {
        match serde_json::from_slice::<TriggerEvent>(bytes) {
            Ok(ev) => ev,
            Err(_) => TriggerEvent { topic: topic.to_string(), data_hex: hex::encode(bytes) },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invoke_request_roundtrip() {
        let req = InvokeRequest::new("resize", b"hello")
            .with_caps("deadbeef")
            .with_content_type("image/png");
        let bytes = req.encode();
        let back = InvokeRequest::decode(&bytes).unwrap();
        assert_eq!(back.function, "resize");
        assert_eq!(back.payload().unwrap(), b"hello");
        assert_eq!(back.caps, "deadbeef");
        assert_eq!(back.content_type.as_deref(), Some("image/png"));
    }

    #[test]
    fn invoke_response_success_and_failure() {
        let ok = InvokeResponse::success(b"thumb");
        assert!(ok.ok);
        assert_eq!(ok.output().unwrap(), b"thumb");
        let back = InvokeResponse::decode(&ok.encode()).unwrap();
        assert_eq!(back, ok);

        let err = InvokeResponse::failure("denied");
        assert!(!err.ok);
        assert_eq!(err.error.as_deref(), Some("denied"));
    }

    #[test]
    fn trigger_event_roundtrip() {
        let ev = TriggerEvent::new("ce-storage/uploads", b"cid123");
        let back = TriggerEvent::decode_lenient("ce-storage/uploads", &ev.encode());
        assert_eq!(back.topic, "ce-storage/uploads");
        assert_eq!(back.data().unwrap(), b"cid123");
    }

    #[test]
    fn trigger_event_lenient_on_raw_bytes() {
        // arbitrary non-JSON bytes → treated as raw event data
        let ev = TriggerEvent::decode_lenient("some/topic", b"\x00\x01\x02");
        assert_eq!(ev.topic, "some/topic");
        assert_eq!(ev.data().unwrap(), vec![0, 1, 2]);
    }

    #[test]
    fn payload_bad_hex_errors() {
        let req = InvokeRequest {
            function: "f".into(),
            caps: String::new(),
            payload_hex: "zz".into(),
            content_type: None,
        };
        assert!(req.payload().is_err());
    }
}
