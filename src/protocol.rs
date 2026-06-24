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

/// Maximum invocation **payload** size in bytes (the decoded request body). Mirrors GCF's event
/// trigger cap (10 MiB). Enforced on both encode (caller) and decode (runtime) so neither side
/// can be forced to buffer an unbounded request — closing an OOM/DoS vector.
pub const MAX_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Maximum handler **output** size in bytes a runtime will return in one [`InvokeResponse`].
/// Larger results should be written to the blob/object store and referenced by CID.
pub const MAX_OUTPUT_BYTES: usize = 10 * 1024 * 1024;

/// The larger of the payload and output caps (const-evaluable without trait methods).
const MAX_BODY_BYTES: usize = if MAX_PAYLOAD_BYTES > MAX_OUTPUT_BYTES {
    MAX_PAYLOAD_BYTES
} else {
    MAX_OUTPUT_BYTES
};

/// Maximum size of an encoded (JSON) invoke request/response on the wire. The hex encoding roughly
/// doubles the byte count, plus JSON framing; this bounds the whole envelope so a decode never
/// allocates more than this even before the payload-length check runs.
pub const MAX_ENVELOPE_BYTES: usize = 2 * MAX_BODY_BYTES + 64 * 1024;

/// Maximum length of an error/diagnostic string carried in an [`InvokeResponse`]. Keeps a
/// misbehaving handler from returning a multi-megabyte error blob.
pub const MAX_ERROR_BYTES: usize = 16 * 1024;

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

    /// Decode the request payload bytes, rejecting an oversized payload
    /// (> [`MAX_PAYLOAD_BYTES`]) before allocating the decoded buffer.
    pub fn payload(&self) -> Result<Vec<u8>> {
        // hex is exactly 2 chars per byte, so the decoded length is known without decoding.
        if self.payload_hex.len() / 2 > MAX_PAYLOAD_BYTES {
            return Err(anyhow!(
                "invoke payload too large: {} bytes (max {})",
                self.payload_hex.len() / 2,
                MAX_PAYLOAD_BYTES
            ));
        }
        hex::decode(&self.payload_hex).map_err(|e| anyhow!("bad payload hex: {e}"))
    }

    /// Validate structural invariants: a non-empty, well-formed function name and a bounded payload.
    /// Called by [`encode`](Self::encode) and [`decode`](Self::decode) so neither a caller nor a
    /// runtime ever handles a malformed or oversized request.
    pub fn validate(&self) -> Result<()> {
        crate::function::Function::validate_name(&self.function)?;
        if self.payload_hex.len() / 2 > MAX_PAYLOAD_BYTES {
            return Err(anyhow!(
                "invoke payload too large: {} bytes (max {})",
                self.payload_hex.len() / 2,
                MAX_PAYLOAD_BYTES
            ));
        }
        Ok(())
    }

    /// Serialize to the bytes carried in an `AppRequest`. Errors on a malformed/oversized request
    /// rather than silently emitting empty bytes.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|e| anyhow!("encoding invoke request: {e}"))?;
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(anyhow!("invoke request envelope too large: {} bytes", bytes.len()));
        }
        Ok(bytes)
    }

    /// Parse from `AppRequest` bytes, rejecting an oversized envelope and validating the result.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(anyhow!("invoke request envelope too large: {} bytes", bytes.len()));
        }
        let req: InvokeRequest =
            serde_json::from_slice(bytes).map_err(|e| anyhow!("malformed invoke request: {e}"))?;
        req.validate()?;
        Ok(req)
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
    /// A success carrying `output` bytes (truncated to [`MAX_OUTPUT_BYTES`] with a flag if larger).
    pub fn success(output: &[u8]) -> Self {
        let (bytes, truncated) = if output.len() > MAX_OUTPUT_BYTES {
            (&output[..MAX_OUTPUT_BYTES], true)
        } else {
            (output, false)
        };
        InvokeResponse {
            ok: true,
            exit_code: Some(0),
            output_hex: hex::encode(bytes),
            error: if truncated {
                Some(format!("output truncated to {MAX_OUTPUT_BYTES} bytes"))
            } else {
                None
            },
        }
    }

    /// A non-zero handler exit carrying its `output` (stdout) and exit `code`.
    pub fn exited(output: &[u8], code: i64) -> Self {
        let bytes = if output.len() > MAX_OUTPUT_BYTES { &output[..MAX_OUTPUT_BYTES] } else { output };
        InvokeResponse {
            ok: code == 0,
            exit_code: Some(code),
            output_hex: hex::encode(bytes),
            error: if code == 0 { None } else { Some(format!("handler exited with code {code}")) },
        }
    }

    /// A failure carrying an error message (truncated to [`MAX_ERROR_BYTES`]).
    pub fn failure(error: impl Into<String>) -> Self {
        let mut msg = error.into();
        if msg.len() > MAX_ERROR_BYTES {
            msg.truncate(MAX_ERROR_BYTES);
        }
        InvokeResponse { ok: false, exit_code: None, output_hex: String::new(), error: Some(msg) }
    }

    /// Decode the response output bytes, rejecting an oversized body before allocating.
    pub fn output(&self) -> Result<Vec<u8>> {
        if self.output_hex.len() / 2 > MAX_OUTPUT_BYTES {
            return Err(anyhow!(
                "invoke output too large: {} bytes (max {})",
                self.output_hex.len() / 2,
                MAX_OUTPUT_BYTES
            ));
        }
        hex::decode(&self.output_hex).map_err(|e| anyhow!("bad output hex: {e}"))
    }

    /// Serialize to reply bytes, erroring rather than emitting empty bytes on failure.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self).map_err(|e| anyhow!("encoding invoke response: {e}"))?;
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(anyhow!("invoke response envelope too large: {} bytes", bytes.len()));
        }
        Ok(bytes)
    }

    /// Parse from reply bytes, rejecting an oversized envelope.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(anyhow!("invoke response envelope too large: {} bytes", bytes.len()));
        }
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

    /// The event data bytes, rejecting an oversized payload before allocating.
    pub fn data(&self) -> Result<Vec<u8>> {
        if self.data_hex.len() / 2 > MAX_PAYLOAD_BYTES {
            return Err(anyhow!(
                "trigger event data too large: {} bytes (max {})",
                self.data_hex.len() / 2,
                MAX_PAYLOAD_BYTES
            ));
        }
        hex::decode(&self.data_hex).map_err(|e| anyhow!("bad event data hex: {e}"))
    }

    /// Serialize for publishing, erroring rather than emitting empty bytes on failure.
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| anyhow!("encoding trigger event: {e}"))
    }

    /// Parse a received event. If the bytes are not a `TriggerEvent`, treat the whole payload as
    /// raw event data (so a function can be triggered by any topic, not just ce-fn-aware ones).
    /// Oversized raw bytes are truncated to [`MAX_PAYLOAD_BYTES`] so a flood event cannot force an
    /// unbounded allocation downstream.
    pub fn decode_lenient(topic: &str, bytes: &[u8]) -> Self {
        match serde_json::from_slice::<TriggerEvent>(bytes) {
            Ok(ev) => ev,
            Err(_) => {
                let capped = if bytes.len() > MAX_PAYLOAD_BYTES { &bytes[..MAX_PAYLOAD_BYTES] } else { bytes };
                TriggerEvent { topic: topic.to_string(), data_hex: hex::encode(capped) }
            }
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
        let bytes = req.encode().unwrap();
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
        let back = InvokeResponse::decode(&ok.encode().unwrap()).unwrap();
        assert_eq!(back, ok);

        let err = InvokeResponse::failure("denied");
        assert!(!err.ok);
        assert_eq!(err.error.as_deref(), Some("denied"));
    }

    #[test]
    fn invoke_response_exited_nonzero() {
        let r = InvokeResponse::exited(b"partial", 3);
        assert!(!r.ok);
        assert_eq!(r.exit_code, Some(3));
        assert_eq!(r.output().unwrap(), b"partial");
        assert!(r.error.as_deref().unwrap().contains("3"));
    }

    #[test]
    fn encode_rejects_oversized_payload() {
        // A payload over the cap must be rejected by encode (and by validate).
        let big = vec![0u8; MAX_PAYLOAD_BYTES + 1];
        let req = InvokeRequest::new("f", &big);
        assert!(req.validate().is_err());
        assert!(req.encode().is_err());
        assert!(req.payload().is_err());
    }

    #[test]
    fn decode_rejects_oversized_envelope() {
        let bytes = vec![b'x'; MAX_ENVELOPE_BYTES + 1];
        assert!(InvokeRequest::decode(&bytes).is_err());
        assert!(InvokeResponse::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_bad_function_name() {
        // A request whose function name is invalid must not decode.
        let raw = serde_json::to_vec(&InvokeRequest {
            function: "Bad Name".into(),
            caps: String::new(),
            payload_hex: String::new(),
            content_type: None,
        })
        .unwrap();
        assert!(InvokeRequest::decode(&raw).is_err());
    }

    #[test]
    fn output_success_truncates_when_oversized() {
        let big = vec![7u8; MAX_OUTPUT_BYTES + 100];
        let r = InvokeResponse::success(&big);
        assert!(r.ok);
        assert_eq!(r.output().unwrap().len(), MAX_OUTPUT_BYTES);
        assert!(r.error.as_deref().unwrap().contains("truncated"));
    }

    #[test]
    fn trigger_event_roundtrip() {
        let ev = TriggerEvent::new("ce-storage/uploads", b"cid123");
        let back = TriggerEvent::decode_lenient("ce-storage/uploads", &ev.encode().unwrap());
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
