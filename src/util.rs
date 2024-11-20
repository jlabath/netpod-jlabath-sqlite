use crate::Request;
use anyhow::{anyhow, Result};
use bendy::decoding::FromBencode;
use rusqlite::types::Value as SqlValue;
use serde_json::{json, Value as JsonValue};

pub fn sql_to_json(value: SqlValue) -> JsonValue {
    match value {
        SqlValue::Integer(i) => json!(i),
        SqlValue::Real(f) => json!(f),
        SqlValue::Text(s) => JsonValue::String(s),
        SqlValue::Null => JsonValue::Null,
        // Handle any other variants as necessary
        v => {
            eprintln!(
                "ERROR Unhandled sql value in sql_to_json: {:?}",
                v.data_type()
            );
            JsonValue::Null
        } // Or another appropriate default for unsupported types
    }
}

pub fn decode_request(buffer: &[u8]) -> Result<Request> {
    // Check if the last byte is `e` (ASCII value for 'e') which marks dictionary termination
    if buffer[buffer.len() - 1] == b'e' {
        Request::from_bencode(buffer).map_err(|e| anyhow!("{}", e))
    } else {
        Err(anyhow!("keep reading"))
    }
}
