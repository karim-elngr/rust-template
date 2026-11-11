use platform::capture_trace_context;
use rdkafka::message::{Header, Headers, OwnedHeaders};
use std::collections::BTreeMap;

pub(super) fn trace_carrier<H: Headers>(
    headers: Option<&H>,
    fallback: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let carrier: BTreeMap<String, String> = headers
        .into_iter()
        .flat_map(Headers::iter)
        .filter(|header| matches!(header.key, "traceparent" | "tracestate"))
        .filter_map(|header| {
            let value = header.value?;
            if value.len() > 1024 {
                return None;
            }
            let value = std::str::from_utf8(value).ok()?;
            Some((header.key.to_owned(), value.to_owned()))
        })
        .collect();
    if carrier.contains_key("traceparent") {
        carrier
    } else {
        fallback.clone()
    }
}

pub(super) fn publisher_headers() -> OwnedHeaders {
    capture_trace_context()
        .iter()
        .filter(|(key, _)| matches!(key.as_str(), "traceparent" | "tracestate"))
        .fold(OwnedHeaders::new(), |headers, (key, value)| {
            headers.insert(Header {
                key,
                value: Some(value.as_str()),
            })
        })
}
