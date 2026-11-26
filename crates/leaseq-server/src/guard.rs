//! Request filtering and response hardening.

use axum::extract::{Request, State};
use axum::http::header::{HeaderName, HeaderValue};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;

use crate::error::ApiError;
use crate::{AppState, Options};

const CSP: &str = "default-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

pub(crate) async fn guard(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    check(state.options, request.method(), request.headers())?;

    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    for (name, value) in [
        (header::CONTENT_SECURITY_POLICY, CSP),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        (header::CACHE_CONTROL, "no-store"),
        (header::REFERRER_POLICY, "no-referrer"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    Ok(response)
}

/// Rejects requests that a hostile web page could make the browser send.
fn check(options: Options, method: &Method, headers: &HeaderMap) -> Result<(), ApiError> {
    // An unreadable header becomes empty, which passes neither check.
    let text = |name: HeaderName| {
        headers
            .get(name)
            .map(|value| value.to_str().unwrap_or_default())
    };
    let host = text(header::HOST).unwrap_or_default();
    if !options.allow_remote && !is_loopback_host(host) {
        return Err(forbidden(
            "the Host header must name the loopback interface",
        ));
    }
    let safe = matches!(*method, Method::GET | Method::HEAD);
    match text(header::ORIGIN) {
        Some(origin) if !safe && !same_origin(origin, host) => {
            Err(forbidden("cross-origin requests are not allowed"))
        }
        _ => Ok(()),
    }
}

fn forbidden(message: &str) -> ApiError {
    ApiError::new(StatusCode::FORBIDDEN, "forbidden", message)
}

/// Accepts `localhost`, `127.0.0.1` and `[::1]`, with an optional port.
fn is_loopback_host(host: &str) -> bool {
    ["localhost", "127.0.0.1", "[::1]"].iter().any(|name| {
        host.get(..name.len())
            .is_some_and(|start| start.eq_ignore_ascii_case(name))
            && match &host[name.len()..] {
                "" => true,
                rest => rest
                    .strip_prefix(':')
                    .is_some_and(|port| port.parse::<u16>().is_ok()),
            }
    })
}

fn same_origin(origin: &str, host: &str) -> bool {
    origin
        .split_once("://")
        .is_some_and(|(_, authority)| authority.eq_ignore_ascii_case(host))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_check_rejects_lookalikes() {
        for good in ["localhost", "LOCALHOST:80", "127.0.0.1:7878", "[::1]:1"] {
            assert!(is_loopback_host(good), "{good}");
        }
        for bad in [
            "",
            "example.com",
            "localhost.example.com",
            "127.0.0.1.example.com",
            "localhost:80@example.com",
            "localhost:99999",
            "[::1]x",
        ] {
            assert!(!is_loopback_host(bad), "{bad}");
        }
    }

    #[test]
    fn origin_must_match_host_exactly() {
        assert!(same_origin("http://localhost:1", "localhost:1"));
        assert!(!same_origin("http://localhost:2", "localhost:1"));
        assert!(!same_origin("null", "localhost:1"));
    }
}
