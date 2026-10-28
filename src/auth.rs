use actix_web::{http::header, HttpRequest, HttpResponse};

use crate::{responses::unauthorized_response, service::Service};

const BEARER_SCHEME: &str = "Bearer";
const API_KEY_HEADER: &str = "X-API-Key";

pub fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// The key a request carries: `Authorization: Bearer <key>`, or
/// `X-API-Key: <key>`.
///
/// The scheme is matched ignoring case, as RFC 9110 says it is and as
/// mapi-lite matches it (CS-623): `bearer <key>` used to be refused (CS-663).
/// Any number of spaces may follow it (RFC 9110's `1*SP`); a key cannot
/// contain one. The key itself is compared exactly.
///
/// A Bearer `Authorization` header is read first, so it wins over
/// `X-API-Key`: a request carrying both is judged on the Bearer key alone.
pub fn extract_api_key(req: &HttpRequest) -> Option<String> {
    if let Some(auth_header) = req.headers().get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_header.to_str() {
            let token = auth_str
                .split_once(' ')
                .filter(|(scheme, _)| scheme.eq_ignore_ascii_case(BEARER_SCHEME))
                .map(|(_, token)| token.trim_start_matches(' '));
            if let Some(token) = token.filter(|t| !t.is_empty()) {
                return Some(token.to_string());
            }
        }
    }

    if let Some(key_header) = req.headers().get(API_KEY_HEADER) {
        if let Ok(key_str) = key_header.to_str() {
            if !key_str.is_empty() {
                return Some(key_str.to_string());
            }
        }
    }

    None
}

/// Returns `Some(HttpResponse)` when the request is unauthorized.
pub async fn authorize_client(
    req: &HttpRequest,
    client_id: &str,
    service: &Service,
) -> Option<HttpResponse> {
    if !service.client_auth_required(client_id).await {
        return None;
    }

    match extract_api_key(req) {
        Some(key) if service.verify_client_api_key(client_id, &key).await => None,
        _ => Some(unauthorized_response()),
    }
}

/// [`authorize_client`] for a `client_id` that may not exist, answering
/// `unknown` for one that does not only where it gives nothing away.
///
/// Checking the id first answered an anonymous caller 401 for a real client
/// and 404 for an unknown one, so anyone could find out which client ids
/// exist (CS-661). Once any client has an API key, an unknown id is answered
/// exactly as a missing or wrong key is: there is no key it could match. With
/// no client keys at all nothing is authenticated, nothing is hidden, and
/// `unknown_client` is the more useful answer.
///
/// The caller is told nothing, so the reason goes to the log: a mistyped
/// `client_id` otherwise looks exactly like a wrong key from both sides. It
/// is `info`, not `warn`, so someone probing for ids does not flood the log,
/// and `{:?}` because the id is the caller's.
pub async fn authorize_known_client(
    req: &HttpRequest,
    client_id: &str,
    service: &Service,
    unknown: impl FnOnce() -> HttpResponse,
) -> Option<HttpResponse> {
    if service.is_client_id_valid(client_id).await {
        return authorize_client(req, client_id, service).await;
    }
    Some(if service.any_client_has_api_key().await {
        log::info!("unauthorized: unknown client_id {client_id:?}");
        unauthorized_response()
    } else {
        unknown()
    })
}

/// Returns `Some(HttpResponse)` when the admin request is unauthorized.
pub fn authorize_admin(req: &HttpRequest, service: &Service) -> Option<HttpResponse> {
    if !service.admin_auth_required() {
        return None;
    }

    match extract_api_key(req) {
        Some(key) if service.verify_admin_api_key(&key) => None,
        _ => Some(unauthorized_response()),
    }
}

#[cfg(test)]
mod unit_tests {
    use super::constant_time_eq;

    #[test]
    fn constant_time_eq_matches_equal_strings() {
        assert!(constant_time_eq("secret", "secret"));
    }

    #[test]
    fn constant_time_eq_rejects_different_strings() {
        assert!(!constant_time_eq("secret", "other"));
        assert!(!constant_time_eq("secret", "secre"));
    }
}

#[cfg(test)]
mod tests {
    use super::{authorize_admin, authorize_client, extract_api_key, BEARER_SCHEME};
    use actix_web::{http::StatusCode, test};

    use crate::{
        service::Service,
        test_support::{
            test_blockchain_interface, test_config_with_api_key, test_config_with_keys,
            unique_dynamic_config_path, TEST_CLIENT_ID,
        },
    };

    #[actix_web::test]
    async fn extract_api_key_from_bearer_header() {
        let req = test::TestRequest::get()
            .insert_header(("Authorization", format!("{BEARER_SCHEME} secret")))
            .to_http_request();
        assert_eq!(extract_api_key(&req).as_deref(), Some("secret"));
    }

    /// CS-663: the scheme in any case is Bearer (RFC 9110), followed by one
    /// space or more; another scheme, or none, carries no key.
    #[actix_web::test]
    async fn extract_api_key_matches_the_scheme_ignoring_case() {
        for scheme in ["Bearer", "bearer", "BEARER", "bEaReR"] {
            let req = test::TestRequest::get()
                .insert_header(("Authorization", format!("{scheme} secret")))
                .to_http_request();
            assert_eq!(extract_api_key(&req).as_deref(), Some("secret"), "{scheme}");
        }
        for value in ["Bearer  secret", "Bearer   secret"] {
            let req = test::TestRequest::get()
                .insert_header(("Authorization", value))
                .to_http_request();
            assert_eq!(extract_api_key(&req).as_deref(), Some("secret"), "{value}");
        }
        for value in [
            "Basic secret",
            "Bearersecret",
            "secret",
            "Bearer ",
            "Bearer   ",
        ] {
            let req = test::TestRequest::get()
                .insert_header(("Authorization", value))
                .to_http_request();
            assert_eq!(extract_api_key(&req), None, "{value}");
        }
    }

    #[actix_web::test]
    async fn extract_api_key_from_x_api_key_header() {
        let req = test::TestRequest::get()
            .insert_header(("X-API-Key", "secret"))
            .to_http_request();
        assert_eq!(extract_api_key(&req).as_deref(), Some("secret"));
    }

    #[actix_web::test]
    async fn authorize_client_allows_when_no_key_configured() {
        let config = test_config_with_api_key(&unique_dynamic_config_path(), None);
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        let req = test::TestRequest::get().to_http_request();

        assert!(authorize_client(&req, TEST_CLIENT_ID, &service)
            .await
            .is_none());
    }

    #[actix_web::test]
    async fn authorize_client_rejects_missing_key() {
        let config = test_config_with_api_key(&unique_dynamic_config_path(), Some("secret"));
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        let req = test::TestRequest::get().to_http_request();

        let resp = authorize_client(&req, TEST_CLIENT_ID, &service)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn authorize_client_accepts_matching_key() {
        let config = test_config_with_api_key(&unique_dynamic_config_path(), Some("secret"));
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        let req = test::TestRequest::get()
            .insert_header(("X-API-Key", "secret"))
            .to_http_request();

        assert!(authorize_client(&req, TEST_CLIENT_ID, &service)
            .await
            .is_none());
    }

    #[actix_web::test]
    async fn authorize_client_rejects_wrong_client_key() {
        let config = test_config_with_api_key(&unique_dynamic_config_path(), Some("secret-a"));
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        let req = test::TestRequest::get()
            .insert_header(("X-API-Key", "secret-b"))
            .to_http_request();

        let resp = authorize_client(&req, TEST_CLIENT_ID, &service)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn authorize_admin_allows_when_no_key_configured() {
        let config = test_config_with_keys(&unique_dynamic_config_path(), None, None);
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        let req = test::TestRequest::get().to_http_request();

        assert!(authorize_admin(&req, &service).is_none());
    }

    #[actix_web::test]
    async fn authorize_admin_rejects_missing_key() {
        let config =
            test_config_with_keys(&unique_dynamic_config_path(), None, Some("admin-secret"));
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        let req = test::TestRequest::get().to_http_request();

        let resp = authorize_admin(&req, &service).unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn authorize_admin_accepts_matching_key() {
        let config =
            test_config_with_keys(&unique_dynamic_config_path(), None, Some("admin-secret"));
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        let req = test::TestRequest::get()
            .insert_header(("Authorization", format!("{BEARER_SCHEME} admin-secret")))
            .to_http_request();

        assert!(authorize_admin(&req, &service).is_none());
    }
}
