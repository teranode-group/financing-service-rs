use actix_web::{http::header, HttpRequest, HttpResponse};

use crate::{responses::unauthorized_response, service::Service};

const BEARER_PREFIX: &str = "Bearer ";
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

pub fn extract_api_key(req: &HttpRequest) -> Option<String> {
    if let Some(auth_header) = req.headers().get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix(BEARER_PREFIX) {
                if !token.is_empty() {
                    return Some(token.to_string());
                }
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
    use super::{authorize_admin, authorize_client, extract_api_key, BEARER_PREFIX};
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
            .insert_header(("Authorization", format!("{BEARER_PREFIX}secret")))
            .to_http_request();
        assert_eq!(extract_api_key(&req).as_deref(), Some("secret"));
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
            .insert_header(("Authorization", format!("{BEARER_PREFIX}admin-secret")))
            .to_http_request();

        assert!(authorize_admin(&req, &service).is_none());
    }
}
