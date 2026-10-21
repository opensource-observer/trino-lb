use reqwest::{Response, header};
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use tracing::instrument;
use trino_lb_core::config::TrinoClusterCredentialsConfig;
use url::Url;
use urlencoding::encode;

use crate::configured_headers::{self, ConfiguredHeaders};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to construct http client"))]
    ConstructHttpClient { source: reqwest::Error },

    #[snafu(display("Invalid Web UI headers"))]
    WebUiHeaders { source: configured_headers::Error },

    #[snafu(display("Failed to join UI login path onto trino endpoint {trino_endpoint}"))]
    JoinUiLoginPathToTrinoEndpoint {
        source: url::ParseError,
        trino_endpoint: Url,
    },

    #[snafu(display("Failed to parse clusterInfo json response"))]
    ParseClusterInfoResponse { source: reqwest::Error },

    #[snafu(display("Failed to log into Trino cluster using endpoint {login_endpoint}"))]
    LogIntoTrinoCluster {
        source: reqwest::Error,
        login_endpoint: Url,
    },

    #[snafu(display(
        "Failed to retrieve stats from Trino cluster using endpoint {stats_endpoint}"
    ))]
    RetrieveStatsFromTrinoCluster {
        source: reqwest::Error,
        stats_endpoint: Url,
    },
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInfo {
    pub running_queries: u64,
    pub blocked_queries: u64,
    pub queued_queries: u64,
    pub active_coordinators: u64,
    pub active_workers: u64,
    pub running_drivers: u64,
    pub total_available_processors: u64,
    pub reserved_memory: f32,
    pub total_input_rows: u64,
    pub total_input_bytes: u64,
    pub total_cpu_time_secs: u64,
}

/// Logs into the Web UI with `credentials`, unless `web_ui_headers` sets `Authorization`, and
/// reads the cluster stats. `web_ui_headers` go with every request.
#[instrument(skip(credentials, web_ui_headers))]
pub async fn get_cluster_info(
    endpoint: &Url,
    ignore_certs: bool,
    credentials: &TrinoClusterCredentialsConfig,
    web_ui_headers: &ConfiguredHeaders,
) -> Result<ClusterInfo, Error> {
    // We create a new client here every time just to be sure we don't accidentally leak the cookie store to a different
    // connection.
    let client = reqwest::Client::builder()
        .cookie_store(true)
        .danger_accept_invalid_certs(ignore_certs)
        .build()
        .context(ConstructHttpClientSnafu)?;
    let headers = web_ui_headers.header_map().context(WebUiHeadersSnafu)?;

    if !web_ui_headers.contains(&header::AUTHORIZATION) {
        let login_endpoint =
            endpoint
                .join("ui/login")
                .context(JoinUiLoginPathToTrinoEndpointSnafu {
                    trino_endpoint: endpoint.clone(),
                })?;
        client
            .post(login_endpoint.clone())
            .headers(headers.clone())
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(login_body(credentials))
            .send()
            .await
            .context(LogIntoTrinoClusterSnafu { login_endpoint })?;
    }

    let stats_endpoint =
        endpoint
            .join("ui/api/stats")
            .context(JoinUiLoginPathToTrinoEndpointSnafu {
                trino_endpoint: endpoint.clone(),
            })?;
    // Checking the status reports a refused login as such, not as an unparsable body.
    let response = client
        .get(stats_endpoint.clone())
        .headers(headers)
        .send()
        .await
        .and_then(Response::error_for_status)
        .context(RetrieveStatsFromTrinoClusterSnafu { stats_endpoint })?;

    response.json().await.context(ParseClusterInfoResponseSnafu)
}

fn login_body(credentials: &TrinoClusterCredentialsConfig) -> String {
    format!(
        "username={}&password={}&redirectPath=",
        encode(&credentials.username),
        encode(&credentials.password),
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf};

    use rstest::rstest;
    use serde_json::json;
    use trino_lb_core::config::HeaderValue;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    use super::*;

    #[rstest]
    #[case("admin", "admin", "username=admin&password=admin&redirectPath=")]
    #[case(
        "foo",
        "superSecure!1&2;3",
        "username=foo&password=superSecure%211%262%3B3&redirectPath="
    )]
    fn test_login_body(
        #[case] username: String,
        #[case] password: String,
        #[case] expected: String,
    ) {
        let credentials = TrinoClusterCredentialsConfig { username, password };

        assert_eq!(login_body(&credentials), expected);
    }

    /// Deletes the token file when the test ends.
    struct TokenFile(PathBuf);

    impl Drop for TokenFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn token_file(token: &str) -> TokenFile {
        let path = std::env::temp_dir().join(format!(
            "trino-lb-web-ui-token-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::write(&path, token).unwrap();
        TokenFile(path)
    }

    fn bearer_headers(token: &TokenFile) -> ConfiguredHeaders {
        ConfiguredHeaders::new(&HashMap::from([
            (
                "Authorization".to_owned(),
                HeaderValue::FromFile {
                    path: token.0.clone(),
                    prefix: "Bearer ".to_owned(),
                },
            ),
            (
                "X-Forwarded-Proto".to_owned(),
                HeaderValue::Value("https".to_owned()),
            ),
        ]))
        .unwrap()
    }

    fn stats_body() -> serde_json::Value {
        json!({
            "runningQueries": 2,
            "blockedQueries": 1,
            "queuedQueries": 3,
            "activeCoordinators": 1,
            "activeWorkers": 1,
            "runningDrivers": 0,
            "totalAvailableProcessors": 4,
            "reservedMemory": 0.0,
            "totalInputRows": 0,
            "totalInputBytes": 0,
            "totalCpuTimeSecs": 0
        })
    }

    async fn mount_login(server: &MockServer, expected_calls: u64) {
        Mock::given(method("POST"))
            .and(path("/ui/login"))
            .respond_with(ResponseTemplate::new(204))
            .expect(expected_calls)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn bearer_token_replaces_the_login_and_is_reread_on_every_poll() {
        let server = MockServer::start().await;
        mount_login(&server, 0).await;
        for token in ["old", "new"] {
            Mock::given(method("GET"))
                .and(path("/ui/api/stats"))
                .and(header("authorization", format!("Bearer {token}")))
                .and(header("x-forwarded-proto", "https"))
                .respond_with(ResponseTemplate::new(200).set_body_json(stats_body()))
                .expect(1)
                .mount(&server)
                .await;
        }
        let token = token_file("old\n");
        let headers = bearer_headers(&token);
        let endpoint = server.uri().parse().unwrap();
        let credentials = TrinoClusterCredentialsConfig::default();

        let info = get_cluster_info(&endpoint, false, &credentials, &headers)
            .await
            .unwrap();
        assert_eq!(info.running_queries, 2);
        assert_eq!(info.queued_queries, 3);

        std::fs::write(&token.0, "new").unwrap();
        get_cluster_info(&endpoint, false, &credentials, &headers)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn logs_in_without_an_authorization_header() {
        let server = MockServer::start().await;
        mount_login(&server, 1).await;
        Mock::given(method("GET"))
            .and(path("/ui/api/stats"))
            .respond_with(ResponseTemplate::new(200).set_body_json(stats_body()))
            .expect(1)
            .mount(&server)
            .await;

        get_cluster_info(
            &server.uri().parse().unwrap(),
            false,
            &TrinoClusterCredentialsConfig::default(),
            &ConfiguredHeaders::default(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn refused_stats_request_is_a_retrieve_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ui/api/stats"))
            .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
            .mount(&server)
            .await;
        let token = token_file("expired");

        let err = get_cluster_info(
            &server.uri().parse().unwrap(),
            false,
            &TrinoClusterCredentialsConfig::default(),
            &bearer_headers(&token),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, Error::RetrieveStatsFromTrinoCluster { .. }),
            "{err:?}"
        );
    }
}
