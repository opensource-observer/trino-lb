use std::{path::PathBuf, time::Duration};

use reqwest::{
    Client, Method, Response,
    header::{HeaderName, HeaderValue, InvalidHeaderName, InvalidHeaderValue},
};
use serde::Deserialize;
use serde_json::json;
use snafu::{ResultExt, Snafu};
use tracing::{Instrument, debug_span, instrument};
use trino_lb_core::{
    TrinoClusterName,
    config::{RestScalerConfig, RestScalerHeaderValue},
};
use url::Url;

use super::ScalerTrait;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to construct http client"))]
    ConstructHttpClient { source: reqwest::Error },

    #[snafu(display("Invalid header name {name:?}"))]
    InvalidHeaderName {
        source: InvalidHeaderName,
        name: String,
    },

    #[snafu(display("Failed to read the value of header {name} from {path:?}"))]
    ReadHeaderFile {
        source: std::io::Error,
        name: HeaderName,
        path: PathBuf,
    },

    #[snafu(display("Invalid value for header {name}"))]
    InvalidHeaderValue {
        source: InvalidHeaderValue,
        name: HeaderName,
    },

    #[snafu(display("Failed to build the scaler API URL from endpoint {endpoint}"))]
    BuildClustersUrl {
        source: url::ParseError,
        endpoint: Url,
    },

    #[snafu(display("Request to the scaler API at {url} failed"))]
    ScalerApiRequest { source: reqwest::Error, url: Url },
}

#[derive(Debug, Deserialize)]
struct ClusterStatus {
    activated: bool,
    ready: bool,
}

pub struct RestScaler {
    client: Client,
    clusters_url: Url,
    headers: Vec<(HeaderName, RestScalerHeaderValue)>,
}

impl RestScaler {
    // Intentionally including the config here, this is only logged on startup
    #[instrument(name = "RestScaler::new")]
    pub fn new(config: &RestScalerConfig) -> Result<Self, Error> {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context(ConstructHttpClientSnafu)?;

        let headers = config
            .headers
            .iter()
            .map(|(name, value)| {
                let name = HeaderName::try_from(name).context(InvalidHeaderNameSnafu { name })?;
                Ok((name, value.clone()))
            })
            .collect::<Result<_, Error>>()?;

        let clusters_url = config
            .endpoint
            .join("v1/clusters/")
            .context(BuildClustersUrlSnafu {
                endpoint: config.endpoint.clone(),
            })?;

        Ok(Self {
            client,
            clusters_url,
            headers,
        })
    }

    fn header_value(
        name: &HeaderName,
        value: &RestScalerHeaderValue,
    ) -> Result<HeaderValue, Error> {
        let value = match value {
            RestScalerHeaderValue::Value(value) => value.clone(),
            RestScalerHeaderValue::FromFile { path, prefix } => {
                let content = std::fs::read_to_string(path).context(ReadHeaderFileSnafu {
                    name: name.clone(),
                    path,
                })?;
                format!("{prefix}{}", content.trim())
            }
        };
        let mut value =
            HeaderValue::try_from(value).context(InvalidHeaderValueSnafu { name: name.clone() })?;
        value.set_sensitive(true);
        Ok(value)
    }

    async fn request(
        &self,
        method: Method,
        cluster: &TrinoClusterName,
        body: Option<serde_json::Value>,
    ) -> Result<Response, Error> {
        let mut url = self.clusters_url.clone();
        url.path_segments_mut()
            // `join` succeeding in `new` means the URL can be a base, so it has path segments.
            .expect("clusters URL has path segments")
            .pop_if_empty()
            .push(cluster);
        let mut request = self.client.request(method, url.clone());
        for (name, value) in &self.headers {
            request = request.header(name, Self::header_value(name, value)?);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        request
            .send()
            .instrument(debug_span!("Calling scaler API"))
            .await
            .and_then(Response::error_for_status)
            .context(ScalerApiRequestSnafu { url })
    }

    async fn set_activated(
        &self,
        cluster: &TrinoClusterName,
        activated: bool,
    ) -> Result<(), Error> {
        self.request(
            Method::PATCH,
            cluster,
            Some(json!({ "activated": activated })),
        )
        .await?;
        Ok(())
    }

    /// Fails rather than guessing: a guessed "not activated" would make trino-lb deactivate a
    /// cluster that is still draining queries.
    async fn status(&self, cluster: &TrinoClusterName) -> Result<ClusterStatus, Error> {
        let response = self.request(Method::GET, cluster, None).await?;
        let url = response.url().clone();
        response.json().await.context(ScalerApiRequestSnafu { url })
    }
}

impl ScalerTrait for RestScaler {
    #[instrument(name = "RestScaler::activate", skip(self))]
    async fn activate(&self, cluster: &TrinoClusterName) -> Result<(), super::Error> {
        Ok(self.set_activated(cluster, true).await?)
    }

    #[instrument(name = "RestScaler::deactivate", skip(self))]
    async fn deactivate(&self, cluster: &TrinoClusterName) -> Result<(), super::Error> {
        Ok(self.set_activated(cluster, false).await?)
    }

    #[instrument(name = "RestScaler::is_ready", skip(self))]
    async fn is_ready(&self, cluster: &TrinoClusterName) -> Result<bool, super::Error> {
        Ok(self.status(cluster).await?.ready)
    }

    #[instrument(name = "RestScaler::is_activated", skip(self))]
    async fn is_activated(&self, cluster: &TrinoClusterName) -> Result<bool, super::Error> {
        Ok(self.status(cluster).await?.activated)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, header, method, path},
    };

    use super::*;

    /// Deletes the token file when the test ends.
    struct TokenFile(PathBuf);

    impl Drop for TokenFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn scaler(server: &MockServer, token: &str) -> (RestScaler, TokenFile) {
        let token_path = std::env::temp_dir().join(format!(
            "trino-lb-rest-scaler-token-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::write(&token_path, token).unwrap();
        let config = RestScalerConfig {
            endpoint: server.uri().parse().unwrap(),
            headers: [(
                "Authorization".to_owned(),
                RestScalerHeaderValue::FromFile {
                    path: token_path.clone(),
                    prefix: "Bearer ".to_owned(),
                },
            )]
            .into(),
        };
        (RestScaler::new(&config).unwrap(), TokenFile(token_path))
    }

    fn cluster() -> TrinoClusterName {
        "tier-1-0".to_owned()
    }

    fn status_body(activated: bool, ready: bool) -> serde_json::Value {
        json!({ "activated": activated, "ready": ready })
    }

    // Only the expected PATCH is mounted, so sending the wrong `activated` value hits wiremock's
    // default 404 and fails.
    #[rstest]
    #[case::activate(true)]
    #[case::deactivate(false)]
    #[tokio::test]
    async fn patches_the_cluster(#[case] activated: bool) {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/v1/clusters/tier-1-0"))
            .and(header("authorization", "Bearer secret"))
            .and(body_json(json!({ "activated": activated })))
            .respond_with(ResponseTemplate::new(200).set_body_json(status_body(activated, false)))
            .expect(1)
            .mount(&server)
            .await;
        let (scaler, _token) = scaler(&server, "secret\n");

        if activated {
            scaler.activate(&cluster()).await.unwrap();
        } else {
            scaler.deactivate(&cluster()).await.unwrap();
        }
    }

    #[tokio::test]
    async fn activate_fails_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let (scaler, _token) = scaler(&server, "secret");

        assert!(scaler.activate(&cluster()).await.is_err());
        assert!(scaler.deactivate(&cluster()).await.is_err());
    }

    #[rstest]
    #[case(true, false)]
    #[case(false, true)]
    #[tokio::test]
    async fn status_maps_to_bools(#[case] activated: bool, #[case] ready: bool) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/clusters/tier-1-0"))
            .and(header("authorization", "Bearer secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(status_body(activated, ready)))
            .mount(&server)
            .await;
        let (scaler, _token) = scaler(&server, "secret");

        assert_eq!(scaler.is_activated(&cluster()).await.unwrap(), activated);
        assert_eq!(scaler.is_ready(&cluster()).await.unwrap(), ready);
    }

    // A status-shaped body, so only the status code can make this fail.
    #[tokio::test]
    async fn status_fails_on_503() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503).set_body_json(status_body(true, true)))
            .mount(&server)
            .await;
        let (scaler, _token) = scaler(&server, "secret");

        assert!(scaler.is_activated(&cluster()).await.is_err());
        assert!(scaler.is_ready(&cluster()).await.is_err());
    }

    #[tokio::test]
    async fn status_fails_on_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(status_body(true, true))
                    .set_delay(REQUEST_TIMEOUT + Duration::from_secs(1)),
            )
            .mount(&server)
            .await;
        let (scaler, _token) = scaler(&server, "secret");

        assert!(scaler.is_ready(&cluster()).await.is_err());
    }

    #[tokio::test]
    async fn cluster_name_is_percent_encoded() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/clusters/a%2Fb"))
            .respond_with(ResponseTemplate::new(200).set_body_json(status_body(true, true)))
            .expect(1)
            .mount(&server)
            .await;
        let (scaler, _token) = scaler(&server, "secret");

        assert!(scaler.is_ready(&"a/b".to_owned()).await.unwrap());
    }

    #[tokio::test]
    async fn literal_header_is_sent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("x-api-key", "abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(status_body(true, true)))
            .expect(1)
            .mount(&server)
            .await;
        let config = RestScalerConfig {
            endpoint: server.uri().parse().unwrap(),
            headers: [(
                "X-Api-Key".to_owned(),
                RestScalerHeaderValue::Value("abc".to_owned()),
            )]
            .into(),
        };

        assert!(
            RestScaler::new(&config)
                .unwrap()
                .is_ready(&cluster())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn rotated_token_is_used_on_next_call() {
        let server = MockServer::start().await;
        for token in ["old", "new"] {
            Mock::given(method("PATCH"))
                .and(header("authorization", format!("Bearer {token}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(status_body(true, false)))
                .expect(1)
                .mount(&server)
                .await;
        }
        let (scaler, token) = scaler(&server, "old");

        scaler.activate(&cluster()).await.unwrap();
        std::fs::write(&token.0, "new").unwrap();
        scaler.activate(&cluster()).await.unwrap();
    }
}
