use std::collections::HashMap;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue, InvalidHeaderName, InvalidHeaderValue};
use snafu::{ResultExt, Snafu};
use trino_lb_core::config::{self, ReadHeaderValueError};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Invalid header name {name:?}"))]
    InvalidHeaderName {
        source: InvalidHeaderName,
        name: String,
    },

    #[snafu(display("Failed to read the value of header {name}"))]
    ReadHeaderValue {
        source: ReadHeaderValueError,
        name: HeaderName,
    },

    #[snafu(display("Invalid value for header {name}"))]
    InvalidHeaderValue {
        source: InvalidHeaderValue,
        name: HeaderName,
    },
}

/// Headers from the config, sent with outgoing requests. Values read from a file are re-read on
/// every [`ConfiguredHeaders::header_map`] call, so a rotated credential is picked up.
#[derive(Debug, Default)]
pub struct ConfiguredHeaders(Vec<(HeaderName, config::HeaderValue)>);

impl ConfiguredHeaders {
    pub fn new(headers: &HashMap<String, config::HeaderValue>) -> Result<Self, Error> {
        headers
            .iter()
            .map(|(name, value)| {
                let name = HeaderName::try_from(name).context(InvalidHeaderNameSnafu { name })?;
                Ok((name, value.clone()))
            })
            .collect::<Result<_, _>>()
            .map(Self)
    }

    pub fn contains(&self, name: &HeaderName) -> bool {
        self.0.iter().any(|(n, _)| n == name)
    }

    /// The headers' current values, all marked sensitive so they are never logged.
    pub fn header_map(&self) -> Result<HeaderMap, Error> {
        let mut map = HeaderMap::with_capacity(self.0.len());
        for (name, value) in &self.0 {
            let value = value
                .read()
                .context(ReadHeaderValueSnafu { name: name.clone() })?;
            let mut value = HeaderValue::try_from(value)
                .context(InvalidHeaderValueSnafu { name: name.clone() })?;
            value.set_sensitive(true);
            map.insert(name.clone(), value);
        }
        Ok(map)
    }
}
