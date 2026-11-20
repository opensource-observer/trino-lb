use std::path::PathBuf;

use clap::Parser;

/// Loadbalancer in front of Stackable Trino clusters
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Config file that contains needed information to start trino-lb.
    #[arg(short, long)]
    pub config_file: PathBuf,

    /// Largest query, in bytes, that `POST /v1/statement` accepts. axum's 2 MB default would
    /// reject long generated queries before Trino's own `query.max-length` gets to decide.
    #[arg(long, env = "TRINO_LB_MAX_STATEMENT_BYTES", default_value_t = 10 * 1024 * 1024)]
    pub max_statement_bytes: usize,
}
