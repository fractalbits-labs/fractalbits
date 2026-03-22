mod config_gen;
mod utils;
mod vpc;

pub use vpc::{create_vpc, destroy_vpc};

/// Resolve GCP project ID from: CLI arg > GCP_PROJECT_ID env var
pub fn resolve_gcp_project(cli_arg: Option<&str>) -> Result<String, std::io::Error> {
    if let Some(p) = cli_arg.filter(|s| !s.is_empty()) {
        return Ok(p.to_string());
    }
    if let Ok(p) = std::env::var("GCP_PROJECT_ID")
        && !p.is_empty()
    {
        return Ok(p);
    }
    Err(std::io::Error::other(
        "GCP project ID required. Set via --gcp-project or GCP_PROJECT_ID env var",
    ))
}

/// Resolve GCP zone from: CLI arg > GCP_ZONE env > default us-central1-a
pub fn resolve_gcp_zone(cli_arg: Option<&str>) -> String {
    if let Some(z) = cli_arg.filter(|s| !s.is_empty()) {
        return z.to_string();
    }
    std::env::var("GCP_ZONE").unwrap_or_else(|_| "us-central1-a".to_string())
}
