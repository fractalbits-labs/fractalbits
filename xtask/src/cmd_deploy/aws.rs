mod config_gen;
mod vpc;

pub use vpc::{create_vpc, destroy_vpc};

use cmd_lib::*;

/// Get the bootstrap bucket name for AWS: fractalbits-bootstrap-{region}-{account}
pub fn get_aws_bootstrap_bucket() -> Result<String, std::io::Error> {
    let region = run_fun!(aws configure get region)?;
    let account_id = run_fun!(aws sts get-caller-identity --query Account --output text)?;
    Ok(format!(
        "fractalbits-bootstrap-{}-{}",
        region.trim(),
        account_id.trim()
    ))
}
