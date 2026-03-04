use crate::CmdResult;
use cmd_lib::*;
use std::io::Error;

use super::ssm_utils;

const SSM_BATCH_SIZE: usize = 50;

pub fn ssm_bootstrap_from_docker(instance_ids: &[String], docker_host_ip: &str) -> CmdResult {
    info!(
        "Bootstrapping {} instances via SSM (Docker S3 at {})",
        instance_ids.len(),
        docker_host_ip
    );

    let bootstrap_command = format!(
        "export AWS_DEFAULT_REGION=localdev && \
         export AWS_ENDPOINT_URL_S3=http://{docker_host_ip}:8080 && \
         export AWS_ACCESS_KEY_ID=test_api_key && \
         export AWS_SECRET_ACCESS_KEY=test_api_secret && \
         aws s3 cp --no-progress s3://fractalbits-bootstrap/bootstrap.sh - | bash 2>&1 | \
         tee -a /var/log/cloud-init-output.log"
    );

    send_batched_ssm_commands(
        instance_ids,
        &bootstrap_command,
        "Bootstrap fractalbits (Docker)",
    )?;

    info!("All instances bootstrapped successfully via SSM (Docker S3)");

    Ok(())
}

fn send_batched_ssm_commands(
    instance_ids: &[String],
    command: &str,
    description_prefix: &str,
) -> Result<(), Error> {
    let batches: Vec<&[String]> = instance_ids.chunks(SSM_BATCH_SIZE).collect();

    info!(
        "Sending SSM commands in {} batches (max {} instances per batch)",
        batches.len(),
        SSM_BATCH_SIZE
    );

    let mut command_results: Vec<(String, Vec<String>)> = Vec::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        let batch_vec: Vec<String> = batch.to_vec();
        let description = format!(
            "{} (batch {}/{})",
            description_prefix,
            batch_idx + 1,
            batches.len()
        );
        let command_id = ssm_utils::ssm_send_command(&batch_vec, command, &description)?;
        info!(
            "SSM command sent for batch {}/{} with ID: {} ({} instances)",
            batch_idx + 1,
            batches.len(),
            command_id,
            batch.len()
        );
        command_results.push((command_id, batch_vec));
    }

    for (command_id, ids) in &command_results {
        ssm_utils::wait_for_ssm_command(command_id, ids)?;
    }

    Ok(())
}
