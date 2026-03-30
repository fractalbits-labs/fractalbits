use cmd_lib::run_fun;
use std::io::Error;

use chrono::Utc;
use uuid::Uuid;
use xtask_common::{
    BootstrapClusterConfig, ClusterAwsConfig, ClusterEtcdConfig, ClusterGlobalConfig,
    DataBlobStorage,
};

use super::super::common::VpcConfig;

/// Generate a global-only BootstrapClusterConfig before CDK deploy.
///
/// Only static parameters are included — no instance IDs, no NSS endpoint,
/// no per-node data. Each instance gets its role via `--role` CLI arg in UserData.
pub fn generate_bootstrap_config(vpc_config: &VpcConfig) -> Result<BootstrapClusterConfig, Error> {
    let region = run_fun!(aws configure get region)?;

    let workflow_cluster_id = Utc::now().format("%Y%m%d-%H%M%S").to_string();

    // Pre-generate a cluster-scoped journal UUID for NSS (embedded in UserData)
    let journal_uuid = Uuid::now_v7().to_string();

    // Query the first AZ in the region (used for S3 Express / EBS placement)
    let local_az = run_fun! {
        aws ec2 describe-availability-zones
            --region $region --query "AvailabilityZones[0].ZoneName" --output text
    }?;

    let aws_config = ClusterAwsConfig {
        // data_blob_bucket: for AllInBss it's unused; for S3Hybrid it comes from CDK output
        // and is not pre-knowable. Leave None — API server reads it from DDB service discovery.
        data_blob_bucket: None,
        local_az,
        remote_az: None,
    };

    let config = BootstrapClusterConfig {
        global: ClusterGlobalConfig {
            deploy_target: xtask_common::DeployTarget::Aws,
            region,
            for_bench: vpc_config.with_bench,
            data_blob_storage: DataBlobStorage::AllInBssSingleAz,
            rss_ha_enabled: vpc_config.root_server_ha,
            rss_backend: vpc_config.rss_backend,
            journal_type: vpc_config.journal_type,
            num_nss_nodes: Some(2), // CDK always creates nss-A and nss-B unconditionally
            num_bss_nodes: Some(vpc_config.num_bss_nodes as usize),
            num_api_servers: Some(vpc_config.num_api_servers as usize),
            num_bench_clients: if vpc_config.with_bench {
                Some(vpc_config.num_bench_clients as usize)
            } else {
                None
            },
            workflow_cluster_id: Some(workflow_cluster_id),
            meta_stack_testing: false,
            use_generic_binaries: vpc_config.use_generic_binaries,
            journal_uuid: Some(journal_uuid),
            storage_alloc_mode: vpc_config.storage_alloc_mode,
        },
        aws: Some(aws_config),
        gcp: None,
        endpoints: None,
        resources: None,
        etcd: if vpc_config.rss_backend == crate::RssBackend::Etcd {
            Some(ClusterEtcdConfig {
                enabled: true,
                cluster_size: vpc_config.num_bss_nodes as usize,
                endpoints: None,
            })
        } else {
            None
        },
        nodes: std::collections::HashMap::new(),
        bootstrap_bucket: super::super::common::get_bootstrap_bucket_name(
            xtask_common::DeployTarget::Aws,
        )?,
    };

    Ok(config)
}
