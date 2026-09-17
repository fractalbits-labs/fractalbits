use cmd_lib::run_fun;
use std::io::Error;

use chrono::Utc;
use uuid::Uuid;
use xtask_common::{
    BootstrapClusterConfig, ClusterAwsConfig, ClusterEtcdConfig, ClusterGlobalConfig,
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

    let aws_config = ClusterAwsConfig {
        // The bucket name is fixed up front (CDK creates it under that name), so the gateway
        // config can reference it before the stack exists.
        data_blob_bucket: if vpc_config.data_blob_storage.uses_s3_volume() {
            Some(super::super::common::get_data_blob_bucket_name()?)
        } else {
            None
        },
    };

    let config = BootstrapClusterConfig {
        global: ClusterGlobalConfig {
            deploy_target: xtask_common::DeployTarget::Aws,
            region,
            for_bench: vpc_config.with_bench,
            data_blob_storage: vpc_config.data_blob_storage,
            rss_ha_enabled: vpc_config.root_server_ha,
            rss_backend: vpc_config.rss_backend,
            num_nss_nodes: Some(1), // CDK creates nss-0 only
            num_bss_nodes: Some(vpc_config.num_bss_nodes as usize),
            num_s3_gateways: Some(vpc_config.num_s3_gateways as usize),
            num_bench_clients: if vpc_config.with_bench {
                Some(vpc_config.num_bench_clients as usize)
            } else {
                None
            },
            workflow_cluster_id: Some(workflow_cluster_id),
            meta_stack_testing: false,
            use_generic_binaries: vpc_config.use_generic_binaries,
            journal_uuid: Some(journal_uuid),
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
