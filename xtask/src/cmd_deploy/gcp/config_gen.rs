use std::collections::HashMap;
use std::io::Error;

use chrono::Utc;
use uuid::Uuid;
use xtask_common::{
    BootstrapClusterConfig, ClusterEtcdConfig, ClusterGcpConfig, ClusterGlobalConfig,
    DataBlobStorage,
};

pub struct GcpDeployParams<'a> {
    pub project_id: &'a str,
    pub zone: &'a str,
    pub region: &'a str,
    pub rss_backend: xtask_common::RssBackend,
    pub rss_ha_enabled: bool,
    pub num_bss_nodes: usize,
    pub num_api_servers: usize,
    pub num_bench_clients: usize,
    pub with_bench: bool,
    pub use_generic_binaries: bool,
    pub bss_storage_alloc_mode: xtask_common::BssStorageAllocMode,
}

/// Generate a global-only BootstrapClusterConfig before Terraform apply.
///
/// Only static parameters are included — no instance names/IPs, no NSS endpoint,
/// no per-node data. Each instance gets its role via `--role` in its startup script.
pub fn generate_bootstrap_config(
    params: &GcpDeployParams,
) -> Result<BootstrapClusterConfig, Error> {
    let workflow_cluster_id = Utc::now().format("%Y%m%d-%H%M%S").to_string();

    // Pre-generate a cluster-scoped journal UUID for NSS (embedded in startup script)
    let journal_uuid = Uuid::now_v7().to_string();

    let gcp_config = ClusterGcpConfig {
        project_id: params.project_id.to_string(),
        zone: params.zone.to_string(),
        remote_zone: None,
        // Network/subnetwork names are deterministic in our Terraform config
        network: "fractalbits-vpc".to_string(),
        subnetwork: "fractalbits-subnet".to_string(),
        service_account: String::new(), // populated post-deploy if needed
        firestore_database: if params.rss_backend == xtask_common::RssBackend::Firestore {
            Some("fractalbits".to_string())
        } else {
            None
        },
    };

    let config = BootstrapClusterConfig {
        global: ClusterGlobalConfig {
            deploy_target: xtask_common::DeployTarget::Gcp,
            region: params.region.to_string(),
            for_bench: params.with_bench,
            data_blob_storage: DataBlobStorage::AllInBssSingleAz,
            rss_ha_enabled: params.rss_ha_enabled,
            rss_backend: params.rss_backend,
            // GCP uses pd_ssd (persistent disk) which is handled via the Ebs code path.
            // No volume_id needed — the disk is pre-attached as device "nss-journal".
            journal_type: xtask_common::JournalType::Ebs,
            num_nss_nodes: Some(2), // Terraform always creates nss-0 and nss-1 unconditionally
            num_bss_nodes: Some(params.num_bss_nodes),
            num_api_servers: Some(params.num_api_servers),
            num_bench_clients: if params.with_bench {
                Some(params.num_bench_clients)
            } else {
                None
            },
            workflow_cluster_id: Some(workflow_cluster_id),
            meta_stack_testing: false,
            use_generic_binaries: params.use_generic_binaries,
            journal_uuid: Some(journal_uuid),
            bss_storage_alloc_mode: params.bss_storage_alloc_mode,
        },
        aws: None,
        gcp: Some(gcp_config),
        endpoints: None,
        resources: None,
        etcd: if params.rss_backend == xtask_common::RssBackend::Etcd {
            Some(ClusterEtcdConfig {
                enabled: true,
                cluster_size: params.num_bss_nodes,
                endpoints: None,
            })
        } else {
            None
        },
        nodes: HashMap::new(),
        bootstrap_bucket: format!("{}-deploy-staging", params.project_id),
    };

    Ok(config)
}
