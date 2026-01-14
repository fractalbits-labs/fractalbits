pub mod bss_node_failure;
pub mod leader_election;
pub mod multi_az;
pub mod nss_ha_failover;

use crate::{
    CmdResult, DataBlobStorage, InitConfig, MultiAzTestType, RssBackend, ServiceName, TestType,
    cmd_build::{self, BuildMode},
    cmd_service,
};
use cmd_lib::*;

pub async fn run_tests(test_type: TestType) -> CmdResult {
    let test_leader_election = || {
        // Test with DDB backend
        info!("Testing leader election with DDB backend...");
        let ddb_config = InitConfig {
            rss_backend: RssBackend::Ddb,
            ..Default::default()
        };
        cmd_service::init_service(ServiceName::All, BuildMode::Debug, ddb_config)?;
        cmd_service::start_service(ServiceName::DdbLocal)?;
        leader_election::run_leader_election_tests(RssBackend::Ddb)?;
        leader_election::cleanup_test_root_server_instances()?;
        cmd_service::stop_service(ServiceName::DdbLocal)?;

        // Test with etcd backend
        info!("Testing leader election with etcd backend...");
        let etcd_config = InitConfig {
            rss_backend: RssBackend::Etcd,
            ..Default::default()
        };
        cmd_service::init_service(ServiceName::All, BuildMode::Debug, etcd_config)?;
        cmd_service::start_service(ServiceName::Etcd)?;
        leader_election::run_leader_election_tests(RssBackend::Etcd)?;
        leader_election::cleanup_test_root_server_instances()?;
        cmd_service::stop_service(ServiceName::Etcd)?;

        Ok(())
    };

    let test_bss_node_failure = || async {
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            InitConfig {
                data_blob_storage: DataBlobStorage::S3HybridSingleAz,
                bss_count: 6,
                ..Default::default()
            },
        )?;
        cmd_service::start_service(ServiceName::All)?;
        bss_node_failure::run_bss_node_failure_tests().await?;
        cmd_service::stop_service(ServiceName::All)
    };

    let test_nss_ha_failover = || async {
        // Test with etcd backend
        info!("Testing NSS HA failover with etcd backend...");
        nss_ha_failover::run_nss_ha_failover_tests(RssBackend::Etcd).await?;

        // Test with DDB backend
        info!("Testing NSS HA failover with DDB backend...");
        nss_ha_failover::run_nss_ha_failover_tests(RssBackend::Ddb).await?;

        Ok(())
    };

    // prepare
    cmd_service::stop_service(ServiceName::All)?;
    cmd_build::build_rust_servers(BuildMode::Debug)?;
    match test_type {
        TestType::MultiAz { subcommand } => multi_az::run_multi_az_tests(subcommand).await,
        TestType::LeaderElection => test_leader_election(),
        TestType::BssNodeFailure => test_bss_node_failure().await,
        TestType::NssHaFailover => test_nss_ha_failover().await,
        TestType::All => {
            test_leader_election()?;
            multi_az::run_multi_az_tests(MultiAzTestType::All).await
        }
    }
}
