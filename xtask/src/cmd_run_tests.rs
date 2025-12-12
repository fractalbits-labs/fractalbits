pub mod bss_node_failure;
pub mod leader_election;
pub mod multi_az;

use crate::{
    CmdResult, DataBlobStorage, InitConfig, MultiAzTestType, ServiceName, TestType,
    cmd_build::{self, BuildMode},
    cmd_service,
};

pub async fn run_tests(test_type: TestType) -> CmdResult {
    let test_leader_election = || {
        cmd_service::init_service(ServiceName::All, BuildMode::Debug, InitConfig::default())?;
        cmd_service::start_service(ServiceName::DdbLocal)?;
        leader_election::run_leader_election_tests()?;
        leader_election::cleanup_test_root_server_instances()?;
        Ok(())
    };

    let test_bss_node_failure = || async {
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            InitConfig {
                data_blob_storage: DataBlobStorage::S3HybridSingleAz,
                for_gui: false,
                with_https: false,
                bss_count: 6,
                nss_disable_restart_limit: false,
            },
        )?;
        cmd_service::start_service(ServiceName::All)?;
        bss_node_failure::run_bss_node_failure_tests().await?;
        cmd_service::stop_service(ServiceName::All)
    };

    // prepare
    cmd_service::stop_service(ServiceName::All)?;
    cmd_build::build_rust_servers(BuildMode::Debug)?;
    match test_type {
        TestType::MultiAz { subcommand } => multi_az::run_multi_az_tests(subcommand).await,
        TestType::LeaderElection => test_leader_election(),
        TestType::BssNodeFailure => test_bss_node_failure().await,
        TestType::All => {
            test_leader_election()?;
            multi_az::run_multi_az_tests(MultiAzTestType::All).await
        }
    }
}
