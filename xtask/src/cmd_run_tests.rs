pub mod bss_node_failure;
pub mod bss_repair;
pub mod fs_server;
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
        cmd_service::init_service(ServiceName::All, BuildMode::Debug, &ddb_config)?;
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
        cmd_service::init_service(ServiceName::All, BuildMode::Debug, &etcd_config)?;
        cmd_service::start_service(ServiceName::Etcd)?;
        leader_election::run_leader_election_tests(RssBackend::Etcd)?;
        leader_election::cleanup_test_root_server_instances()?;
        cmd_service::stop_service(ServiceName::Etcd)?;

        // Test with Firestore backend
        info!("Testing leader election with Firestore backend...");
        let firestore_config = InitConfig {
            rss_backend: RssBackend::Firestore,
            ..Default::default()
        };
        cmd_service::init_service(ServiceName::All, BuildMode::Debug, &firestore_config)?;
        cmd_service::start_service(ServiceName::FirestoreEmulator)?;
        leader_election::run_leader_election_tests(RssBackend::Firestore)?;
        leader_election::cleanup_test_root_server_instances()?;
        cmd_service::stop_service(ServiceName::FirestoreEmulator)?;

        Ok(())
    };

    let test_bss_node_failure = || async {
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            &InitConfig {
                data_blob_storage: DataBlobStorage::AllInBssSingleAz,
                bss_count: 6,
                ..Default::default()
            },
        )?;
        cmd_service::start_service(ServiceName::All)?;
        bss_node_failure::run_bss_node_failure_tests().await?;
        cmd_service::stop_service(ServiceName::All)
    };

    let test_nss_ha_with_ebs = || async {
        // Test with etcd backend
        info!("Testing NSS HA (EBS) failover with etcd backend...");
        nss_ha_failover::run_ebs_ha_failover_tests(RssBackend::Etcd).await?;

        // Test with DDB backend
        info!("Testing NSS HA (EBS) failover with DDB backend...");
        nss_ha_failover::run_ebs_ha_failover_tests(RssBackend::Ddb).await?;

        Ok(())
    };

    let test_fs_server = |run_fuse: bool, run_nfs: bool, disk_cache: bool| async move {
        fs_server::build_fs_server()?;
        if run_fuse {
            fs_server::ensure_fuse_uring()?;
        }
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            &InitConfig {
                data_blob_storage: DataBlobStorage::AllInBssSingleAz,
                bss_count: 6,
                ..Default::default()
            },
        )?;
        cmd_service::start_service(ServiceName::All)?;
        let result = fs_server::run_fs_server_tests(run_fuse, run_nfs, disk_cache).await;
        let _ = cmd_service::stop_service(ServiceName::FsServer);
        run_cmd! { ignore pkill -x fs_server 2>/dev/null; }?;
        cmd_service::stop_service(ServiceName::All)?;
        result
    };

    let test_bss_repair = || async {
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            &InitConfig {
                data_blob_storage: DataBlobStorage::AllInBssSingleAz,
                bss_count: 6,
                ..Default::default()
            },
        )?;
        cmd_service::start_service(ServiceName::All)?;
        let result = bss_repair::run_bss_repair_tests().await;
        cmd_service::stop_service(ServiceName::All)?;
        result
    };

    // prepare
    cmd_service::stop_service(ServiceName::All)?;
    cmd_build::build_zig_servers(cmd_build::ZigBuildOpts {
        mode: BuildMode::Debug,
        ..Default::default()
    })?;
    cmd_build::build_rust_servers(BuildMode::Debug)?;
    match test_type {
        TestType::MultiAz { subcommand } => multi_az::run_multi_az_tests(subcommand).await,
        TestType::LeaderElection => test_leader_election(),
        TestType::BssNodeFailure => test_bss_node_failure().await,
        TestType::BssRepair => test_bss_repair().await,
        TestType::NssHaWithEBS => test_nss_ha_with_ebs().await,
        TestType::FsServer {
            fuse,
            nfs,
            disk_cache_only,
        } => {
            let (run_fuse, run_nfs) = match (fuse, nfs) {
                (false, false) => (true, true),
                other => other,
            };
            test_fs_server(run_fuse, run_nfs, disk_cache_only).await
        }
        TestType::All => {
            test_leader_election()?;
            test_bss_repair().await?;
            multi_az::run_multi_az_tests(MultiAzTestType::All).await?;
            test_fs_server(true, true, false).await
        }
    }
}
