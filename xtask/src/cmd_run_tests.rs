pub mod bss_node_failure;
pub mod bss_repair;
pub mod fs_server;
pub mod leader_election;
pub mod nss_failover;

use crate::{
    CmdResult, DataBlobStorage, InitConfig, RssBackend, ServiceName, TestType,
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

    let test_fs_server = |disk_cache: bool, data_blob_storage: DataBlobStorage| async move {
        fs_server::build_fs_binaries()?;
        fs_server::ensure_fuse_uring()?;
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            &InitConfig {
                data_blob_storage,
                bss_count: 6,
                ..Default::default()
            },
        )?;
        cmd_service::start_service(ServiceName::All)?;
        let result = fs_server::run_fs_server_tests(disk_cache).await;
        let _ = fs_server::unmount_fs(fs_server::MOUNT_POINT);
        let _ = fs_server::stop_gateway();
        cmd_service::stop_service(ServiceName::All)?;
        result
    };

    let test_nss_failover = |backend: RssBackend| async move {
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            &InitConfig {
                rss_backend: backend,
                data_blob_storage: DataBlobStorage::AllInBssSingleAz,
                bss_count: 1,
                ..Default::default()
            },
        )?;
        cmd_service::start_service(ServiceName::All)?;
        let result = nss_failover::run_nss_failover_tests(backend).await;
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

    // prepare: the fs units are standalone, so a previous run that died
    // mid-test can leave them running with the binaries about to be rebuilt.
    let _ = fs_server::unmount_fs(fs_server::MOUNT_POINT);
    let _ = fs_server::stop_gateway();
    cmd_service::stop_service(ServiceName::All)?;
    cmd_build::build_zig_servers(cmd_build::ZigBuildOpts {
        mode: BuildMode::Debug,
        ..Default::default()
    })?;
    cmd_build::build_rust_servers(BuildMode::Debug)?;
    match test_type {
        TestType::LeaderElection => test_leader_election(),
        TestType::BssNodeFailure => test_bss_node_failure().await,
        TestType::BssRepair => test_bss_repair().await,
        TestType::NssFailover => test_nss_failover(RssBackend::Etcd).await,
        TestType::FsServer {
            disk_cache_only,
            data_blob_storage,
        } => test_fs_server(disk_cache_only, data_blob_storage).await,
        TestType::Pjdfstest {
            subdir,
            data_blob_storage,
        } => {
            cmd_service::init_service(
                ServiceName::All,
                BuildMode::Debug,
                &InitConfig {
                    data_blob_storage,
                    ..Default::default()
                },
            )?;
            cmd_service::start_service(ServiceName::All)?;
            let result = fs_server::pjdfs::run_pjdfstest(subdir.as_deref()).await;
            let _ = fs_server::stop_gateway();
            cmd_service::stop_service(ServiceName::All)?;
            result
        }
        TestType::All => {
            test_fs_server(false, DataBlobStorage::AllInBssSingleAz).await?;
            test_bss_node_failure().await?;
            test_bss_repair().await?;
            test_nss_failover(RssBackend::Etcd).await?;
            test_leader_election()
        }
    }
}
