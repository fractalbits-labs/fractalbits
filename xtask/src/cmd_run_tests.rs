pub mod bss_node_failure;
pub mod bss_repair;
pub mod fs_server;
pub mod leader_election;
pub mod multi_az;
pub mod nss_failover;

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

    let test_fs_server = |disk_cache: bool| async move {
        fs_server::build_fs_server()?;
        fs_server::ensure_fuse_uring()?;

        // Phase A: EC (k=4, m=2, 6 nodes). The bulk of the FUSE
        // suite runs here; it's the default storage topology for the
        // fs_server end-to-end tests.
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
        let result_ec = fs_server::run_fs_server_tests(disk_cache, false).await;
        let _ = cmd_service::stop_service(ServiceName::FsServer);
        run_cmd! { ignore pkill -x fs_server 2>/dev/null; }?;
        cmd_service::stop_service(ServiceName::All)?;
        result_ec?;

        // Phase B: 3-replica replicated (N=3, R=W=2). Exercises the
        // version-aware fan-out + inline-repair read path on a real
        // multi-replica topology -- the partition-rejoin scenario
        // (stop a node, override-flush, restart, read) is impossible
        // to observe on the single-node default and pointless on EC
        // (k+m shards survive a single drop via parity).
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            &InitConfig {
                data_blob_storage: DataBlobStorage::AllInBssSingleAz,
                bss_count: 3,
                ..Default::default()
            },
        )?;
        cmd_service::start_service(ServiceName::All)?;
        let result_3r = fs_server::run_fs_server_tests(disk_cache, true).await;
        let _ = cmd_service::stop_service(ServiceName::FsServer);
        run_cmd! { ignore pkill -x fs_server 2>/dev/null; }?;
        cmd_service::stop_service(ServiceName::All)?;
        result_3r?;

        Ok(())
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
        TestType::NssFailover => test_nss_failover(RssBackend::Etcd).await,
        TestType::FsServer { disk_cache_only } => test_fs_server(disk_cache_only).await,
        TestType::Pjdfstest { subdir } => {
            cmd_service::init_service(ServiceName::All, BuildMode::Debug, &InitConfig::default())?;
            cmd_service::start_service(ServiceName::All)?;
            let result = fs_server::pjdfs::run_pjdfstest(subdir.as_deref()).await;
            cmd_service::stop_service(ServiceName::All)?;
            result
        }
        TestType::All => {
            test_fs_server(false).await?;
            test_bss_node_failure().await?;
            test_bss_repair().await?;
            test_nss_failover(RssBackend::Etcd).await?;
            test_leader_election()?;
            multi_az::run_multi_az_tests(MultiAzTestType::All).await
        }
    }
}
