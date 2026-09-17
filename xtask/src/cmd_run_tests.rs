pub mod bss_node_failure;
pub mod bss_repair;
pub mod fs_server;
pub mod leader_election;
pub mod nss_failover;

use cmd_lib::*;

use crate::{
    CmdResult, DataBlobStorage, InitConfig, RssBackend, ServiceName, TestType,
    cmd_build::{self, BuildMode},
    cmd_precheckin::stage,
    cmd_service,
};

pub async fn run_tests(
    test_type: TestType,
    with_leader_election: bool,
    data_blob_storage: DataBlobStorage,
) -> CmdResult {
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

    let test_pjdfstest = |subdir: Option<String>, data_blob_storage: DataBlobStorage| async move {
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
    };

    // prepare: the fs units are standalone, so a previous run that died
    // mid-test can leave them running with the binaries about to be rebuilt.
    stage("run-tests: build servers (debug)", async {
        let _ = fs_server::unmount_fs(fs_server::MOUNT_POINT);
        let _ = fs_server::stop_gateway();
        cmd_service::stop_service(ServiceName::All)?;
        cmd_build::build_zig_servers(cmd_build::ZigBuildOpts {
            mode: BuildMode::Debug,
            ..Default::default()
        })?;
        cmd_build::build_rust_servers(BuildMode::Debug)
    })
    .await?;

    let fs_server_stage = |disk_cache_only: bool, data_blob_storage: DataBlobStorage| {
        stage(
            format!(
                "run-tests fs-server (disk_cache_only={disk_cache_only}, {data_blob_storage:?})"
            ),
            test_fs_server(disk_cache_only, data_blob_storage),
        )
    };
    let pjdfstest_stage = |subdir: Option<String>, data_blob_storage: DataBlobStorage| {
        stage(
            format!("run-tests pjdfstest ({data_blob_storage:?})"),
            test_pjdfstest(subdir, data_blob_storage),
        )
    };
    let bss_node_failure_stage = || stage("run-tests bss-node-failure", test_bss_node_failure());
    let bss_repair_stage = || stage("run-tests bss-repair", test_bss_repair());
    let nss_failover_stage = |backend: RssBackend| {
        stage(
            format!("run-tests nss-failover ({backend:?} backend)"),
            test_nss_failover(backend),
        )
    };
    let leader_election_stage = || {
        stage("run-tests leader-election", async {
            test_leader_election()
        })
    };

    match test_type {
        TestType::LeaderElection => leader_election_stage().await,
        TestType::BssNodeFailure => bss_node_failure_stage().await,
        TestType::BssRepair => bss_repair_stage().await,
        TestType::NssFailover => nss_failover_stage(RssBackend::Etcd).await,
        TestType::FsServer {
            disk_cache_only,
            data_blob_storage,
        } => fs_server_stage(disk_cache_only, data_blob_storage).await,
        TestType::Pjdfstest {
            subdir,
            data_blob_storage,
        } => pjdfstest_stage(subdir, data_blob_storage).await,
        TestType::All => {
            fs_server_stage(false, data_blob_storage).await?;
            pjdfstest_stage(None, data_blob_storage).await?;
            // EC quorum and repair only apply to data blobs held in BSS.
            if matches!(data_blob_storage, DataBlobStorage::S3HybridSingleAz) {
                info!("Skipping bss-node-failure and bss-repair: data blobs are on the S3 volume");
            } else {
                bss_node_failure_stage().await?;
                bss_repair_stage().await?;
            }
            nss_failover_stage(RssBackend::Etcd).await?;
            if with_leader_election {
                leader_election_stage().await?;
            }
            Ok(())
        }
    }
}
