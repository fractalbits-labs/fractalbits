use crate::cmd_service::{start_service, stop_service};
use crate::*;

pub fn run_cmd_nightly() -> CmdResult {
    // Full build (release mode) - matches config.py BUILD_MODE = "release"
    cmd_build::build_all(true)?;

    // Initialize and start prerequisite services based on main.py comment:
    // Prerequisites: cargo xtask service start etcd rss bss
    let init_config = InitConfig {
        rss_backend: RssBackend::Etcd,
        journal_type: JournalType::Nvme,
        nss_disable_restart_limit: true,
        ..Default::default()
    };

    // Initialize all services (includes nss, mirrord formatting)
    run_cmd!(info "Initializing services for nss_failover_test...")?;
    cmd_service::init_service(ServiceName::All, BuildMode::Release, init_config)?;

    // Start only etcd, rss, bss - nss/mirrord are managed by the test
    run_cmd!(info "Starting etcd, rss, bss services...")?;
    start_service(ServiceName::Etcd)?;
    start_service(ServiceName::Rss)?;
    start_service(ServiceName::Bss)?;

    // Run the nss_failover_test main.py script
    let nightly_log = "test_nss_failover_nightly.log";
    let result = run_cmd! {
        info "Running nss_failover_test with log $nightly_log ...";
        python3 ./core/nss_failover_test/main.py --duration 3600 |& ts -m $TS_FMT >$nightly_log;
    }
    .map_err(|e| {
        let _ = run_cmd!(tail $nightly_log);
        e
    });

    // Stop all services regardless of test result
    let _ = stop_service(ServiceName::All);

    result
}
