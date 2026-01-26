use crate::cmd_service::{start_service, stop_service};
use crate::*;
use chrono::Local;
use std::path::Path;

fn setup_python_venv() -> CmdResult {
    let venv_dir = "./core/nss_failover_test/.venv";
    let venv_pip = "./core/nss_failover_test/.venv/bin/pip";
    let requirements = "./core/nss_failover_test/requirements.txt";

    if !Path::new(venv_dir).exists() {
        run_cmd! {
            info "Creating Python virtual environment...";
            python3 -m venv $venv_dir;
        }?;
    }

    run_cmd! {
        info "Installing Python dependencies...";
        $venv_pip install -q -r $requirements;
    }
}

pub fn run_cmd_nightly() -> CmdResult {
    // Clean environment: stop all processes before running
    run_cmd!(info "Cleaning environment: stopping all services...")?;
    stop_service(ServiceName::All)?;

    // Full build (release mode)
    cmd_build::build_for_nightly()?;

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

    // Set up Python virtual environment and install dependencies
    setup_python_venv()?;

    // Create timestamp-based log directory: data/logs/nightly/<YYYYMMDD_HHMMSS>/
    let log_dir = format!("data/logs/nightly/{}", Local::now().format("%Y%m%d_%H%M%S"));
    run_cmd!(mkdir -p $log_dir)?;

    let nightly_log = format!("{}/nss_failover.log", log_dir);
    let venv_python = "./core/nss_failover_test/.venv/bin/python3";
    let result = run_cmd! {
        info "Running nss_failover_test with log $nightly_log ...";
        $venv_python ./core/nss_failover_test/main.py --duration 7200 --log-dir $log_dir &>$nightly_log;
    }
    .map_err(|e| {
        let _ = run_cmd!(tail $nightly_log);
        e
    });

    // Stop all services regardless of test result
    let _ = stop_service(ServiceName::All);

    result
}
