use crate::cmd_service::{start_service, stop_service};
use crate::*;
use chrono::Local;
use std::path::Path;
use xtask_common::generate_initial_journal_config;
use xtask_common::{generate_bss_journal_vg_config, generate_bss_metadata_vg_config};

fn setup_python_venv() -> CmdResult {
    let venv_dir = "./core/crash_recovery_test/.venv";
    let venv_pip = "./core/crash_recovery_test/.venv/bin/pip";

    if !Path::new(venv_dir).exists() {
        run_cmd! {
            info "Creating Python virtual environment...";
            python3 -m venv $venv_dir;
        }?;
    }

    // Install pip itself to ensure venv is functional
    run_cmd! {
        info "Setting up Python virtual environment...";
        $venv_pip install -q --upgrade pip;
    }
}

fn run_crash_recovery_test_nss(multi_bss: bool, initial_run: bool, log_dir: &str) -> CmdResult {
    // Kill leftover test processes
    run_cmd! {
        ignore pkill -f test_async_fractal_art &>/dev/null;
    }?;

    // Full build (release mode) - skip if already built
    if initial_run {
        cmd_build::build_for_nightly()?;
    }

    // Initialize all services and start BSS
    let init_config = InitConfig {
        bss_count: if multi_bss { 6 } else { 1 },
        ..Default::default()
    };
    run_cmd!(info "Initializing all services...")?;
    cmd_service::init_service(ServiceName::All, BuildMode::Release, &init_config)?;
    run_cmd!(info "Starting BSS service...")?;
    start_service(ServiceName::Bss)?;

    if initial_run {
        setup_python_venv()?;
    }

    let nightly_log = format!("{}/crash_recovery_nss.log", log_dir);
    let venv_python = "./core/crash_recovery_test/.venv/bin/python3";

    // Export VG configs and journal config so nss_server format and
    // test_async_fractal_art (launched by the Python test harness) pick them up.
    let journal_uuid = std::fs::read_to_string("data/etc/journal_uuid.txt")?
        .trim()
        .to_string();
    let shared_dir = "local/journal/".to_string() + &journal_uuid;
    let metadata_vg_config = generate_bss_metadata_vg_config(init_config.bss_count);
    let journal_vg_config = generate_bss_journal_vg_config(init_config.bss_count);
    let journal_config = generate_initial_journal_config(&journal_uuid, "nss-0");

    // Run NSS crash recovery test
    let result = run_cmd! {
        info "Running NSS crash_recovery_test with log $nightly_log ...";
        METADATA_VG_CONFIG=$metadata_vg_config JOURNAL_VG_CONFIG=$journal_vg_config JOURNAL_CONFIG=$journal_config SHARED_DIR=$shared_dir
            $venv_python ./core/crash_recovery_test/main.py --mode nss --build-mode release &>$nightly_log;
    }
    .inspect_err(|_| {
        run_cmd! { ignore tail $nightly_log; }.ok();
    });

    // Stop BSS regardless of test result
    let _ = stop_service(ServiceName::Bss);

    // Check for core dumps regardless of test result
    let core_dump_result = crate::cmd_precheckin::check_for_core_dumps();

    // Report test failure first, then core dump failure
    result?;
    core_dump_result
}

fn run_crash_recovery_test_bss(log_dir: &str) -> CmdResult {
    // Kill leftover test processes
    run_cmd! {
        ignore pkill -f test_bss_storage_engine &>/dev/null;
    }?;

    let nightly_log = format!("{}/crash_recovery_bss.log", log_dir);
    let venv_python = "./core/crash_recovery_test/.venv/bin/python3";

    // Run BSS crash recovery test
    let result = run_cmd! {
        info "Running BSS crash_recovery_test with log $nightly_log ...";
        $venv_python ./core/crash_recovery_test/main.py --mode bss --build-mode release &>$nightly_log;
    }
    .inspect_err(|_| {
        run_cmd! { ignore tail $nightly_log; }.ok();
    });

    // Check for core dumps regardless of test result
    let core_dump_result = crate::cmd_precheckin::check_for_core_dumps();

    // Report test failure first, then core dump failure
    result?;
    core_dump_result
}

pub fn run_cmd_nightly(multi_bss: bool) -> CmdResult {
    info!("Running nightly crash recovery tests (NSS + BSS modes, release build)");

    // Check for leftover core dumps from a previous run
    crate::cmd_precheckin::check_for_core_dumps()?;

    // Create timestamp-based log directory
    let log_dir = format!("data/logs/nightly/{}", Local::now().format("%Y%m%d_%H%M%S"));
    run_cmd! {
        mkdir -p $log_dir;
        rm -rf data/coredumps;
        mkdir -p data/coredumps;
    }?;

    // Run NSS test first
    info!("Starting NSS crash recovery test...");
    if let Err(e) = run_crash_recovery_test_nss(multi_bss, true, &log_dir) {
        error!(
            "NSS crash recovery test failed. Logs and coredumps preserved in {}",
            log_dir
        );
        return Err(e);
    }

    // Run BSS test after NSS
    info!("Starting BSS crash recovery test...");
    run_crash_recovery_test_bss(&log_dir)?;

    info!("Nightly crash recovery tests completed successfully");
    Ok(())
}
