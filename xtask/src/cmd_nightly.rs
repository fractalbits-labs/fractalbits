use crate::cmd_service::{start_service, stop_service};
use crate::*;
use chrono::Local;
use std::path::Path;
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

fn run_crash_recovery_test(multi_bss: bool, initial_run: bool) -> CmdResult {
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

    // Check for leftover core dumps from a previous run
    crate::cmd_precheckin::check_for_core_dumps()?;

    // Create timestamp-based log directory
    let log_dir = format!("data/logs/nightly/{}", Local::now().format("%Y%m%d_%H%M%S"));
    run_cmd! {
        mkdir -p $log_dir;
        rm -rf data/coredumps;
        mkdir -p data/coredumps;
    }?;

    let nightly_log = format!("{}/crash_recovery.log", log_dir);
    let venv_python = "./core/crash_recovery_test/.venv/bin/python3";

    // Export VG configs and journal UUID so nss_server format and
    // test_async_fractal_art (launched by the Python test harness) pick them up.
    let journal_uuid = std::fs::read_to_string("data/etc/journal_uuid.txt")?
        .trim()
        .to_string();
    let shared_dir = "local/journal/".to_string() + &journal_uuid;
    let metadata_vg_config = generate_bss_metadata_vg_config(init_config.bss_count);
    let journal_vg_config = generate_bss_journal_vg_config(init_config.bss_count);

    // Run crash recovery test
    let result = run_cmd! {
        info "Running crash_recovery_test with log $nightly_log ...";
        METADATA_VG_CONFIG=$metadata_vg_config JOURNAL_VG_CONFIG=$journal_vg_config JOURNAL_UUID=$journal_uuid SHARED_DIR=$shared_dir
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

pub fn run_cmd_nightly(multi_bss: bool) -> CmdResult {
    info!("Running nightly crash recovery test (NSS mode, release build)");
    run_crash_recovery_test(multi_bss, true)
}
