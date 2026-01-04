use crate::cmd_build::BuildMode;
use crate::cmd_service::{init_service, start_service, stop_service};
use crate::etcd_utils::resolve_etcd_bin;
use crate::{CmdResult, DataBlobStorage, InitConfig, JournalType, RssBackend, ServiceName};
use cmd_lib::*;
use colored::*;
use std::time::Duration;

const ETCD_SERVICE_DISCOVERY_PREFIX: &str = "/fractalbits-service-discovery/";

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
struct ObserverState {
    observer_state: String,
    nss_machine: MachineState,
    mirrord_machine: MachineState,
    version: u64,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
struct MachineState {
    machine_id: String,
    running_service: String,
    expected_role: String,
    service_status: String,
    health_port: u16,
    consecutive_failures: u32,
}

fn get_observer_state_from_etcd() -> Option<ObserverState> {
    let etcdctl = resolve_etcd_bin("etcdctl");
    let key = format!("{}observer_state", ETCD_SERVICE_DISCOVERY_PREFIX);

    let result = run_fun!($etcdctl get $key --print-value-only);
    match result {
        Ok(output) => {
            let output = output.trim();
            if output.is_empty() {
                return None;
            }
            serde_json::from_str(output).ok()
        }
        Err(_) => None,
    }
}

fn cleanup_observer_state() -> CmdResult {
    let etcdctl = resolve_etcd_bin("etcdctl");
    let key = format!("{}observer_state", ETCD_SERVICE_DISCOVERY_PREFIX);
    let _ = run_cmd!($etcdctl del $key >/dev/null);
    // Also clean up leader election key
    let leader_prefix = "/fractalbits-leader-election-observer/";
    let _ = run_cmd!($etcdctl del --prefix $leader_prefix >/dev/null);
    Ok(())
}

fn wait_for_observer_state(expected_state: &str, timeout_secs: u64) -> Option<ObserverState> {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(timeout_secs) {
        let state = get_observer_state_from_etcd();
        if state
            .as_ref()
            .is_some_and(|s| s.observer_state == expected_state)
        {
            return state;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    None
}

fn verify_process_running(binary_name: &str) -> bool {
    run_cmd!(pgrep -x $binary_name >/dev/null 2>&1).is_ok()
}

fn kill_nss_process() -> CmdResult {
    info!("Killing nss_server process...");
    let _ = run_cmd!(pkill -SIGKILL nss_server);
    std::thread::sleep(Duration::from_millis(500));
    Ok(())
}

fn seed_initial_observer_state() -> CmdResult {
    let etcdctl = resolve_etcd_bin("etcdctl");
    let key = format!("{}observer_state", ETCD_SERVICE_DISCOVERY_PREFIX);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();

    // Initial state: nss-A runs NSS (active), nss-B runs mirrord (standby)
    let initial_state = format!(
        r#"{{"observer_state":"active_standby","nss_machine":{{"machine_id":"nss-A","running_service":"nss","expected_role":"active","service_status":"failure","health_port":18077,"consecutive_failures":0}},"mirrord_machine":{{"machine_id":"nss-B","running_service":"mirrord","expected_role":"standby","service_status":"failure","health_port":18099,"consecutive_failures":0}},"version":1,"last_updated":{}}}"#,
        now
    );

    run_cmd!($etcdctl put $key $initial_state >/dev/null)?;
    info!("Seeded initial observer state in etcd");
    Ok(())
}

pub async fn run_nss_ha_failover_tests() -> CmdResult {
    info!("Running NSS HA failover tests...");

    // Clean up any previous runs
    let _ = stop_service(ServiceName::All);

    println!("{}", "=== Test 1: Full Stack Initialization ===".bold());
    if let Err(e) = test_full_stack_initialization().await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "Test 1 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("{}", "=== Test 2: NSS Failure and Failover ===".bold());
    if let Err(e) = test_nss_failure_failover().await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "Test 2 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("{}", "=== Test 3: Mirrord Recovery ===".bold());
    if let Err(e) = test_mirrord_recovery().await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "Test 3 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("{}", "=== Test 4: Observer Restart ===".bold());
    if let Err(e) = test_observer_restart().await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "Test 4 FAILED".red().bold(), e);
        return Err(e);
    }

    // Final cleanup
    let _ = stop_service(ServiceName::All);

    println!(
        "{}",
        "=== All NSS HA Failover Tests PASSED ===".green().bold()
    );
    Ok(())
}

async fn test_full_stack_initialization() -> CmdResult {
    info!("Initializing services with JournalType::Nvme...");

    // JournalType::Nvme enables active/standby mode with mirrord
    // Observer is automatically enabled for this journal type
    let init_config = InitConfig {
        journal_type: JournalType::Nvme,
        rss_backend: RssBackend::Etcd,
        data_blob_storage: DataBlobStorage::AllInBssSingleAz,
        bss_count: 1,
        nss_disable_restart_limit: true,
        ..Default::default()
    };

    init_service(ServiceName::All, BuildMode::Debug, init_config)?;

    info!("Starting etcd...");
    start_service(ServiceName::Etcd)?;

    // Clean up any previous observer state (from init_service phase)
    cleanup_observer_state()?;

    // Seed initial observer state BEFORE starting RSS
    // The observer should load our seeded state instead of initializing fresh
    info!("Seeding initial observer state in etcd...");
    seed_initial_observer_state()?;

    info!("Starting RSS (with observer enabled)...");
    start_service(ServiceName::Rss)?;

    info!("Starting BSS...");
    start_service(ServiceName::Bss)?;

    // Start nss_role_agents - they will read the observer state to determine their role
    // Start mirrord (standby) first because nss (active) needs to connect to mirrord
    // for sync notifications. The observer has a 30-second initial grace period so it
    // won't trigger failover during the startup window.
    info!("Starting NssRoleAgentB (mirrord/standby) first...");
    start_service(ServiceName::NssRoleAgentB)?;

    info!("Starting NssRoleAgentA (nss/active)...");
    start_service(ServiceName::NssRoleAgentA)?;

    // Verify processes are running
    if !verify_process_running("nss_server") {
        return Err(std::io::Error::other("nss_server is not running"));
    }
    if !verify_process_running("mirrord") {
        return Err(std::io::Error::other("mirrord is not running"));
    }

    // Wait for observer to update machine statuses based on health checks
    info!("Waiting for observer to update machine statuses...");
    std::thread::sleep(Duration::from_secs(10));

    // Verify initial state is active_standby
    let state = wait_for_observer_state("active_standby", 15);
    if state.is_none() {
        let current = get_observer_state_from_etcd();
        return Err(std::io::Error::other(format!(
            "Observer did not create initial active_standby state. Current state: {:?}",
            current.map(|s| s.observer_state)
        )));
    }

    let state = state.unwrap();
    info!(
        "Observer initialized: state={}, nss={}, mirrord={}",
        state.observer_state, state.nss_machine.machine_id, state.mirrord_machine.machine_id
    );

    // Verify nss-A is running NSS
    if state.nss_machine.machine_id != "nss-A" {
        return Err(std::io::Error::other(format!(
            "Expected nss-A to be NSS machine, got {}",
            state.nss_machine.machine_id
        )));
    }

    // Verify nss-B is running mirrord
    if state.mirrord_machine.machine_id != "nss-B" {
        return Err(std::io::Error::other(format!(
            "Expected nss-B to be mirrord machine, got {}",
            state.mirrord_machine.machine_id
        )));
    }

    println!(
        "{}",
        "SUCCESS: Full stack initialized with active_standby state!".green()
    );
    Ok(())
}

async fn test_nss_failure_failover() -> CmdResult {
    info!("Testing NSS failure and failover...");

    // Verify we're in active_standby state
    let state = get_observer_state_from_etcd();
    if state.is_none() || state.as_ref().unwrap().observer_state != "active_standby" {
        return Err(std::io::Error::other(
            "Expected active_standby state before testing failover",
        ));
    }

    info!("Killing NSS process to simulate failure...");
    kill_nss_process()?;

    // Verify nss_server is dead
    std::thread::sleep(Duration::from_secs(1));
    if verify_process_running("nss_server") {
        info!("nss_server still running, trying again...");
        kill_nss_process()?;
    }

    // Wait for observer to detect failure and transition to solo_degraded
    // max_failures=3 * heartbeat=1s = ~5s + margin
    info!("Waiting for observer to detect failure and transition to solo_degraded...");
    let state = wait_for_observer_state("solo_degraded", 20);
    if state.is_none() {
        let current = get_observer_state_from_etcd();
        return Err(std::io::Error::other(format!(
            "Observer did not transition to solo_degraded. Current state: {:?}",
            current.map(|s| format!("{} (version {})", s.observer_state, s.version))
        )));
    }

    let state = state.unwrap();
    info!(
        "Failover detected: state={}, nss={} (role={}), mirrord={} (role={})",
        state.observer_state,
        state.nss_machine.machine_id,
        state.nss_machine.expected_role,
        state.mirrord_machine.machine_id,
        state.mirrord_machine.expected_role
    );

    // After failover, nss-B should be promoted to NSS with solo role
    if state.nss_machine.expected_role != "solo" {
        return Err(std::io::Error::other(format!(
            "Expected NSS machine to have solo role, got {}",
            state.nss_machine.expected_role
        )));
    }

    println!(
        "{}",
        "SUCCESS: NSS failure detected and failover to solo_degraded!".green()
    );
    Ok(())
}

async fn test_mirrord_recovery() -> CmdResult {
    info!("Testing mirrord recovery to active_standby...");

    // Verify we're in solo_degraded state
    let state = get_observer_state_from_etcd();
    if state.is_none() || state.as_ref().unwrap().observer_state != "solo_degraded" {
        return Err(std::io::Error::other(
            "Expected solo_degraded state before testing recovery",
        ));
    }

    // The nss_role_agent should automatically restart the failed service
    // Wait for mirrord to restart on the degraded machine
    info!("Waiting for nss_role_agent to restart mirrord on failed machine...");
    let mut recovered = false;
    for i in 0..30 {
        if verify_process_running("mirrord") || verify_process_running("nss_server") {
            // Check if we have both processes running
            let nss_running = verify_process_running("nss_server");
            let mirrord_running = verify_process_running("mirrord");
            info!(
                "Attempt {}: nss_running={}, mirrord_running={}",
                i + 1,
                nss_running,
                mirrord_running
            );
            if nss_running && mirrord_running {
                recovered = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    if !recovered {
        return Err(std::io::Error::other(
            "Failed to recover both nss and mirrord processes",
        ));
    }

    // Wait for observer to detect recovery and transition back to active_standby
    info!("Waiting for observer to detect recovery and transition to active_standby...");
    let state = wait_for_observer_state("active_standby", 30);
    if state.is_none() {
        let current = get_observer_state_from_etcd();
        return Err(std::io::Error::other(format!(
            "Observer did not recover to active_standby. Current state: {:?}",
            current.map(|s| format!("{} (version {})", s.observer_state, s.version))
        )));
    }

    let state = state.unwrap();
    info!(
        "Recovery complete: state={}, nss={} (role={}), mirrord={} (role={})",
        state.observer_state,
        state.nss_machine.machine_id,
        state.nss_machine.expected_role,
        state.mirrord_machine.machine_id,
        state.mirrord_machine.expected_role
    );

    println!(
        "{}",
        "SUCCESS: Mirrord recovered and state is active_standby!".green()
    );
    Ok(())
}

async fn test_observer_restart() -> CmdResult {
    info!("Testing RSS/observer restart and state persistence...");

    // Get current state version
    let state_before = get_observer_state_from_etcd();
    if state_before.is_none() {
        return Err(std::io::Error::other("No observer state found in etcd"));
    }
    let version_before = state_before.as_ref().unwrap().version;
    let state_name_before = state_before.as_ref().unwrap().observer_state.clone();
    info!(
        "State before restart: {} (version {})",
        state_name_before, version_before
    );

    // Stop RSS (which includes the observer)
    info!("Stopping RSS service...");
    stop_service(ServiceName::Rss)?;
    std::thread::sleep(Duration::from_secs(2));

    // Restart RSS (observer should load state from etcd)
    info!("Restarting RSS service...");
    start_service(ServiceName::Rss)?;

    // Wait for observer to resume
    std::thread::sleep(Duration::from_secs(5));

    // Verify state is still valid
    let state_after = get_observer_state_from_etcd();
    if state_after.is_none() {
        return Err(std::io::Error::other(
            "Observer state missing after restart",
        ));
    }

    let state_after = state_after.unwrap();
    info!(
        "State after restart: {} (version {})",
        state_after.observer_state, state_after.version
    );

    // State should be preserved or updated (version might increase due to heartbeat)
    if state_after.observer_state != state_name_before {
        // This might be OK if the state changed due to health checks
        info!(
            "Note: State changed from {} to {} after restart",
            state_name_before, state_after.observer_state
        );
    }

    // Version should be >= previous (observer continues from persisted state)
    if state_after.version < version_before {
        return Err(std::io::Error::other(format!(
            "State version decreased after restart: {} -> {}",
            version_before, state_after.version
        )));
    }

    println!(
        "{}",
        "SUCCESS: RSS/observer restarted and state is preserved!".green()
    );
    Ok(())
}
