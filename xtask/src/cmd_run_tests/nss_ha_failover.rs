use crate::cmd_build::BuildMode;
use crate::cmd_service::{init_service, start_service, stop_service, wait_for_port_ready};
use crate::etcd_utils::resolve_etcd_bin;
use crate::{CmdResult, DataBlobStorage, InitConfig, JournalType, RssBackend, ServiceName};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use data_types::{ObserverPersistentState, ObserverState};
use std::io::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use test_common::*;
use xtask_common::LOCAL_DDB_ENVS;

const ETCD_SERVICE_DISCOVERY_PREFIX: &str = "/fractalbits-service-discovery/";

fn get_observer_state_from_etcd() -> Option<ObserverPersistentState> {
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

fn get_observer_state_from_ddb() -> Option<ObserverPersistentState> {
    let key_json = r#"{"service_id": {"S": "observer_state"}}"#;
    let result = run_fun!(
        $[LOCAL_DDB_ENVS]
        aws dynamodb get-item
            --table-name fractalbits-service-discovery
            --key $key_json
            --consistent-read
            --output json
    );
    match result {
        Ok(output) => {
            let output = output.trim();
            if output.is_empty() {
                return None;
            }
            let json: serde_json::Value = serde_json::from_str(output).ok()?;
            let state_str = json.get("Item")?.get("state")?.get("S")?.as_str()?;
            serde_json::from_str(state_str).ok()
        }
        Err(_) => None,
    }
}

fn get_observer_state(backend: RssBackend) -> Option<ObserverPersistentState> {
    match backend {
        RssBackend::Etcd => get_observer_state_from_etcd(),
        RssBackend::Ddb => get_observer_state_from_ddb(),
    }
}

fn wait_for_observer_state(
    backend: RssBackend,
    expected_state: ObserverState,
    timeout_secs: u64,
) -> Option<ObserverPersistentState> {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(timeout_secs) {
        let state = get_observer_state(backend);
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

pub async fn run_nss_ha_failover_tests(backend: RssBackend) -> CmdResult {
    info!(
        "Running NSS HA failover tests with {} backend...",
        backend.as_ref()
    );

    // Clean up any previous runs
    let _ = stop_service(ServiceName::All);

    println!("{}", "=== Test 1: Full Stack Initialization ===".bold());
    if let Err(e) = test_full_stack_initialization(backend).await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "Test 1 FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "{}",
        "=== Test 2: api_server During NSS Failover (Concurrent Operations) ===".bold()
    );
    if let Err(e) = test_api_server_during_nss_failover(backend).await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "Test 2 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("{}", "=== Test 3: Mirrord Recovery ===".bold());
    if let Err(e) = test_mirrord_recovery(backend).await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "Test 3 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("{}", "=== Test 4: Observer Restart ===".bold());
    if let Err(e) = test_observer_restart(backend).await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "Test 4 FAILED".red().bold(), e);
        return Err(e);
    }

    // Final cleanup
    let _ = stop_service(ServiceName::All);

    println!(
        "{}",
        format!(
            "=== All NSS HA Failover Tests ({}) PASSED ===",
            backend.as_ref()
        )
        .green()
        .bold()
    );
    Ok(())
}

async fn test_full_stack_initialization(backend: RssBackend) -> CmdResult {
    info!("Initializing services with JournalType::Nvme...");

    // JournalType::Nvme enables active/standby mode with mirrord
    // Observer is automatically enabled for this journal type
    let init_config = InitConfig {
        journal_type: JournalType::Nvme,
        rss_backend: backend,
        data_blob_storage: DataBlobStorage::AllInBssSingleAz,
        bss_count: 1,
        nss_disable_restart_limit: true,
        ..Default::default()
    };
    init_service(ServiceName::All, BuildMode::Debug, init_config)?;
    start_service(ServiceName::All)?;

    // Verify initial state is active_standby
    let state = wait_for_observer_state(backend, ObserverState::ActiveStandby, 15);
    if state.is_none() {
        let current = get_observer_state(backend);
        return Err(Error::other(format!(
            "Observer did not create initial active_standby state. Current state: {:?}",
            current.map(|s| s.observer_state)
        )));
    }

    let state = state.unwrap();
    info!(
        "Observer initialized: state={}, nss={}, mirrord={}",
        state.observer_state, state.nss_machine.machine_id, state.standby_machine.machine_id
    );

    // Verify nss-A is running NSS
    if state.nss_machine.machine_id != "nss-A" {
        return Err(Error::other(format!(
            "Expected nss-A to be NSS machine, got {}",
            state.nss_machine.machine_id
        )));
    }

    // Verify nss-B is running mirrord
    if state.standby_machine.machine_id != "nss-B" {
        return Err(Error::other(format!(
            "Expected nss-B to be mirrord machine, got {}",
            state.standby_machine.machine_id
        )));
    }

    println!(
        "{}",
        "SUCCESS: Full stack initialized with active_standby state!".green()
    );
    Ok(())
}

async fn test_mirrord_recovery(backend: RssBackend) -> CmdResult {
    info!("Testing mirrord recovery to active_standby...");

    // Wait for state to stabilize (active_degraded is transient)
    info!("Waiting for observer state to stabilize...");
    let mut stable_state: Option<ObserverPersistentState> = None;
    for i in 0..30 {
        let state = get_observer_state(backend);
        if let Some(s) = state {
            let state_name = s.observer_state;
            info!("Attempt {}: observer state = {}", i + 1, state_name);

            // These are stable states we can proceed with
            if state_name == ObserverState::ActiveStandby
                || state_name == ObserverState::SoloDegraded
            {
                stable_state = Some(s);
                break;
            }
            // active_degraded is transient, keep waiting
            if state_name == ObserverState::ActiveDegraded {
                info!("State is transient (active_degraded), waiting...");
            }
        }
        std::thread::sleep(Duration::from_secs(1));
    }

    let state = match stable_state {
        Some(s) => s,
        None => {
            let current = get_observer_state(backend);
            return Err(Error::other(format!(
                "Observer state did not stabilize. Current: {:?}",
                current.map(|s| s.observer_state)
            )));
        }
    };

    let current_state = state.observer_state;
    info!("Stable observer state: {}", current_state);

    // If already active_standby, the automatic recovery already happened - that's success!
    if current_state == ObserverState::ActiveStandby {
        info!("System already recovered to active_standby automatically!");
        // Verify both processes are running
        if !verify_process_running("nss_server") {
            return Err(Error::other(
                "nss_server not running in active_standby state",
            ));
        }
        if !verify_process_running("mirrord") {
            return Err(Error::other("mirrord not running in active_standby state"));
        }
        println!(
            "{}",
            "SUCCESS: System automatically recovered to active_standby!".green()
        );
        return Ok(());
    }

    // Otherwise we're in solo_degraded, wait for recovery

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
        return Err(Error::other(
            "Failed to recover both nss and mirrord processes",
        ));
    }

    // Wait for observer to detect recovery and transition back to active_standby
    info!("Waiting for observer to detect recovery and transition to active_standby...");
    let state = wait_for_observer_state(backend, ObserverState::ActiveStandby, 30);
    if state.is_none() {
        let current = get_observer_state(backend);
        return Err(Error::other(format!(
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
        state.standby_machine.machine_id,
        state.standby_machine.expected_role
    );

    println!(
        "{}",
        "SUCCESS: Mirrord recovered and state is active_standby!".green()
    );
    Ok(())
}

async fn test_observer_restart(backend: RssBackend) -> CmdResult {
    info!("Testing RSS/observer restart and state persistence...");

    // Get current state version
    let state_before = get_observer_state(backend);
    if state_before.is_none() {
        return Err(Error::other("No observer state found in backend"));
    }
    let version_before = state_before.as_ref().unwrap().version;
    let state_name_before = state_before.as_ref().unwrap().observer_state;
    info!(
        "State before restart: {} (version {})",
        state_name_before, version_before
    );

    // Stop RSS (which includes the observer)
    info!("Stopping RSS service...");
    stop_service(ServiceName::Rss)?;
    std::thread::sleep(Duration::from_secs(2));

    // Restart RSS (observer should load state from backend)
    info!("Restarting RSS service...");
    start_service(ServiceName::Rss)?;

    // Wait for observer to resume
    std::thread::sleep(Duration::from_secs(5));

    // Verify state is still valid
    let state_after = get_observer_state(backend);
    if state_after.is_none() {
        return Err(Error::other("Observer state missing after restart"));
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
        return Err(Error::other(format!(
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

async fn test_api_server_during_nss_failover(backend: RssBackend) -> CmdResult {
    info!("Testing api_server with continuous GET requests during NSS failover...");

    // Step 1: Verify starting state is active_standby
    info!("Step 1: Verifying active_standby state...");
    let state = get_observer_state(backend);
    if state.is_none() || state.as_ref().unwrap().observer_state != ObserverState::ActiveStandby {
        return Err(Error::other(
            "Expected active_standby state before testing failover",
        ));
    }

    // Verify services are running
    if !verify_process_running("nss_server") {
        return Err(Error::other(
            "nss_server is not running before failover test",
        ));
    }
    if !verify_process_running("mirrord") {
        return Err(Error::other("mirrord is not running before failover test"));
    }

    // Step 2: Create bucket and upload multiple test objects
    info!("Step 2: Creating bucket and uploading test objects...");
    let ctx = context();
    let bucket_name = "test-failover-concurrent";

    match ctx.client.create_bucket().bucket(bucket_name).send().await {
        Ok(_) => info!("Bucket created: {}", bucket_name),
        Err(e) => {
            let service_error = e.into_service_error();
            if !service_error.is_bucket_already_owned_by_you() {
                return Err(Error::other(format!(
                    "Failed to create bucket: {:?}",
                    service_error
                )));
            }
            info!("Bucket already exists: {}", bucket_name);
        }
    }

    // Upload multiple objects before failover
    for i in 0..5 {
        let key = format!("obj-{}", i);
        let data = format!("Test data for object {}", i);
        ctx.client
            .put_object()
            .bucket(bucket_name)
            .key(&key)
            .body(ByteStream::from(data.into_bytes()))
            .send()
            .await
            .map_err(|e| Error::other(format!("Failed to upload {}: {}", key, e)))?;
    }
    info!("Uploaded 5 test objects");

    // Step 3: Spawn continuous read tasks that run throughout the entire failover
    info!("Step 3: Spawning continuous GET request tasks...");
    let stop_signal = Arc::new(AtomicBool::new(false));
    let client = ctx.client.clone();

    let read_tasks: Vec<_> = (0..5)
        .map(|i| {
            let client = client.clone();
            let bucket = bucket_name.to_string();
            let stop = stop_signal.clone();

            tokio::spawn(async move {
                let key = format!("obj-{}", i);
                let expected_data = format!("Test data for object {}", i);
                let mut success_count: u32 = 0;
                let mut error_503_count: u32 = 0;
                let mut other_error_count: u32 = 0;
                let mut other_error_details: Option<String> = None;

                // Continuously send GET requests until stop signal
                while !stop.load(Ordering::Relaxed) {
                    match client.get_object().bucket(&bucket).key(&key).send().await {
                        Ok(result) => {
                            let body = result.body.collect().await;
                            match body {
                                Ok(data) => {
                                    if data.into_bytes().as_ref() == expected_data.as_bytes() {
                                        success_count += 1;
                                    } else {
                                        other_error_count += 1;
                                        if other_error_details.is_none() {
                                            other_error_details =
                                                Some(format!("obj-{}: data mismatch", i));
                                        }
                                    }
                                }
                                Err(e) => {
                                    other_error_count += 1;
                                    if other_error_details.is_none() {
                                        other_error_details =
                                            Some(format!("obj-{}: body read error: {}", i, e));
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let is_503 = e
                                .raw_response()
                                .map(|r| r.status().as_u16() == 503)
                                .unwrap_or(false);

                            if is_503 {
                                error_503_count += 1;
                            } else {
                                other_error_count += 1;
                                if other_error_details.is_none() {
                                    let status = e.raw_response().map(|r| r.status().as_u16());
                                    other_error_details = Some(format!(
                                        "obj-{}: unexpected error (status={:?}): {}",
                                        i, status, e
                                    ));
                                }
                            }
                        }
                    }
                    // Small delay between requests
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                (
                    i,
                    success_count,
                    error_503_count,
                    other_error_count,
                    other_error_details,
                )
            })
        })
        .collect();

    // Step 4: Wait a bit then kill NSS to trigger failover
    info!("Step 4: Waiting 500ms then killing NSS process...");
    tokio::time::sleep(Duration::from_millis(500)).await;
    kill_nss_process()?;

    // Verify nss_server is dead
    std::thread::sleep(Duration::from_secs(1));
    if verify_process_running("nss_server") {
        info!("nss_server still running, trying again...");
        kill_nss_process()?;
    }

    // Step 5: Wait for failover to complete
    info!("Step 5: Waiting for failover to solo_degraded...");
    let state = wait_for_observer_state(backend, ObserverState::SoloDegraded, 30);
    if state.is_none() {
        let current = get_observer_state(backend);
        return Err(Error::other(format!(
            "Observer did not transition to solo_degraded. Current state: {:?}",
            current.map(|s| format!("{} (version {})", s.observer_state, s.version))
        )));
    }

    let state = state.unwrap();
    info!(
        "Failover detected: state={}, nss={} (role={})",
        state.observer_state, state.nss_machine.machine_id, state.nss_machine.expected_role
    );

    // Verify solo role
    if state.nss_machine.expected_role != "solo" {
        return Err(Error::other(format!(
            "Expected NSS machine to have solo role, got {}",
            state.nss_machine.expected_role
        )));
    }

    // Step 6: Wait for new NSS to be ready
    info!("Step 6: Waiting for new NSS to be ready on port 8087...");
    wait_for_port_ready(8087, 30)?;

    // Step 7: Give some time for requests to succeed after failover completes
    info!("Step 7: Letting continuous GET requests run for 5 more seconds after failover...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Step 8: Stop the continuous reads and collect results
    info!("Step 8: Stopping continuous GET requests and collecting results...");
    stop_signal.store(true, Ordering::Relaxed);

    // Wait for tasks to finish
    let mut all_succeeded = true;
    for task in read_tasks {
        match tokio::time::timeout(Duration::from_secs(30), task).await {
            Ok(Ok((i, success, err_503, other_err, details))) => {
                info!(
                    "Task {}: success={}, 503_errors={}, other_errors={}",
                    i, success, err_503, other_err
                );

                // Must have at least some successes after failover
                if success == 0 {
                    eprintln!("Task {} had no successful reads!", i);
                    all_succeeded = false;
                }

                // Non-503 errors are not acceptable
                if other_err > 0 {
                    eprintln!(
                        "Task {} had {} unexpected errors: {:?}",
                        i, other_err, details
                    );
                    all_succeeded = false;
                }
            }
            Ok(Err(e)) => {
                eprintln!("Read task panicked: {:?}", e);
                all_succeeded = false;
            }
            Err(_) => {
                eprintln!("Read task timed out waiting to finish");
                all_succeeded = false;
            }
        }
    }

    if !all_succeeded {
        return Err(Error::other(
            "Some continuous GET tasks had failures during failover",
        ));
    }

    // Step 9: Verify new writes work with the new NSS
    // Give api_server a moment to receive updated NSS address from RSS cache notifier
    info!("Step 9: Verifying new writes work with the new NSS...");
    let post_failover_key = "post-failover-obj";
    let post_failover_data = b"Data written after failover";

    // Retry post-failover write with delays (api_server may need time to update NSS address)
    let mut write_succeeded = false;
    for attempt in 0..5 {
        match ctx
            .client
            .put_object()
            .bucket(bucket_name)
            .key(post_failover_key)
            .body(ByteStream::from_static(post_failover_data))
            .send()
            .await
        {
            Ok(_) => {
                info!("Post-failover write succeeded on attempt {}", attempt + 1);
                write_succeeded = true;
                break;
            }
            Err(e) => {
                let is_503 = e
                    .raw_response()
                    .map(|r| r.status().as_u16() == 503)
                    .unwrap_or(false);
                if is_503 && attempt < 4 {
                    info!(
                        "Post-failover write attempt {} got 503, retrying...",
                        attempt + 1
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                return Err(Error::other(format!(
                    "Failed to write post-failover object after {} attempts: {e}",
                    attempt + 1
                )));
            }
        }
    }

    if !write_succeeded {
        return Err(Error::other(
            "Post-failover write did not succeed after retries",
        ));
    }

    let get_result = ctx
        .client
        .get_object()
        .bucket(bucket_name)
        .key(post_failover_key)
        .send()
        .await
        .map_err(|e| Error::other(format!("Failed to read post-failover object: {e}")))?;

    let body = get_result
        .body
        .collect()
        .await
        .map_err(|e| Error::other(format!("Failed to read body: {e}")))?;
    if body.into_bytes().as_ref() != post_failover_data {
        return Err(Error::other("Post-failover object data mismatch"));
    }
    info!("Post-failover write/read verified successfully");

    println!(
        "{}",
        "SUCCESS: api_server handles continuous GET requests during NSS failover!".green()
    );
    Ok(())
}
