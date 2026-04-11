use crate::cmd_build::BuildMode;
use crate::cmd_service::{init_service, start_service, stop_service, wait_for_port_ready};
use crate::etcd_utils::resolve_etcd_bin;
use crate::{CmdResult, DataBlobStorage, InitConfig, JournalType, RssBackend, ServiceName};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use data_types::{ObserverPersistentState, ObserverState};
use std::io::Error;
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

fn get_observer_state_from_firestore() -> Option<ObserverPersistentState> {
    let url = "http://localhost:8282/v1/projects/test-project/databases/fractalbits/documents/fractalbits-service-discovery/observer_state";
    let result = run_fun!(curl -sf $url);
    match result {
        Ok(output) => {
            let output = output.trim();
            if output.is_empty() {
                return None;
            }
            let json: serde_json::Value = serde_json::from_str(output).ok()?;
            let state_str = json
                .get("fields")?
                .get("state")?
                .get("stringValue")?
                .as_str()?;
            serde_json::from_str(state_str).ok()
        }
        Err(_) => None,
    }
}

fn get_observer_state(backend: RssBackend) -> Option<ObserverPersistentState> {
    match backend {
        RssBackend::Etcd => get_observer_state_from_etcd(),
        RssBackend::Ddb => get_observer_state_from_ddb(),
        RssBackend::Firestore => get_observer_state_from_firestore(),
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
    run_cmd! {
        info "Killing nss_server process...";
        ignore pkill -SIGKILL nss_server;
    }?;
    std::thread::sleep(Duration::from_millis(500));
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


pub async fn run_ebs_ha_failover_tests(backend: RssBackend) -> CmdResult {
    info!(
        "Running EBS HA failover tests with {} backend...",
        backend.as_ref()
    );

    let _ = stop_service(ServiceName::All);

    println!("{}", "=== EBS Test 1: Full Stack Initialization ===".bold());
    if let Err(e) = test_ebs_full_stack_initialization(backend).await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "EBS Test 1 FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "{}",
        "=== EBS Test 2: api_server During EBS Failover ===".bold()
    );
    if let Err(e) = test_ebs_api_server_during_failover(backend).await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "EBS Test 2 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("{}", "=== EBS Test 3: EBS Standby Recovery ===".bold());
    if let Err(e) = test_ebs_standby_recovery(backend).await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "EBS Test 3 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("{}", "=== EBS Test 4: Observer Restart ===".bold());
    if let Err(e) = test_observer_restart(backend).await {
        let _ = stop_service(ServiceName::All);
        eprintln!("{}: {}", "EBS Test 4 FAILED".red().bold(), e);
        return Err(e);
    }

    let _ = stop_service(ServiceName::All);

    println!(
        "{}",
        format!(
            "=== All EBS HA Failover Tests ({}) PASSED ===",
            backend.as_ref()
        )
        .green()
        .bold()
    );
    Ok(())
}

async fn test_ebs_full_stack_initialization(backend: RssBackend) -> CmdResult {
    info!("Initializing services with JournalType::Ebs...");

    let init_config = InitConfig {
        journal_type: JournalType::Ebs,
        rss_backend: backend,
        data_blob_storage: DataBlobStorage::AllInBssSingleAz,
        bss_count: 1,
        nss_disable_restart_limit: true,
        ..Default::default()
    };
    init_service(ServiceName::All, BuildMode::Debug, &init_config)?;
    start_service(ServiceName::All)?;

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
        "Observer initialized: state={}, nss={}, standby={}",
        state.observer_state, state.nss_machine.machine_id, state.standby_machine.machine_id
    );

    if state.nss_machine.machine_id != "nss-0" {
        return Err(Error::other(format!(
            "Expected nss-0 to be NSS machine, got {}",
            state.nss_machine.machine_id
        )));
    }

    if state.standby_machine.machine_id != "nss-1" {
        return Err(Error::other(format!(
            "Expected nss-1 to be standby machine, got {}",
            state.standby_machine.machine_id
        )));
    }

    // Verify EBS mode: standby should have running_service = noop
    if state.standby_machine.running_service != data_types::ServiceType::Noop {
        return Err(Error::other(format!(
            "Expected standby running_service=noop for EBS, got {}",
            state.standby_machine.running_service
        )));
    }

    println!(
        "{}",
        "SUCCESS: EBS full stack initialized with active_standby state!".green()
    );
    Ok(())
}

async fn test_ebs_api_server_during_failover(backend: RssBackend) -> CmdResult {
    info!("Testing api_server during EBS failover...");

    let state = get_observer_state(backend);
    if state.is_none() || state.as_ref().unwrap().observer_state != ObserverState::ActiveStandby {
        return Err(Error::other(
            "Expected active_standby state before testing failover",
        ));
    }

    if !verify_process_running("nss_server") {
        return Err(Error::other(
            "nss_server is not running before failover test",
        ));
    }

    // Create bucket and upload test objects
    let ctx = context();
    let bucket_name = "test-ebs-failover";

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

    for i in 0..3 {
        let key = format!("ebs-obj-{}", i);
        let data = format!("EBS test data for object {}", i);
        ctx.client
            .put_object()
            .bucket(bucket_name)
            .key(&key)
            .body(ByteStream::from(data.into_bytes()))
            .send()
            .await
            .map_err(|e| Error::other(format!("Failed to upload {}: {}", key, e)))?;
    }
    info!("Uploaded 3 test objects");

    // Kill NSS to trigger failover
    info!("Killing NSS process to trigger EBS failover...");
    kill_nss_process()?;

    std::thread::sleep(Duration::from_secs(1));
    if verify_process_running("nss_server") {
        kill_nss_process()?;
    }

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

    if state.nss_machine.expected_role != "solo" {
        return Err(Error::other(format!(
            "Expected NSS machine to have solo role, got {}",
            state.nss_machine.expected_role
        )));
    }

    // Wait for new NSS to be ready
    wait_for_port_ready(8087, 30)?;

    // Verify reads still work after failover
    for i in 0..3 {
        let key = format!("ebs-obj-{}", i);
        let expected_data = format!("EBS test data for object {}", i);

        let mut read_ok = false;
        for attempt in 0..5 {
            match ctx
                .client
                .get_object()
                .bucket(bucket_name)
                .key(&key)
                .send()
                .await
            {
                Ok(result) => {
                    let body = result
                        .body
                        .collect()
                        .await
                        .map_err(|e| Error::other(format!("Failed to read body: {e}")))?;
                    if body.into_bytes().as_ref() == expected_data.as_bytes() {
                        read_ok = true;
                        break;
                    }
                }
                Err(e) => {
                    let is_503 = e
                        .raw_response()
                        .map(|r| r.status().as_u16() == 503)
                        .unwrap_or(false);
                    if is_503 && attempt < 4 {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    return Err(Error::other(format!(
                        "Failed to read {} after failover: {e}",
                        key
                    )));
                }
            }
        }
        if !read_ok {
            return Err(Error::other(format!(
                "Failed to verify read for {} after failover",
                key
            )));
        }
    }

    // Verify new writes work
    let post_key = "post-ebs-failover-obj";
    let post_data = b"Data written after EBS failover";
    let mut write_ok = false;
    for attempt in 0..5 {
        match ctx
            .client
            .put_object()
            .bucket(bucket_name)
            .key(post_key)
            .body(ByteStream::from_static(post_data))
            .send()
            .await
        {
            Ok(_) => {
                write_ok = true;
                break;
            }
            Err(e) => {
                let is_503 = e
                    .raw_response()
                    .map(|r| r.status().as_u16() == 503)
                    .unwrap_or(false);
                if is_503 && attempt < 4 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                return Err(Error::other(format!(
                    "Failed to write post-failover object: {e}"
                )));
            }
        }
    }
    if !write_ok {
        return Err(Error::other(
            "Post-failover write did not succeed after retries",
        ));
    }

    println!(
        "{}",
        "SUCCESS: api_server handles EBS failover correctly!".green()
    );
    Ok(())
}

async fn test_ebs_standby_recovery(backend: RssBackend) -> CmdResult {
    info!("Testing EBS standby recovery to active_standby...");

    // EBS recovery should be fast: SoloDegraded -> ActiveStandby directly (no ActiveDegraded)
    let mut stable_state: Option<ObserverPersistentState> = None;
    for i in 0..30 {
        let state = get_observer_state(backend);
        if let Some(s) = state {
            let state_name = s.observer_state;
            info!("Attempt {}: observer state = {}", i + 1, state_name);

            if state_name == ObserverState::ActiveStandby
                || state_name == ObserverState::SoloDegraded
            {
                stable_state = Some(s);
                break;
            }
            // For EBS, ActiveDegraded should NOT occur
            if state_name == ObserverState::ActiveDegraded {
                return Err(Error::other(
                    "EBS failover should not enter ActiveDegraded state",
                ));
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

    if state.observer_state == ObserverState::ActiveStandby {
        info!("EBS: System already recovered to active_standby (fast path)!");
        println!(
            "{}",
            "SUCCESS: EBS standby recovered directly to active_standby!".green()
        );
        return Ok(());
    }

    // Wait for recovery (SoloDegraded -> ActiveStandby, no intermediate state)
    let state = wait_for_observer_state(backend, ObserverState::ActiveStandby, 30);
    if state.is_none() {
        let current = get_observer_state(backend);
        return Err(Error::other(format!(
            "EBS observer did not recover to active_standby. Current state: {:?}",
            current.map(|s| format!("{} (version {})", s.observer_state, s.version))
        )));
    }

    let state = state.unwrap();
    info!(
        "EBS recovery complete: state={}, nss={} (role={}), standby={} (role={})",
        state.observer_state,
        state.nss_machine.machine_id,
        state.nss_machine.expected_role,
        state.standby_machine.machine_id,
        state.standby_machine.expected_role
    );

    println!(
        "{}",
        "SUCCESS: EBS standby recovered to active_standby!".green()
    );
    Ok(())
}
