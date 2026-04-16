use crate::cmd_service::wait_for_port_ready;
use crate::etcd_utils::resolve_etcd_bin;
use crate::{CmdResult, RssBackend};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use data_types::JournalConfig;
use std::time::{Duration, Instant};
use test_common::*;
use tokio::time::sleep;
use xtask_common::LOCAL_DDB_ENVS;

const ETCD_SERVICE_DISCOVERY_PREFIX: &str = "/fractalbits-service-discovery/";

// --- journal-configs helpers ---

fn get_journal_configs_etcd() -> Option<Vec<JournalConfig>> {
    let etcdctl = resolve_etcd_bin("etcdctl");
    let key = format!("{ETCD_SERVICE_DISCOVERY_PREFIX}journal-configs");
    let output = run_fun!($etcdctl get $key --print-value-only).ok()?;
    let output = output.trim();
    if output.is_empty() {
        return None;
    }
    serde_json::from_str(output).ok()
}

fn get_journal_configs_ddb() -> Option<Vec<JournalConfig>> {
    let key_json = r#"{"service_id": {"S": "journal-configs"}}"#;
    let output = run_fun!(
        $[LOCAL_DDB_ENVS]
        aws dynamodb get-item
            --table-name fractalbits-service-discovery
            --key $key_json
            --consistent-read
            --output json
    )
    .ok()?;
    let output = output.trim();
    if output.is_empty() {
        return None;
    }
    let json: serde_json::Value = serde_json::from_str(output).ok()?;
    let value_str = json.get("Item")?.get("value")?.get("S")?.as_str()?;
    serde_json::from_str(value_str).ok()
}

fn get_journal_configs(backend: RssBackend) -> Option<Vec<JournalConfig>> {
    match backend {
        RssBackend::Etcd => get_journal_configs_etcd(),
        RssBackend::Ddb => get_journal_configs_ddb(),
        _ => None,
    }
}

/// Wait until journal-configs shows running_nss_id == expected_nss_id for the first journal.
fn wait_for_journal_reassignment(
    backend: RssBackend,
    expected_nss_id: &str,
    timeout_secs: u64,
) -> Option<Vec<JournalConfig>> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(timeout_secs) {
        if let Some(configs) = get_journal_configs(backend)
            && let Some(first) = configs.first()
            && first.running_nss_id.as_deref() == Some(expected_nss_id)
        {
            return Some(configs);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    None
}

// --- test implementation ---

pub async fn run_nss_failover_tests(backend: RssBackend) -> CmdResult {
    info!(
        "Running NSS failover tests with {} backend...",
        backend.as_ref()
    );

    println!(
        "{}",
        "=== Test 1: NSS Failover with Data Verification ===".bold()
    );
    if let Err(e) = test_nss_failover_with_data(backend).await {
        eprintln!("{}: {}", "Test 1 FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "{}",
        format!(
            "=== All NSS Failover Tests ({}) PASSED ===",
            backend.as_ref()
        )
        .green()
        .bold()
    );
    Ok(())
}

async fn test_nss_failover_with_data(backend: RssBackend) -> CmdResult {
    let ctx = context();
    let bucket = ctx.create_bucket("test-nss-failover").await;

    // Step 1: Verify initial state — nss-0 should own the journal
    println!("  Step 1: Verify initial journal assignment");
    let configs = get_journal_configs(backend).ok_or_else(|| {
        std::io::Error::other("Failed to read journal-configs from service discovery")
    })?;
    let first_config = configs
        .first()
        .ok_or_else(|| std::io::Error::other("journal-configs is empty"))?;
    assert_eq!(
        first_config.running_nss_id.as_deref(),
        Some("nss-0"),
        "Expected nss-0 to own the journal initially"
    );
    let initial_version = first_config.version;
    println!(
        "    OK: journal {} assigned to nss-0 (version {})",
        first_config.journal_uuid, initial_version
    );

    // Step 2: Write test objects before failover
    println!("  Step 2: Write test objects before failover");
    for i in 0..5 {
        let key = format!("failover-obj-{i}");
        let data = format!("failover test data {i}");
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from(data.into_bytes()))
            .send()
            .await
            .map_err(|e| std::io::Error::other(format!("Failed to put {key}: {e}")))?;
    }
    println!("    OK: 5 test objects written");

    // Step 3: Kill nss_role_agent for nss-0 to trigger failover
    println!("  Step 3: Killing nss_role_agent@0 to trigger failover");
    run_cmd!(systemctl --user stop nss_role_agent@0.service)?;
    sleep(Duration::from_secs(1)).await;

    // Verify nss_role_agent@0 is stopped
    if run_cmd!(systemctl --user is-active --quiet nss_role_agent@0.service).is_ok() {
        return Err(std::io::Error::other(
            "nss_role_agent@0 should be stopped but is still active",
        ));
    }
    println!("    OK: nss_role_agent@0 confirmed stopped");

    // Step 4: Wait for observer to detect failure and reassign journal to nss-1
    // Wait for: grace period (10s from RSS start, mostly elapsed by now)
    //         + stale threshold (5s after last health report from nss-0)
    //         + observer loop time
    println!("  Step 4: Waiting for observer to reassign journal to nss-1...");
    let configs = wait_for_journal_reassignment(backend, "nss-1", 60).ok_or_else(|| {
        // Show current state for debugging
        let current = get_journal_configs(backend);
        std::io::Error::other(format!(
            "Observer did not reassign journal to nss-1 within 60s. Current: {:?}",
            current.and_then(|c| c.first().map(|jc| format!(
                "running_nss_id={:?}, version={}",
                jc.running_nss_id, jc.version
            )))
        ))
    })?;

    let reassigned_config = configs.first().expect("configs should not be empty");
    assert_eq!(reassigned_config.running_nss_id.as_deref(), Some("nss-1"));
    assert!(
        reassigned_config.version > initial_version,
        "Version should have been bumped: {} -> {}",
        initial_version,
        reassigned_config.version
    );
    println!(
        "    OK: journal reassigned to nss-1 (version {} -> {})",
        initial_version, reassigned_config.version
    );

    // Step 5: Wait for nss-1 to start serving (nss_role_agent@1 picks up the role)
    println!("  Step 5: Waiting for nss-1 to start serving on port 8087...");
    wait_for_port_ready(8087, 30)?;
    println!("    OK: port 8087 is ready");

    // Step 6: Verify reads work after failover (with retries for api_server reconnection)
    println!("  Step 6: Verify data reads after failover");
    for i in 0..5 {
        let key = format!("failover-obj-{i}");
        let expected = format!("failover test data {i}");

        let mut read_ok = false;
        for attempt in 0..10 {
            match ctx
                .client
                .get_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
            {
                Ok(result) => {
                    let body =
                        result.body.collect().await.map_err(|e| {
                            std::io::Error::other(format!("Failed to read body: {e}"))
                        })?;
                    assert_eq!(
                        body.into_bytes().as_ref(),
                        expected.as_bytes(),
                        "Data mismatch for {key}"
                    );
                    read_ok = true;
                    break;
                }
                Err(e) => {
                    if attempt < 9 {
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    return Err(std::io::Error::other(format!(
                        "Failed to read {key} after failover: {e}"
                    )));
                }
            }
        }
        assert!(read_ok, "Failed to read {key} after all retries");
    }
    println!("    OK: all 5 objects verified after failover");

    // Step 7: Verify new writes work after failover
    println!("  Step 7: Verify new writes after failover");
    let post_key = "post-failover-obj";
    let post_data = b"data written after failover";
    let mut write_ok = false;
    for attempt in 0..10 {
        match ctx
            .client
            .put_object()
            .bucket(&bucket)
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
                if attempt < 9 {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                return Err(std::io::Error::other(format!(
                    "Failed to write post-failover object: {e}"
                )));
            }
        }
    }
    assert!(
        write_ok,
        "Post-failover write did not succeed after retries"
    );

    // Verify the write by reading it back
    let result = ctx
        .client
        .get_object()
        .bucket(&bucket)
        .key(post_key)
        .send()
        .await
        .map_err(|e| std::io::Error::other(format!("Failed to read post-failover object: {e}")))?;
    let body = result
        .body
        .collect()
        .await
        .map_err(|e| std::io::Error::other(format!("Failed to read body: {e}")))?;
    assert_eq!(body.into_bytes().as_ref(), post_data);
    println!("    OK: post-failover write and read verified");

    // Cleanup
    println!("  Cleanup: Deleting test objects");
    for i in 0..5 {
        let _ = ctx
            .client
            .delete_object()
            .bucket(&bucket)
            .key(format!("failover-obj-{i}"))
            .send()
            .await;
    }
    let _ = ctx
        .client
        .delete_object()
        .bucket(&bucket)
        .key(post_key)
        .send()
        .await;
    let _ = ctx.client.delete_bucket().bucket(&bucket).send().await;

    println!(
        "{}",
        "SUCCESS: NSS failover test with data verification passed!".green()
    );
    Ok(())
}
