use crate::CmdResult;
use crate::cmd_service::start_bss_instance;
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use std::time::{Duration, Instant};
use test_common::*;
use tokio::time::sleep;

pub async fn run_bss_node_failure_tests() -> CmdResult {
    info!("Running BSS node failure tests...");

    println!(
        "\n{}",
        "=== Test: Write/Read/Delete with One Node Down ===".bold()
    );
    if let Err(e) = test_write_read_delete_with_node_down().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== All BSS Node Failure Tests PASSED ===".green().bold()
    );
    Ok(())
}

async fn test_write_read_delete_with_node_down() -> CmdResult {
    let ctx = context();
    let bucket = ctx.create_bucket("test-bss-failure").await;

    // Statistics tracking
    let mut put_success = 0;
    let mut put_fail = 0;
    let mut get_success = 0;
    let mut get_fail = 0;
    let mut get_slow = 0; // operations taking > 1 second
    let mut delete_success = 0;
    let mut delete_fail = 0;

    // Step 1: Write objects with all nodes up (baseline)
    println!("  Step 1: Write objects with all BSS nodes up (baseline)");
    let mut baseline_put_times = Vec::new();
    for i in 1..=10 {
        let key = format!("baseline-object-{i}");
        let start = Instant::now();
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from(format!("data-{i}").into_bytes()))
            .send()
            .await
            .expect("Failed to put object with all nodes up");
        baseline_put_times.push(start.elapsed());
    }
    let avg_baseline_put: Duration =
        baseline_put_times.iter().sum::<Duration>() / baseline_put_times.len() as u32;
    println!(
        "    OK: 10 objects written, avg latency: {:?}",
        avg_baseline_put
    );

    // Read baseline objects
    let mut baseline_get_times = Vec::new();
    for i in 1..=10 {
        let key = format!("baseline-object-{i}");
        let start = Instant::now();
        ctx.client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("Failed to get baseline object");
        baseline_get_times.push(start.elapsed());
    }
    let avg_baseline_get: Duration =
        baseline_get_times.iter().sum::<Duration>() / baseline_get_times.len() as u32;
    println!(
        "    OK: 10 objects read, avg latency: {:?}",
        avg_baseline_get
    );

    // Step 2: Stop BSS node 0
    println!("  Step 2: Stopping BSS node 0");
    run_cmd!(systemctl --user stop bss@0.service)?;
    sleep(Duration::from_secs(2)).await;

    if run_cmd!(systemctl --user is-active --quiet bss@0.service).is_ok() {
        return Err(std::io::Error::other(
            "BSS node 0 should be stopped but is still active",
        ));
    }
    println!("    OK: BSS node 0 confirmed down");

    // Step 3: Write new objects with one node down
    println!("  Step 3: Write objects with one node down");
    let mut degraded_put_times = Vec::new();
    for i in 1..=10 {
        let key = format!("degraded-object-{i}");
        let start = Instant::now();
        match ctx
            .client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from(format!("degraded-data-{i}").into_bytes()))
            .send()
            .await
        {
            Ok(_) => {
                let elapsed = start.elapsed();
                degraded_put_times.push(elapsed);
                put_success += 1;
                println!("    Put {key}: OK ({:?})", elapsed);
            }
            Err(e) => {
                put_fail += 1;
                println!("    Put {key}: {} ({:?})", "FAILED".red(), start.elapsed());
                warn!("Put error: {e}");
            }
        }
    }

    // Step 4: Read baseline objects (written when all nodes were up)
    println!("  Step 4: Read baseline objects (may hit down node)");
    for i in 1..=10 {
        let key = format!("baseline-object-{i}");
        let start = Instant::now();
        match ctx
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(resp) => {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(1) {
                    get_slow += 1;
                    println!("    Get {key}: OK but {} ({:?})", "SLOW".yellow(), elapsed);
                } else {
                    println!("    Get {key}: OK ({:?})", elapsed);
                }
                get_success += 1;
                let body = resp.body.collect().await.expect("Failed to read body");
                assert_eq!(
                    body.into_bytes().as_ref(),
                    format!("data-{i}").as_bytes(),
                    "Data mismatch for {key}"
                );
            }
            Err(e) => {
                get_fail += 1;
                println!("    Get {key}: {} ({:?})", "FAILED".red(), start.elapsed());
                warn!("Get error: {e}");
            }
        }
    }

    // Step 5: Read degraded objects (written with node down)
    println!("  Step 5: Read degraded objects");
    for i in 1..=10 {
        let key = format!("degraded-object-{i}");
        let start = Instant::now();
        match ctx
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(resp) => {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(1) {
                    get_slow += 1;
                    println!("    Get {key}: OK but {} ({:?})", "SLOW".yellow(), elapsed);
                } else {
                    println!("    Get {key}: OK ({:?})", elapsed);
                }
                get_success += 1;
                let body = resp.body.collect().await.expect("Failed to read body");
                assert_eq!(
                    body.into_bytes().as_ref(),
                    format!("degraded-data-{i}").as_bytes(),
                    "Data mismatch for {key}"
                );
            }
            Err(e) => {
                get_fail += 1;
                println!("    Get {key}: {} ({:?})", "FAILED".red(), start.elapsed());
                warn!("Get error: {e}");
            }
        }
    }

    // Step 6: Delete some objects
    println!("  Step 6: Delete objects with one node down");
    for i in 1..=5 {
        let key = format!("baseline-object-{i}");
        let start = Instant::now();
        match ctx
            .client
            .delete_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => {
                delete_success += 1;
                println!("    Delete {key}: OK ({:?})", start.elapsed());
            }
            Err(e) => {
                delete_fail += 1;
                println!(
                    "    Delete {key}: {} ({:?})",
                    "FAILED".red(),
                    start.elapsed()
                );
                warn!("Delete error: {e}");
            }
        }
    }

    // Step 7: Restart BSS node 0
    println!("  Step 7: Restarting BSS node 0");
    start_bss_instance(0)?;
    sleep(Duration::from_secs(2)).await;

    // Step 8: Verify remaining objects after restart
    println!("  Step 8: Verify objects after node restart");
    let mut post_restart_success = 0;
    let mut post_restart_fail = 0;
    for i in 6..=10 {
        let key = format!("baseline-object-{i}");
        match ctx
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => {
                post_restart_success += 1;
                println!("    Get {key}: OK");
            }
            Err(e) => {
                post_restart_fail += 1;
                println!("    Get {key}: {}", "FAILED".red());
                warn!("Post-restart get error: {e}");
            }
        }
    }
    for i in 1..=10 {
        let key = format!("degraded-object-{i}");
        match ctx
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => {
                post_restart_success += 1;
                println!("    Get {key}: OK");
            }
            Err(e) => {
                post_restart_fail += 1;
                println!("    Get {key}: {}", "FAILED".red());
                warn!("Post-restart get error: {e}");
            }
        }
    }

    // Cleanup
    println!("  Cleanup: Deleting test objects");
    for i in 1..=10 {
        let _ = ctx
            .client
            .delete_object()
            .bucket(&bucket)
            .key(format!("baseline-object-{i}"))
            .send()
            .await;
        let _ = ctx
            .client
            .delete_object()
            .bucket(&bucket)
            .key(format!("degraded-object-{i}"))
            .send()
            .await;
    }

    // Summary
    println!("\n  === Summary ===");
    println!("    Baseline avg PUT latency: {:?}", avg_baseline_put);
    println!("    Baseline avg GET latency: {:?}", avg_baseline_get);
    if !degraded_put_times.is_empty() {
        let avg_degraded_put: Duration =
            degraded_put_times.iter().sum::<Duration>() / degraded_put_times.len() as u32;
        println!("    Degraded avg PUT latency: {:?}", avg_degraded_put);
        let slowdown =
            avg_degraded_put.as_millis() as f64 / avg_baseline_put.as_millis().max(1) as f64;
        println!("    PUT slowdown factor: {:.2}x", slowdown);
    }
    println!("    PUT: {} success, {} failed", put_success, put_fail);
    println!(
        "    GET: {} success, {} failed, {} slow (>1s)",
        get_success, get_fail, get_slow
    );
    println!(
        "    DELETE: {} success, {} failed",
        delete_success, delete_fail
    );
    println!(
        "    Post-restart: {} success, {} failed",
        post_restart_success, post_restart_fail
    );

    // Determine test result
    let total_ops = put_success + put_fail + get_success + get_fail + delete_success + delete_fail;
    let failed_ops = put_fail + get_fail + delete_fail;
    let failure_rate = failed_ops as f64 / total_ops as f64 * 100.0;

    if failure_rate > 50.0 {
        println!(
            "\n  {}",
            format!(
                "RESULT: High failure rate ({:.1}%) - quorum system not resilient enough",
                failure_rate
            )
            .red()
        );
        return Err(std::io::Error::other(format!(
            "High failure rate: {:.1}% of operations failed with one node down",
            failure_rate
        )));
    } else if get_slow > 5 || failure_rate > 10.0 {
        println!(
            "\n  {}",
            format!(
                "RESULT: Degraded performance - {} slow ops, {:.1}% failure rate",
                get_slow, failure_rate
            )
            .yellow()
        );
        println!("    This indicates the circuit breaker improvement would help!");
    } else {
        println!(
            "\n  {}",
            "RESULT: System handled node failure gracefully".green()
        );
    }

    println!("{}", "SUCCESS: BSS node failure test completed".green());
    Ok(())
}
