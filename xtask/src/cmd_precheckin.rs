use crate::*;
use std::future::Future;
use std::time::Instant;

async fn run_stage(bar: &str, name: String, fut: impl Future<Output = CmdResult>) -> CmdResult {
    info!("{bar}");
    info!("STAGE: {name}");
    info!("{bar}");
    let start = Instant::now();
    let result = fut.await;
    let secs = start.elapsed().as_secs();
    match &result {
        Ok(()) => info!("STAGE DONE: {name} ({secs}s)"),
        Err(_) => error!("STAGE FAILED: {name} ({secs}s)"),
    }
    result
}

/// Run a top-level stage with banners and elapsed time, so each one is easy to spot in a long
/// precheckin log.
pub async fn stage(name: impl Into<String>, fut: impl Future<Output = CmdResult>) -> CmdResult {
    run_stage(&"=".repeat(70), name.into(), fut).await
}

/// Like `stage`, with a lighter banner for a pass nested inside a stage.
pub async fn substage(name: impl Into<String>, fut: impl Future<Output = CmdResult>) -> CmdResult {
    run_stage(&"-".repeat(70), name.into(), fut).await
}

pub async fn run_cmd_precheckin(
    init_config: InitConfig,
    s3_api_only: bool,
    zig_unit_tests_only: bool,
    debug_s3_gateway: bool,
    with_fractal_art_tests: bool,
    all: bool,
    docker: DockerTestMode,
) -> CmdResult {
    let build_envs = cmd_build::get_build_envs();
    if docker == DockerTestMode::Only {
        return run_docker_tests().await;
    }

    if debug_s3_gateway {
        stage("build s3_gateway", async {
            cmd_service::stop_service(ServiceName::S3Gateway)?;
            run_cmd! {
                $[build_envs] cargo build -p s3_gateway;
            }
        })
        .await?;
    } else {
        stage("build servers (debug)", async {
            cmd_service::stop_service(ServiceName::All)?;
            cmd_build::build_rust_servers(BuildMode::Debug)?;
            cmd_build::build_zig_servers(cmd_build::ZigBuildOpts {
                mode: BuildMode::Debug,
                ..Default::default()
            })
        })
        .await?;
    }

    if s3_api_only {
        return run_s3_api_tests(&init_config, debug_s3_gateway).await;
    }

    if zig_unit_tests_only {
        return run_zig_unit_tests().await;
    }

    cmd_service::init_service(ServiceName::All, BuildMode::Debug, &init_config)?;
    run_zig_unit_tests().await?;
    stage(
        "cargo tests (except s3 api, fs_gateway and fs_client)",
        async {
            run_cmd! {
                $[build_envs] cargo test --workspace
                    --exclude s3_gateway --exclude fs_gateway --exclude fs_client;
            }
        },
    )
    .await?;

    run_s3_api_tests(&init_config, false).await?;

    if with_fractal_art_tests {
        run_fractal_art_tests().await?;
    }

    if all {
        cmd_run_tests::run_tests(TestType::All).await?;
    }

    check_for_core_dumps()?;

    if docker == DockerTestMode::Included {
        run_docker_tests().await?;
    }

    info!("Precheckin is OK");
    Ok(())
}

async fn run_fractal_art_tests() -> CmdResult {
    let working_dir = run_fun!(pwd)?;
    let test_async_fractal_art =
        format!("{working_dir}/{ZIG_DEBUG_OUT}/bin/test_async_fractal_art");
    if !std::path::Path::new(&test_async_fractal_art).exists() {
        info!("Skipping fractal-art-tests");
        return Ok(());
    }
    stage("fractal art tests", async {
        run_fractal_art_tests_inner(&working_dir, &test_async_fractal_art)
    })
    .await
}

fn run_fractal_art_tests_inner(working_dir: &str, test_async_fractal_art: &str) -> CmdResult {
    let format_log = "data/logs/format.log";
    let ts = ["ts", "-m", TS_FMT];
    let nss_server = format!("{working_dir}/{ZIG_DEBUG_OUT}/bin/nss_server");

    // Start BSS instance for testing
    cmd_service::start_service(ServiceName::Bss)?;
    run_cmd!(mkdir -p data/logs)?;

    let async_fractal_art_log = "data/logs/test_async_fractal_art_fat.log";
    run_cmd! {
        info "Running async fractal art fat tests with log $async_fractal_art_log";
        $nss_server format --init_test_tree |& $[ts] >$format_log;
        $test_async_fractal_art --tests fat
            --ops 100000 --parallelism 1000 |& $[ts] >$async_fractal_art_log;
    }?;

    let async_fractal_art_log = "data/logs/test_async_fractal_art_rename.log";
    run_cmd! {
        info "Running async fractal art rename tests with log $async_fractal_art_log";
        $nss_server format --init_test_tree |& $[ts] >$format_log;
        $test_async_fractal_art --prefill 100000 --tests rename
            --ops 10000 --parallelism 1000 --debug |& $[ts] >$async_fractal_art_log;
    }?;

    let async_fractal_art_log = "data/logs/test_async_fractal_art.log";
    run_cmd! {
        info "Running async fractal art tests with log $async_fractal_art_log";
        $nss_server format --init_test_tree |& $[ts] >$format_log;
        $test_async_fractal_art -p 20 |& $[ts] >$async_fractal_art_log;
        $test_async_fractal_art -p 20 |& $[ts] >>$async_fractal_art_log;
        $test_async_fractal_art -p 20 |& $[ts] >>$async_fractal_art_log;
    }?;

    // Stop all BSS instances
    cmd_service::stop_service(ServiceName::Bss)?;
    Ok(())
}

async fn run_s3_api_tests(init_config: &InitConfig, debug_s3_gateway: bool) -> CmdResult {
    let build_envs = cmd_build::get_build_envs();
    if debug_s3_gateway {
        return stage("s3 api tests", async {
            cmd_service::start_service(ServiceName::S3Gateway)?;
            run_cmd! {
                $[build_envs] cargo test --package s3_gateway;
            }?;
            if init_config.with_https {
                run_cmd! {
                    info "Run cargo tests (s3 https api tests)";
                    $[build_envs] USE_HTTPS_ENDPOINT=true cargo test --package s3_gateway;
                }?;
            }
            Ok(())
        })
        .await;
    }

    for backend in [RssBackend::Ddb, RssBackend::Etcd] {
        let config = InitConfig {
            rss_backend: backend,
            ..init_config.clone()
        };
        stage(format!("s3 api tests ({backend:?} backend)"), async {
            cmd_service::init_service(ServiceName::All, BuildMode::Debug, &config)?;
            cmd_service::start_service(ServiceName::All)?;
            run_cmd! {
                $[build_envs] cargo test --package s3_gateway;
            }?;
            if config.with_https {
                run_cmd! {
                    info "Run cargo tests (s3 https api tests)";
                    $[build_envs] USE_HTTPS_ENDPOINT=true cargo test --package s3_gateway;
                }?;
            }
            Ok(())
        })
        .await?;
        let _ = cmd_service::stop_service(ServiceName::All);
    }

    Ok(())
}

pub async fn run_zig_unit_tests() -> CmdResult {
    if !std::path::Path::new(&format!("{ZIG_REPO_PATH}/build.zig")).exists() {
        info!("Skipping zig unit-tests");
        return Ok(());
    }

    stage("zig unit tests", async {
        run_cmd! {
            cd $ZIG_REPO_PATH;
            zig build -p ../$ZIG_DEBUG_OUT test --summary all 2>&1;
        }
    })
    .await
}

async fn run_docker_tests() -> CmdResult {
    stage("docker tests", async { run_docker_tests_inner() }).await
}

fn run_docker_tests_inner() -> CmdResult {
    info!("Building Docker image...");
    cmd_docker::run_cmd_docker(DockerCommand::Build {
        release: true,
        all_from_source: true,
        image_name: "fractalbits".to_string(),
        tag: "latest".to_string(),
    })?;

    info!("Starting Docker container...");
    cmd_docker::run_cmd_docker(DockerCommand::Run {
        image_name: "fractalbits".to_string(),
        tag: "latest".to_string(),
        port: 8080,
        name: None,
        detach: true,
        wait_ready: true,
    })?;

    let result = (|| -> CmdResult {
        info!("Running s3_gateway tests against Docker container...");
        let build_envs = cmd_build::get_build_envs();
        let test_result = run_cmd!($[build_envs] cargo test --package s3_gateway);
        if test_result.is_err() {
            info!("Tests failed, showing container logs...");
            run_cmd! { ignore docker logs fractalbits-dev 2>&1 | tail -200; }?;
        }
        test_result?;

        Ok(())
    })();

    info!("Stopping Docker container...");
    let stop_result = cmd_docker::run_cmd_docker(DockerCommand::Stop { name: None });

    result?;
    stop_result
}

pub fn check_for_core_dumps() -> CmdResult {
    if let Ok(core_file) = run_fun!(find data/ -type f -name "core.*") {
        let core_files: Vec<&str> = core_file.split("\n").filter(|s| !s.is_empty()).collect();
        if !core_files.is_empty() {
            cmd_die!("Found core file(s) in directory ./data: ${core_files:?}");
        }
    }
    Ok(())
}
