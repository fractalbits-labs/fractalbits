use crate::*;

pub fn run_cmd_precheckin(
    init_config: InitConfig,
    s3_api_only: bool,
    zig_unit_tests_only: bool,
    debug_api_server: bool,
    with_fractal_art_tests: bool,
    docker: DockerTestMode,
) -> CmdResult {
    if docker == DockerTestMode::Only {
        return run_docker_tests();
    }

    if debug_api_server {
        cmd_service::stop_service(ServiceName::ApiServer)?;
        run_cmd! {
            cargo build -p api_server;
        }?;
    } else {
        cmd_service::stop_service(ServiceName::All)?;
        cmd_build::build_rust_servers(BuildMode::Debug)?;
        cmd_build::build_zig_servers(cmd_build::ZigBuildOpts {
            mode: BuildMode::Debug,
            ..Default::default()
        })?;
    }

    if s3_api_only {
        return run_s3_api_tests(init_config, debug_api_server);
    }

    if zig_unit_tests_only {
        return run_zig_unit_tests(init_config);
    }

    cmd_service::init_service(ServiceName::All, BuildMode::Debug, init_config)?;
    run_zig_unit_tests(init_config)?;
    run_cmd! {
        info "Run cargo tests (except s3 api)";
        cargo test --workspace --exclude api_server;
    }?;

    run_s3_api_tests(init_config, false)?;

    if with_fractal_art_tests {
        run_fractal_art_tests()?;
    }

    if let Ok(core_file) = run_fun!(ls data | grep ^core) {
        let core_files: Vec<&str> = core_file.split("\n").collect();
        cmd_die!("Found core file(s) in directory ./data: ${core_files:?}");
    }

    if docker == DockerTestMode::Included {
        run_docker_tests()?;
    }

    info!("Precheckin is OK");
    Ok(())
}

fn run_fractal_art_tests() -> CmdResult {
    let rand_log = "data/logs/test_fractal_art_random.log";
    let format_log = "data/logs/format.log";
    let ts = ["ts", "-m", TS_FMT];
    let working_dir = run_fun!(pwd)?;
    let nss_server = format!("{working_dir}/{ZIG_DEBUG_OUT}/bin/nss_server");
    let test_fractal_art = format!("{working_dir}/{ZIG_DEBUG_OUT}/bin/test_fractal_art");
    let test_async_fractal_art =
        format!("{working_dir}/{ZIG_DEBUG_OUT}/bin/test_async_fractal_art");

    if !std::path::Path::new(&test_fractal_art).exists() {
        info!("Skipping fractal-art-tests");
        return Ok(());
    }

    // Start BSS instance for testing
    cmd_service::start_service(ServiceName::Bss)?;
    run_cmd! {
        mkdir -p data/logs;
        info "Running fractal art tests (random) with log $rand_log";
        $nss_server format |& $[ts] >$format_log;
        $test_fractal_art --tests random --size 400000 --ops 1000000 --threads 20 |& $[ts] >$rand_log;
    }?;

    let fat_log = "data/logs/test_fractal_art_fat.log";
    run_cmd! {
        info "Running fractal art tests (fat) with log $fat_log";
        $nss_server format |& $[ts] >$format_log;
        $test_fractal_art --tests fat --ops 1000000 |& $[ts] >$fat_log;
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

fn run_s3_api_tests(init_config: InitConfig, debug_api_server: bool) -> CmdResult {
    if debug_api_server {
        cmd_service::start_service(ServiceName::ApiServer)?;
        run_cmd! {
            info "Run cargo tests (s3 api tests)";
            cargo test --package api_server;
        }?;
        if init_config.with_https {
            run_cmd! {
                info "Run cargo tests (s3 https api tests)";
                USE_HTTPS_ENDPOINT=true cargo test --package api_server;
            }?;
        }
        return Ok(());
    }

    // Test with DDB backend
    let ddb_config = InitConfig {
        rss_backend: RssBackend::Ddb,
        ..init_config
    };
    info!("Testing with DDB backend...");
    cmd_service::init_service(ServiceName::All, BuildMode::Debug, ddb_config)?;
    cmd_service::start_service(ServiceName::All)?;
    run_cmd! {
        info "Run cargo tests (s3 api tests - DDB backend)";
        cargo test --package api_server;
    }?;

    if init_config.with_https {
        run_cmd! {
            info "Run cargo tests (s3 https api tests - DDB backend)";
            USE_HTTPS_ENDPOINT=true cargo test --package api_server;
        }?;
    }

    cmd_service::stop_service(ServiceName::All)?;

    // Test with etcd backend
    let etcd_config = InitConfig {
        rss_backend: RssBackend::Etcd,
        ..init_config
    };
    info!("Testing with etcd backend...");
    cmd_service::init_service(ServiceName::All, BuildMode::Debug, etcd_config)?;
    cmd_service::start_service(ServiceName::All)?;
    run_cmd! {
        info "Run cargo tests (s3 api tests - etcd backend)";
        cargo test --package api_server;
    }?;

    if init_config.with_https {
        run_cmd! {
            info "Run cargo tests (s3 https api tests - etcd backend)";
            USE_HTTPS_ENDPOINT=true cargo test --package api_server;
        }?;
    }

    let _ = cmd_service::stop_service(ServiceName::All);

    Ok(())
}

pub fn run_zig_unit_tests(init_config: InitConfig) -> CmdResult {
    if !std::path::Path::new(&format!("{ZIG_REPO_PATH}/build.zig")).exists() {
        info!("Skipping zig unit-tests");
        return Ok(());
    }

    cmd_service::init_service(ServiceName::All, BuildMode::Debug, init_config)?;

    // Start all BSS instances for testing
    for id in 0..init_config.bss_count {
        cmd_service::start_bss_instance(id)?;
    }

    let working_dir = run_fun!(pwd)?;
    let journal_uuid = std::fs::read_to_string("data/etc/journal_uuid.txt")?
        .trim()
        .to_string();
    let shared_dir = match init_config.journal_type {
        JournalType::Ebs => format!("../ebs/{}", journal_uuid),
        JournalType::Nvme => format!("local/journal/{}", journal_uuid),
    };
    run_cmd! {
        info "Formatting nss_server";
        SHARED_DIR=$shared_dir JOURNAL_UUID=$journal_uuid $working_dir/$ZIG_DEBUG_OUT/bin/nss_server format;
    }?;

    run_cmd! {
        info "Running zig unit tests";
        cd $ZIG_REPO_PATH;
        zig build -p ../$ZIG_DEBUG_OUT test --summary all 2>&1;
    }?;
    // Stop all BSS instances
    cmd_service::stop_service(ServiceName::Bss)?;

    info!("Zig unit tests completed successfully");
    Ok(())
}

fn run_docker_tests() -> CmdResult {
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
        info!("Running api_server tests against Docker container...");
        let test_result = run_cmd!(cargo test --package api_server);
        if test_result.is_err() {
            info!("Tests failed, showing container logs...");
            let _ = run_cmd!(docker logs fractalbits-dev 2>&1 | tail -200);
        }
        test_result?;

        Ok(())
    })();

    info!("Stopping Docker container...");
    let stop_result = cmd_docker::run_cmd_docker(DockerCommand::Stop { name: None });

    result?;
    stop_result?;

    info!("Docker tests completed successfully");
    Ok(())
}
