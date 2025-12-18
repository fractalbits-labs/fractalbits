use crate::{InitConfig, cmd_build::*, cmd_service::*, *};

pub fn prepare_bench(with_flame_graph: bool) -> CmdResult {
    if with_flame_graph && run_cmd!(bash -c "type addr2line" | grep -q .cargo).is_err() {
        // From https://github.com/iced-rs/iced/issues/2394
        run_cmd! {
            info "Try to install addr2line to make perf script work with rust binary ...";
            cargo install addr2line --features="bin";
        }?;
    }
    Ok(())
}

pub fn run_cmd_bench(
    service: BenchService,
    workload: BenchWorkload,
    with_flame_graph: bool,
    keys_limit: usize,
    service_name: &mut ServiceName,
) -> CmdResult {
    let build_mode = BuildMode::Release;
    let uri;
    let bench_exe;
    let workload = workload.as_ref();
    let mut bench_opts = Vec::new();
    let keys_limit = keys_limit.to_string();

    build_zig_servers(build_mode)?;
    match service {
        BenchService::NssRpc => {
            *service_name = ServiceName::Nss;
            build_bench_rpc()?;
            init_service(*service_name, build_mode, InitConfig::default())?;
            start_service(ServiceName::Bss)?;
            start_service(ServiceName::Nss)?;
            uri = "127.0.0.1:8087";
            bench_exe = "./target/release/rewrk_rpc";
            bench_opts.extend_from_slice(&[
                "-t",
                "24",
                "-c",
                "1152",
                "-w",
                workload,
                "-k",
                &keys_limit,
            ]);
        }
        BenchService::BssRpc => {
            *service_name = ServiceName::Bss;
            build_bench_rpc()?;
            init_service(
                *service_name,
                build_mode,
                InitConfig {
                    data_blob_storage: DataBlobStorage::S3HybridSingleAz,
                    for_gui: false,
                    with_https: false,
                    bss_count: 1,
                    nss_disable_restart_limit: false,
                    rss_backend: Default::default(),
                },
            )?;
            cmd_service::start_bss_instance(0)?;
            uri = "127.0.0.1:8088";
            bench_exe = "./target/release/rewrk_rpc";
            bench_opts.extend_from_slice(&[
                "-t",
                "24",
                "-c",
                "1152",
                "-w",
                workload,
                "-p",
                "bss",
                "-k",
                &keys_limit,
            ]);
        }
    }

    let duration_secs = 300;
    let perf_handle = if with_flame_graph {
        run_cmd! {
            info "Start perf in the background ...";
            sudo bash -c "echo 0 > /proc/sys/kernel/kptr_restrict";
            sudo bash -c "echo -1 > /proc/sys/kernel/perf_event_paranoid";
        }?;

        Some(spawn!(perf record -F 99 --call-graph dwarf -a -g -- sleep $duration_secs)?)
    } else {
        None
    };

    run_cmd! {
        info "Starting benchmark ...";
        $bench_exe $[bench_opts] -d ${duration_secs}s -h $uri --pct;
    }?;

    if let Some(mut handle) = perf_handle {
        handle.wait()?;
        let flamegraph_path = run_fun!(brew --prefix flamegraph)?;
        run_cmd! {
            info "Post-processing perf data ...";
            perf script > out.perf;
            ${flamegraph_path}/bin/stackcollapse-perf.pl out.perf > out.folded;
            ${flamegraph_path}/bin/flamegraph.pl out.folded > out_perf.svg;
            info "Flamegraph \"out_perf.svg\" is generated";
        }?;
    }

    // stop service after benchmark to save cpu power
    run_cmd! {
        ignore killall nss_server &>/dev/null;
        ignore killall bss_server &>/dev/null;
    }?;
    Ok(())
}
