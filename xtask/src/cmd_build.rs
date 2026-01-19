use crate::*;
use std::io::Error;
use std::path::Path;
use std::sync::LazyLock;
use strum::{AsRefStr, EnumString};

pub static BUILD_ENVS: LazyLock<Vec<String>> =
    LazyLock::new(|| build_envs().expect("failed to initialize BUILD_ENVS"));

pub fn get_build_envs() -> &'static Vec<String> {
    &BUILD_ENVS
}

#[derive(Copy, Clone, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum BuildMode {
    Debug,
    Release,
}

pub fn build_mode(release: bool) -> BuildMode {
    match release {
        true => BuildMode::Release,
        false => BuildMode::Debug,
    }
}

pub fn build_envs() -> Result<Vec<String>, Error> {
    let timestamp = run_fun!(date "+%s")?;
    let main_info = get_repo_info(".")?;

    let mut envs = vec![
        format!("MAIN_BUILD_INFO={main_info}"),
        format!("BUILD_TIMESTAMP={timestamp}"),
    ];

    if Path::new(ZIG_REPO_PATH).exists() {
        let core_info = get_repo_info(ZIG_REPO_PATH)?;
        envs.push(format!("CORE_BUILD_INFO={core_info}"));
    }

    if Path::new("crates/ha").exists() {
        let ha_info = get_repo_info("crates/ha")?;
        envs.push(format!("HA_BUILD_INFO={ha_info}"));
    }

    if Path::new("crates/root_server").exists() {
        let rs_info = get_repo_info("crates/root_server")?;
        envs.push(format!("ROOT_SERVER_BUILD_INFO={rs_info}"));
    }

    Ok(envs)
}

fn get_repo_info(repo_path: &str) -> FunResult {
    if repo_path == "." || repo_path.is_empty() {
        let git_branch = run_fun!(git branch --show-current)?;
        let git_rev = run_fun!(git rev-parse --short HEAD)?;
        let dirty = if run_cmd!(git diff-index --quiet HEAD).is_ok() {
            ""
        } else {
            "+"
        };
        return Ok(format!("main:{git_branch}-{git_rev}{dirty}"));
    }

    if !Path::new(repo_path).exists() {
        return Ok(String::new());
    }

    let git_branch = run_fun!(cd $repo_path; git branch --show-current)?;
    let git_rev = run_fun!(cd $repo_path; git rev-parse --short HEAD)?;
    let dirty = if run_cmd!(cd $repo_path; git diff-index --quiet HEAD).is_ok() {
        ""
    } else {
        "+"
    };

    let repo_name = Path::new(repo_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(repo_path);

    Ok(format!("{repo_name}:{git_branch}-{git_rev}{dirty}"))
}

pub fn build_bench_rpc() -> CmdResult {
    let build_envs = get_build_envs();
    run_cmd! {
        info "Building benchmark tool `rewrk_rpc` ...";
        cd crates/bench_rpc;
        $[build_envs] cargo build --release;
    }
}

pub fn build_zig_servers(mode: BuildMode) -> CmdResult {
    if !Path::new(ZIG_REPO_PATH).exists() {
        return Ok(());
    }

    let build_envs = get_build_envs();
    let (opts, zig_out) = match mode {
        BuildMode::Debug => ("", ZIG_DEBUG_OUT),
        BuildMode::Release => ("--release=safe", ZIG_RELEASE_OUT),
    };
    run_cmd! {
        info "Building zig-based servers ...";
        cd $ZIG_REPO_PATH;
        $[build_envs] zig build -p ../$zig_out $opts 2>&1;
        info "Building bss and nss server done";
    }
}

pub fn build_rust_servers(mode: BuildMode) -> CmdResult {
    let build_envs = get_build_envs();
    match mode {
        BuildMode::Debug => {
            run_cmd! {
                info "Building rust-based servers in debug mode ...";
                $[build_envs] cargo build --workspace
                    --exclude fractalbits-bootstrap
                    --exclude rewrk*;
            }
        }
        BuildMode::Release => {
            run_cmd! {
                info "Building rust-based servers in release mode ...";
                $[build_envs] cargo build --workspace
                    --exclude container-all-in-one
                    --release;
            }
        }
    }
}

pub fn build_ui(region: &str) -> CmdResult {
    if !Path::new(UI_REPO_PATH).exists() {
        return Ok(());
    }

    run_cmd! {
        info "Building ui ...";
        cd $UI_REPO_PATH;
        npm install;
        VITE_AWS_REGION=$region npm run build;
    }
}

pub fn build_all(release: bool) -> CmdResult {
    let build_mode = build_mode(release);
    build_rust_servers(build_mode)?;
    build_zig_servers(build_mode)?;
    if release {
        build_bench_rpc()?;
    }
    build_ui(crate::UI_DEFAULT_REGION)?;
    Ok(())
}

/// Build only what's needed for Docker container (api_server, nss_role_agent, root_server, rss_admin, zig servers)
pub fn build_for_docker(release: bool) -> CmdResult {
    let build_envs = get_build_envs();
    let build_flag = if release { "--release" } else { "" };

    // Build packages that always exist
    run_cmd! {
        info "Building rust binaries for Docker...";
        $[build_envs] cargo build $build_flag
            -p api_server
            -p container-all-in-one;
    }?;

    // Build ha packages if they exist
    if Path::new("crates/ha").exists() {
        run_cmd! {
            $[build_envs] cargo build $build_flag
                -p nss_role_agent;
        }?;
    }

    // Build root_server packages if they exist
    if Path::new("crates/root_server").exists() {
        run_cmd! {
            $[build_envs] cargo build $build_flag
                -p root_server
                -p rss_admin;
        }?;
    }

    // Build zig servers if core repo exists
    build_zig_servers(build_mode(release))?;

    Ok(())
}

pub fn build_prebuilt(_release: bool) -> CmdResult {
    if !Path::new(&format!("{ZIG_REPO_PATH}/.git/")).exists() {
        warn!("No core repo found, skip building prebuilt");
        return Ok(());
    }

    // Ensure cross-compilation targets are installed
    run_cmd! {
        rustup target add x86_64-unknown-linux-gnu;
        rustup target add aarch64-unknown-linux-gnu;
    }?;

    let build_envs = get_build_envs();

    struct ArchTarget {
        arch: &'static str,
        rust_target: &'static str,
        rust_cpu: &'static str,
        zig_target: &'static str,
        zig_cpu: &'static str,
    }

    let targets = [
        ArchTarget {
            arch: "x86_64",
            rust_target: "x86_64-unknown-linux-gnu",
            rust_cpu: "x86-64-v3",
            zig_target: "x86_64-linux-gnu",
            zig_cpu: "x86_64_v3",
        },
        ArchTarget {
            arch: "aarch64",
            rust_target: "aarch64-unknown-linux-gnu",
            rust_cpu: "neoverse-n1",
            zig_target: "aarch64-linux-gnu",
            zig_cpu: "neoverse_n1",
        },
    ];

    for target in &targets {
        let arch = target.arch;
        let rust_target = target.rust_target;
        let rust_cpu = target.rust_cpu;
        let zig_target = target.zig_target;
        let zig_cpu = target.zig_cpu;
        let build_dir = format!("target/{rust_target}/release");

        run_cmd! {
            info "Building Zig binaries for $arch (release mode)...";
            cd $ZIG_REPO_PATH;
            $[build_envs] zig build -p ../$build_dir/zig-out
                -Doptimize=ReleaseSafe
                -Dtarget=$zig_target
                -Dcpu=$zig_cpu
                -Dfor_prebuilt=true 2>&1;
            info "Zig build complete for $arch";
        }?;

        run_cmd! {
            info "Building Rust binaries for $arch (size-optimized release mode with zigbuild)...";
            RUSTFLAGS="-C target-cpu=$rust_cpu -C opt-level=z -C codegen-units=1 -C strip=symbols"
            $[build_envs] cargo zigbuild --release --target $rust_target
                --workspace --exclude fractalbits-bootstrap --exclude rewrk*;
        }?;

        info!("Copying binaries to prebuilt/{arch} directory...");
        let prebuilt_dir = format!("prebuilt/{arch}");
        run_cmd!(mkdir -p $prebuilt_dir)?;
        for bin in [
            "nss_role_agent",
            "root_server",
            "rss_admin",
            "zig-out/bin/bss_server",
            "zig-out/bin/mirrord",
            "zig-out/bin/nss_server",
        ] {
            run_cmd!(cp -f $build_dir/$bin $prebuilt_dir/)?;
        }
    }

    info!("Prebuilt binaries are ready for all architectures");
    Ok(())
}
