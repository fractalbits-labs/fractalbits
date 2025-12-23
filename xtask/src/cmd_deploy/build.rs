use crate::etcd_utils::download_etcd_for_deploy;
use crate::*;
use std::path::Path;

use super::common::{CPU_TARGETS, CpuTarget, RUST_BINS, ZIG_BINS};

pub fn build(
    deploy_target: DeployTarget,
    release_mode: bool,
    zig_extra_build: &[String],
    api_server_build_env: &[String],
) -> CmdResult {
    let (zig_build_opt, rust_build_opt, build_dir) = if release_mode {
        ("--release=safe", "--release", "release")
    } else {
        ("", "", "debug")
    };

    // Format Zig extra build options as -Dkey=value
    // Always include journal_atomic_write_size=16384 (16KB) for deployment
    let mut zig_build_with_defaults = vec!["journal_atomic_write_size=16384".to_string()];
    zig_build_with_defaults.extend_from_slice(zig_extra_build);
    let zig_extra_opts: &Vec<String> = &zig_build_with_defaults
        .iter()
        .map(|opt| format!("-D{}", opt))
        .collect();

    // Ensure required Rust targets are installed
    run_cmd! {
        info "Ensuring required Rust targets are installed";
        rustup target add x86_64-unknown-linux-gnu;
        rustup target add aarch64-unknown-linux-gnu;
    }?;

    // Create deploy directories for all CPU targets
    for target in CPU_TARGETS {
        let deploy_dir = format!("prebuilt/deploy/{}/{}", target.arch, target.name);
        run_cmd!(mkdir -p $deploy_dir)?;
    }

    // Build fractalbits-bootstrap separately for each architecture without CPU flags
    if matches!(
        deploy_target,
        DeployTarget::Bootstrap | DeployTarget::Rust | DeployTarget::All
    ) {
        build_bootstrap(rust_build_opt, build_dir)?;
    }

    // Build other Rust projects with CPU-specific optimizations
    if matches!(deploy_target, DeployTarget::Rust | DeployTarget::All) {
        build_rust(rust_build_opt, build_dir, api_server_build_env)?;
    }

    // Build Zig projects for all CPU targets
    if matches!(deploy_target, DeployTarget::Zig | DeployTarget::All)
        && Path::new(ZIG_REPO_PATH).exists()
    {
        build_zig(zig_build_opt, build_dir, zig_extra_opts)?;
    }

    // Build and copy UI
    if matches!(deploy_target, DeployTarget::Ui | DeployTarget::All)
        && Path::new(UI_REPO_PATH).exists()
    {
        build_ui()?;
    }

    // Download (extract) warp binary for each architecture
    if deploy_target == DeployTarget::All {
        download_warp_binaries()?;
        download_etcd_for_deploy()?;
    }

    info!("Deploy build is done");

    Ok(())
}

fn build_bootstrap(rust_build_opt: &str, build_dir: &str) -> CmdResult {
    let build_envs = cmd_build::get_build_envs();
    for arch in ["x86_64", "aarch64"] {
        let rust_target = format!("{arch}-unknown-linux-gnu");
        run_cmd! {
            info "Building fractalbits-bootstrap for $arch";
            $[build_envs] cargo zigbuild
                -p fractalbits-bootstrap --target $rust_target $rust_build_opt;
        }?;

        // Copy fractalbits-bootstrap to arch-level directory
        let src_path = format!("target/{}/{}/fractalbits-bootstrap", rust_target, build_dir);
        let dst_path = format!("prebuilt/deploy/{}/fractalbits-bootstrap", arch);
        run_cmd! {
            mkdir -p prebuilt/deploy/$arch;
            cp $src_path $dst_path;
        }?;
    }
    Ok(())
}

fn build_rust(rust_build_opt: &str, build_dir: &str, api_server_build_env: &[String]) -> CmdResult {
    info!("Building Rust projects for all CPU targets");
    let build_envs = cmd_build::get_build_envs();

    for target in CPU_TARGETS {
        let rust_cpu = target.rust_cpu;
        let rust_target = target.rust_target;

        if rust_cpu.is_empty() {
            continue;
        }
        if api_server_build_env.is_empty() {
            run_cmd! {
                info "Building Rust projects for $rust_target ($rust_cpu)";
                RUSTFLAGS="-C target-cpu=$rust_cpu"
                $[build_envs] cargo zigbuild
                    --target $rust_target $rust_build_opt --workspace
                    --exclude xtask
                    --exclude fractalbits-bootstrap;
            }?;
        } else {
            run_cmd! {
                info "Building Rust projects for $rust_target ($rust_cpu)";
                RUSTFLAGS="-C target-cpu=$rust_cpu"
                $[build_envs] cargo zigbuild
                    --target $rust_target $rust_build_opt --workspace
                    --exclude xtask
                    --exclude fractalbits-bootstrap
                    --exclude api_server;

                info "Building api_server ...";
                RUSTFLAGS="-C target-cpu=$rust_cpu"
                $[api_server_build_env] $[build_envs] cargo zigbuild
                    --target $rust_target $rust_build_opt
                    --package api_server;
            }?;
        }

        // Copy Rust binaries to deploy directory (excluding fractalbits-bootstrap)
        copy_rust_binaries(target, rust_target, build_dir)?;
    }
    Ok(())
}

fn copy_rust_binaries(target: &CpuTarget, rust_target: &str, build_dir: &str) -> CmdResult {
    let deploy_dir = format!("prebuilt/deploy/{}/{}", target.arch, target.name);
    for bin in RUST_BINS {
        if *bin != "fractalbits-bootstrap" {
            let src_path = format!("target/{}/{}/{}", rust_target, build_dir, bin);
            let dst_path = format!("{}/{}", deploy_dir, bin);
            if Path::new(&src_path).exists() {
                run_cmd!(cp $src_path $dst_path)?;
            }
        }
    }
    Ok(())
}

fn build_zig(zig_build_opt: &str, build_dir: &str, zig_extra_opts: &Vec<String>) -> CmdResult {
    info!("Building Zig projects for all CPU targets");
    let build_envs = cmd_build::get_build_envs();

    for target in CPU_TARGETS {
        let zig_out_dir = if target.arch == "aarch64" {
            format!("target/aarch64-unknown-linux-gnu/{build_dir}/zig-out")
        } else {
            format!("target/x86_64-unknown-linux-gnu/{build_dir}/zig-out")
        };

        let zig_target = target.zig_target;
        let zig_cpu = target.zig_cpu;
        run_cmd! {
            info "Building Zig projects for $zig_target ($zig_cpu)";
            cd $ZIG_REPO_PATH;
            $[build_envs] zig build
                -p ../$zig_out_dir
                -Dtarget=$zig_target -Dcpu=$zig_cpu $zig_build_opt $[zig_extra_opts] 2>&1;
        }?;

        // Copy Zig binaries to deploy directory
        copy_zig_binaries(target, &zig_out_dir)?;
    }
    Ok(())
}

fn copy_zig_binaries(target: &CpuTarget, zig_out_dir: &str) -> CmdResult {
    let deploy_dir = format!("prebuilt/deploy/{}/{}", target.arch, target.name);
    for bin in ZIG_BINS {
        let src_path = format!("{}/bin/{}", zig_out_dir, bin);
        let dst_path = format!("{}/{}", deploy_dir, bin);
        run_cmd!(cp $src_path $dst_path)?;
    }
    Ok(())
}

fn build_ui() -> CmdResult {
    let region = run_fun!(aws configure list | grep region | awk r"{print $2}")?;
    cmd_build::build_ui(&region)?;
    run_cmd! {
        rm -rf prebuilt/deploy/ui;
        cp -r ui/dist prebuilt/deploy/ui;
    }?;
    Ok(())
}

fn download_warp_binaries() -> CmdResult {
    for arch in ["x86_64", "aarch64"] {
        let linux_arch = if arch == "aarch64" { "arm64" } else { "x86_64" };

        let warp_version = "v1.3.0";
        let warp_file = format!("warp_Linux_{linux_arch}.tar.gz");
        let warp_path = format!("third_party/minio/{warp_file}");

        let base_url = "https://github.com/minio/warp/releases/download";
        let download_url = format!("{base_url}/{warp_version}/{warp_file}");
        let checksums_url = format!("{base_url}/{warp_version}/checksums.txt");

        // Check if already downloaded
        if !Path::new(&warp_path).exists() {
            run_cmd! {
                info "Downloading warp binary for $linux_arch";
                mkdir -p third_party/minio;
                curl -sL -o $warp_path $download_url;
            }?;
        }

        run_cmd! {
            cd third_party/minio;
            info "Verifying warp binary checksum for $linux_arch";
            curl -sL -o warp_checksums.txt $checksums_url;
            grep $warp_file warp_checksums.txt | sha256sum -c --quiet;
            rm -f warp_checksums.txt;
        }?;

        // Extract warp to arch-level directory
        let deploy_dir = format!("prebuilt/deploy/{}", arch);
        run_cmd! {
            info "Extracting warp binary to $deploy_dir for $linux_arch";
            tar -xzf third_party/minio/$warp_file -C $deploy_dir warp;
        }?;
    }

    Ok(())
}
