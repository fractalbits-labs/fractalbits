use crate::etcd_utils::download_etcd_for_deploy;
use crate::*;
use std::path::Path;
use xtask_common::DeployTarget as UploadTarget;

use super::common::{ARCH_TARGETS, ArchTarget, RUST_BINS, ZIG_BINS};

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

    // Ensure required Rust targets are installed
    run_cmd! {
        info "Ensuring required Rust targets are installed";
        rustup target add x86_64-unknown-linux-gnu;
        rustup target add aarch64-unknown-linux-gnu;
    }?;

    // Create deploy directories: generic (shared), aws, and on_prem
    for target in ARCH_TARGETS {
        let generic_dir = get_generic_deploy_dir(target);
        let aws_dir = get_zig_deploy_dir(UploadTarget::Aws, target);
        let on_prem_dir = get_zig_deploy_dir(UploadTarget::OnPrem, target);
        run_cmd!(mkdir -p $generic_dir $aws_dir $on_prem_dir)?;
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

    // Build Zig projects for all CPU targets (for both aws and on_prem)
    if matches!(deploy_target, DeployTarget::Zig | DeployTarget::All)
        && Path::new(ZIG_REPO_PATH).exists()
    {
        build_zig(zig_build_opt, build_dir, zig_extra_build)?;
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

        // Copy fractalbits-bootstrap to generic directory
        let src_path = format!("target/{}/{}/fractalbits-bootstrap", rust_target, build_dir);
        let generic_dir = format!("prebuilt/deploy/generic/{}", arch);
        let dst_path = format!("{}/fractalbits-bootstrap", generic_dir);
        run_cmd! {
            mkdir -p $generic_dir;
            cp $src_path $dst_path;
        }?;
    }
    Ok(())
}

/// Get generic deploy directory for shared binaries: prebuilt/deploy/generic/{arch}/
fn get_generic_deploy_dir(target: &ArchTarget) -> String {
    format!("prebuilt/deploy/generic/{}", target.arch)
}

/// Get Zig deploy directory for target-specific binaries: prebuilt/deploy/{aws|on_prem}/{arch}/
fn get_zig_deploy_dir(upload_target: UploadTarget, target: &ArchTarget) -> String {
    let deploy_name = match upload_target {
        UploadTarget::Aws => "aws",
        UploadTarget::OnPrem => "on_prem",
    };
    format!("prebuilt/deploy/{}/{}", deploy_name, target.arch)
}

fn build_rust(rust_build_opt: &str, build_dir: &str, api_server_build_env: &[String]) -> CmdResult {
    info!("Building Rust projects for all arch targets");
    let build_envs = cmd_build::get_build_envs();

    for target in ARCH_TARGETS {
        let rust_cpu = target.rust_cpu;
        let rust_target = target.rust_target;
        let arch = target.arch;

        if api_server_build_env.is_empty() {
            run_cmd! {
                info "Building Rust projects for $rust_target ($arch, cpu=$rust_cpu)";
                RUSTFLAGS="-C target-cpu=$rust_cpu"
                $[build_envs] cargo zigbuild
                    --target $rust_target $rust_build_opt --workspace
                    --exclude xtask
                    --exclude fractalbits-bootstrap;
            }?;
        } else {
            run_cmd! {
                info "Building Rust projects for $rust_target ($arch, cpu=$rust_cpu)";
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

fn copy_rust_binaries(target: &ArchTarget, rust_target: &str, build_dir: &str) -> CmdResult {
    let deploy_dir = get_generic_deploy_dir(target);
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

fn build_zig(zig_build_opt: &str, build_dir: &str, zig_extra_build: &[String]) -> CmdResult {
    info!("Building Zig projects for all arch targets (aws and on_prem)");
    let build_envs = cmd_build::get_build_envs();

    // Build for each deploy target with different atomic_write_size and sampling_ratio
    for upload_target in [UploadTarget::Aws, UploadTarget::OnPrem] {
        let (atomic_write_size, sampling_ratio, deploy_name) = match upload_target {
            UploadTarget::Aws => (16384, 1, "aws"),
            UploadTarget::OnPrem => (4096, 4, "on_prem"),
        };

        // Format Zig build options with target-specific settings
        let mut zig_build_with_defaults = vec![
            format!("journal_atomic_write_size={}", atomic_write_size),
            format!("journal_sampling_ratio={}", sampling_ratio),
        ];
        zig_build_with_defaults.extend(zig_extra_build.iter().cloned());
        let zig_extra_opts: Vec<String> = zig_build_with_defaults
            .iter()
            .map(|opt| format!("-D{}", opt))
            .collect();

        for target in ARCH_TARGETS {
            let zig_out_dir = format!(
                "target/{}/{build_dir}/zig-out-{}-{}",
                target.rust_target, deploy_name, target.arch
            );

            let zig_target = target.zig_target;
            let zig_cpu = target.zig_cpu;
            let arch = target.arch;
            let zig_opts = zig_extra_opts.clone();
            run_cmd! {
                info "Building Zig projects for $zig_target ($arch, cpu=$zig_cpu, $deploy_name, atomic_write_size=$atomic_write_size)";
                cd $ZIG_REPO_PATH;
                $[build_envs] zig build
                    -p ../$zig_out_dir
                    -Dtarget=$zig_target -Dcpu=$zig_cpu $zig_build_opt $[zig_opts] 2>&1;
            }?;

            // Copy Zig binaries to target-specific deploy directory
            copy_zig_binaries(upload_target, target, &zig_out_dir)?;
        }
    }
    Ok(())
}

fn copy_zig_binaries(
    upload_target: UploadTarget,
    target: &ArchTarget,
    zig_out_dir: &str,
) -> CmdResult {
    let deploy_dir = get_zig_deploy_dir(upload_target, target);
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

        // Extract warp to generic directory
        let deploy_dir = format!("prebuilt/deploy/generic/{}", arch);
        run_cmd! {
            info "Extracting warp binary to $deploy_dir for $linux_arch";
            tar -xzf third_party/minio/$warp_file -C $deploy_dir warp;
        }?;
    }

    Ok(())
}
