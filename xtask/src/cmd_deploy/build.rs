use crate::docker_build::{
    BinarySources, DockerBuildConfig, build_docker_image, get_host_arch, stage_binaries_for_docker,
};
use crate::etcd_utils::download_etcd_for_deploy;
use crate::etcd_utils::resolve_etcd_dir_for_arch;
use crate::*;
use std::io::Error;
use std::path::Path;
use std::time::Duration;
use xtask_common::DeployTarget as UploadTarget;

use super::common::{ARCH_TARGETS, AWS_CPU_TARGETS, ArchTarget, RUST_BINS, ZIG_BINS};
use super::upload::upload_with_endpoint;

pub fn build(
    target: DeployBuildTarget,
    release_mode: bool,
    zig_extra_build: &[String],
    api_server_build_env: &[String],
    for_on_prem: bool,
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

    // Create deploy directories: generic (shared) and AWS CPU-specific
    for target in ARCH_TARGETS {
        let generic_dir = get_generic_deploy_dir(target);
        run_cmd!(mkdir -p $generic_dir)?;
    }
    for target in AWS_CPU_TARGETS {
        let aws_cpu_dir = get_aws_cpu_deploy_dir(target);
        run_cmd!(mkdir -p $aws_cpu_dir)?;
    }

    // Build fractalbits-bootstrap separately for each architecture without CPU flags
    if matches!(
        target,
        DeployBuildTarget::Bootstrap | DeployBuildTarget::Rust | DeployBuildTarget::All
    ) {
        build_bootstrap(rust_build_opt, build_dir)?;
    }

    // Build other Rust projects with CPU-specific optimizations
    if matches!(target, DeployBuildTarget::Rust | DeployBuildTarget::All) {
        build_rust(rust_build_opt, build_dir, api_server_build_env)?;
    }

    // Build Zig projects for all CPU targets (for both aws and on_prem)
    if matches!(target, DeployBuildTarget::Zig | DeployBuildTarget::All)
        && Path::new(ZIG_REPO_PATH).exists()
    {
        build_zig(zig_build_opt, build_dir, zig_extra_build)?;
    }

    // Build and copy UI
    if matches!(target, DeployBuildTarget::Ui | DeployBuildTarget::All)
        && Path::new(UI_REPO_PATH).exists()
    {
        build_ui()?;
    }

    // Download (extract) warp binary for each architecture
    if target == DeployBuildTarget::All {
        download_warp_binaries()?;
        download_etcd_for_deploy()?;
    }

    // Build and export Docker image (only for on-prem deployment)
    if for_on_prem && target == DeployBuildTarget::All {
        build_docker_with_prepopulated_binaries()?;
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

/// Get AWS CPU-specific deploy directory: prebuilt/deploy/aws/{arch}/{cpu_name}/
fn get_aws_cpu_deploy_dir(target: &ArchTarget) -> String {
    format!("prebuilt/deploy/aws/{}/{}", target.arch, target.cpu_name)
}

fn build_rust(rust_build_opt: &str, build_dir: &str, api_server_build_env: &[String]) -> CmdResult {
    info!("Building Rust projects for all arch targets (generic + AWS CPU-specific)");
    let build_envs = cmd_build::get_build_envs();

    // Build for ARCH_TARGETS (generic/baseline builds)
    for target in ARCH_TARGETS {
        let rust_cpu = target.rust_cpu;
        let rust_target = target.rust_target;
        let arch = target.arch;

        if api_server_build_env.is_empty() {
            run_cmd! {
                info "Building Rust projects for $rust_target ($arch, cpu=$rust_cpu) [generic]";
                RUSTFLAGS="-C target-cpu=$rust_cpu"
                $[build_envs] cargo zigbuild
                    --target $rust_target $rust_build_opt --workspace
                    --exclude xtask
                    --exclude fractalbits-bootstrap;
            }?;
        } else {
            run_cmd! {
                info "Building Rust projects for $rust_target ($arch, cpu=$rust_cpu) [generic]";
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

        // Copy Rust binaries to generic directory (excluding fractalbits-bootstrap)
        copy_rust_binaries_to_generic(target, rust_target, build_dir)?;
    }

    // Build for AWS_CPU_TARGETS (CPU-specific AWS builds)
    for target in AWS_CPU_TARGETS {
        let rust_cpu = target.rust_cpu;
        let rust_target = target.rust_target;
        let arch = target.arch;
        let cpu_name = target.cpu_name;

        if api_server_build_env.is_empty() {
            run_cmd! {
                info "Building Rust projects for $rust_target ($arch, cpu=$rust_cpu) [aws/$cpu_name]";
                RUSTFLAGS="-C target-cpu=$rust_cpu"
                $[build_envs] cargo zigbuild
                    --target $rust_target $rust_build_opt --workspace
                    --exclude xtask
                    --exclude fractalbits-bootstrap;
            }?;
        } else {
            run_cmd! {
                info "Building Rust projects for $rust_target ($arch, cpu=$rust_cpu) [aws/$cpu_name]";
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

        // Copy Rust binaries to AWS CPU-specific directory (excluding bootstrap/etcd/warp)
        copy_rust_binaries_to_aws_cpu(target, rust_target, build_dir)?;
    }
    Ok(())
}

fn copy_rust_binaries_to_generic(
    target: &ArchTarget,
    rust_target: &str,
    build_dir: &str,
) -> CmdResult {
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

fn copy_rust_binaries_to_aws_cpu(
    target: &ArchTarget,
    rust_target: &str,
    build_dir: &str,
) -> CmdResult {
    let deploy_dir = get_aws_cpu_deploy_dir(target);
    // Copy all Rust binaries except fractalbits-bootstrap (which comes from generic)
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
    info!("Building Zig projects for all arch targets (generic + AWS CPU-specific)");
    let build_envs = cmd_build::get_build_envs();

    // Build for generic (on-prem settings: atomic_write_size=4096, sampling_ratio=4)
    let generic_atomic_write_size = 4096;
    let generic_sampling_ratio = 4;

    let mut zig_build_with_defaults = vec![
        format!("journal_atomic_write_size={}", generic_atomic_write_size),
        format!("journal_sampling_ratio={}", generic_sampling_ratio),
    ];
    zig_build_with_defaults.extend(zig_extra_build.iter().cloned());
    let zig_extra_opts: Vec<String> = zig_build_with_defaults
        .iter()
        .map(|opt| format!("-D{}", opt))
        .collect();

    for target in ARCH_TARGETS {
        let zig_out_dir = format!(
            "target/{}/{build_dir}/zig-out-generic-{}",
            target.rust_target, target.arch
        );

        let zig_target = target.zig_target;
        let zig_cpu = target.zig_cpu;
        let arch = target.arch;
        let zig_opts = zig_extra_opts.clone();
        run_cmd! {
            info "Building Zig projects for $zig_target ($arch, cpu=$zig_cpu) [generic]";
            cd $ZIG_REPO_PATH;
            $[build_envs] zig build
                -p ../$zig_out_dir
                -Dtarget=$zig_target -Dcpu=$zig_cpu $zig_build_opt $[zig_opts] 2>&1;
        }?;

        // Copy Zig binaries to generic directory
        copy_zig_binaries_to_generic(target, &zig_out_dir)?;
    }

    // Build for AWS CPU targets (aws settings: atomic_write_size=16384, sampling_ratio=1)
    let aws_atomic_write_size = 16384;
    let aws_sampling_ratio = 1;

    let mut zig_build_with_defaults = vec![
        format!("journal_atomic_write_size={}", aws_atomic_write_size),
        format!("journal_sampling_ratio={}", aws_sampling_ratio),
    ];
    zig_build_with_defaults.extend(zig_extra_build.iter().cloned());
    let zig_extra_opts: Vec<String> = zig_build_with_defaults
        .iter()
        .map(|opt| format!("-D{}", opt))
        .collect();

    for target in AWS_CPU_TARGETS {
        let zig_out_dir = format!(
            "target/{}/{build_dir}/zig-out-aws-{}-{}",
            target.rust_target, target.arch, target.cpu_name
        );

        let zig_target = target.zig_target;
        let zig_cpu = target.zig_cpu;
        let arch = target.arch;
        let cpu_name = target.cpu_name;
        let zig_opts = zig_extra_opts.clone();
        run_cmd! {
            info "Building Zig projects for $zig_target ($arch, cpu=$zig_cpu) [aws/$cpu_name]";
            cd $ZIG_REPO_PATH;
            $[build_envs] zig build
                -p ../$zig_out_dir
                -Dtarget=$zig_target -Dcpu=$zig_cpu $zig_build_opt $[zig_opts] 2>&1;
        }?;

        // Copy Zig binaries to AWS CPU-specific directory
        copy_zig_binaries_to_aws_cpu(target, &zig_out_dir)?;
    }
    Ok(())
}

fn copy_zig_binaries_to_generic(target: &ArchTarget, zig_out_dir: &str) -> CmdResult {
    let deploy_dir = get_generic_deploy_dir(target);
    for bin in ZIG_BINS {
        let src_path = format!("{}/bin/{}", zig_out_dir, bin);
        let dst_path = format!("{}/{}", deploy_dir, bin);
        run_cmd!(cp $src_path $dst_path)?;
    }
    Ok(())
}

fn copy_zig_binaries_to_aws_cpu(target: &ArchTarget, zig_out_dir: &str) -> CmdResult {
    let deploy_dir = get_aws_cpu_deploy_dir(target);
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

/// Build all binaries from source for both architectures (aarch64 and x86_64)
fn build_all_binaries_for_docker() -> CmdResult {
    let build_envs = cmd_build::get_build_envs();

    run_cmd! {
        info "Ensuring required Rust targets are installed";
        rustup target add x86_64-unknown-linux-gnu;
        rustup target add aarch64-unknown-linux-gnu;
    }?;

    for target in ARCH_TARGETS {
        let arch = target.arch;
        let rust_target = target.rust_target;
        let rust_cpu = target.rust_cpu;
        let zig_target = target.zig_target;
        let zig_cpu = target.zig_cpu;
        let build_dir = format!("target/{}/release", rust_target);

        // Build main repo Rust binaries (api_server, container-all-in-one)
        run_cmd! {
            info "Building api_server and container-all-in-one for $arch";
            RUSTFLAGS="-C target-cpu=$rust_cpu"
            $[build_envs] cargo zigbuild --release --target $rust_target
                -p api_server
                -p container-all-in-one;
        }?;

        // Build ha packages if they exist
        if Path::new("crates/ha").exists() {
            run_cmd! {
                info "Building nss_role_agent for $arch";
                RUSTFLAGS="-C target-cpu=$rust_cpu"
                $[build_envs] cargo zigbuild --release --target $rust_target
                    -p nss_role_agent;
            }?;
        }

        // Build root_server packages if they exist
        if Path::new("crates/root_server").exists() {
            run_cmd! {
                info "Building root_server and rss_admin for $arch";
                RUSTFLAGS="-C target-cpu=$rust_cpu"
                $[build_envs] cargo zigbuild --release --target $rust_target
                    -p root_server
                    -p rss_admin;
            }?;
        }

        // Build zig servers if core repo exists
        if Path::new(ZIG_REPO_PATH).exists() {
            let zig_out = format!("{}/zig-out-docker", build_dir);
            run_cmd! {
                info "Building Zig binaries for Docker ($arch)";
                cd $ZIG_REPO_PATH;
                $[build_envs] zig build -p ../$zig_out
                    --release=safe
                    -Dtarget=$zig_target -Dcpu=$zig_cpu
                    -Djournal_atomic_write_size=4096
                    -Djournal_sampling_ratio=4 2>&1;
            }?;
        }
    }
    Ok(())
}

fn build_docker_with_prepopulated_binaries() -> CmdResult {
    const CONTAINER_NAME: &str = "fractalbits-prepopulate";
    const IMAGE_NAME: &str = "fractalbits-prepopulate-base";

    info!("Building Docker images for both architectures with pre-populated binaries...");

    // Build all binaries from source for both architectures
    build_all_binaries_for_docker()?;

    // Ensure etcd is downloaded for both architectures
    download_etcd_for_deploy()?;

    // Prepare staging directory
    let staging_dir = "target/docker-staging";
    run_cmd!(rm -rf $staging_dir)?;

    // Stage binaries and build base images for each architecture
    for target in ARCH_TARGETS {
        let arch = target.arch;
        let rust_target = target.rust_target;
        let build_dir = format!("target/{}/release", rust_target);
        let zig_out_dir = format!("{}/zig-out-docker/bin", build_dir);
        let prebuilt_dir = format!("prebuilt/dev/{}", arch);
        let bin_subdir = format!("bin-{}", arch);

        // Stage binaries using unified function
        let sources = BinarySources {
            rust_bin_dir: &build_dir,
            zig_bin_dir: Some(&zig_out_dir),
            prebuilt_dir: &prebuilt_dir,
            etcd_dir: &resolve_etcd_dir_for_arch(arch),
            prefer_built: true,
        };
        stage_binaries_for_docker(&sources, staging_dir, &bin_subdir)?;

        // Build base image using unified function
        let config = DockerBuildConfig {
            image_name: IMAGE_NAME,
            tag: arch,
            arch: Some(arch),
            platform: Some(target.docker_platform),
            staging_dir,
            bin_subdir: &bin_subdir,
            include_volume: false,
            data_source: None,
        };
        build_docker_image(&config)?;
    }

    // Use the host architecture image for pre-population
    let host_arch = get_host_arch();
    let host_image = format!("{}:{}", IMAGE_NAME, host_arch);

    // Clean up any existing container
    let _ = run_cmd!(ignore docker stop $CONTAINER_NAME 2>/dev/null);
    let _ = run_cmd!(ignore docker rm -f $CONTAINER_NAME 2>/dev/null);

    // Start the Docker container for pre-population
    info!("Starting Docker container to populate with binaries...");
    run_cmd!(
        docker run -d --privileged --name $CONTAINER_NAME
            -p 8080:8080 -p 18080:18080
            $host_image
    )?;

    // Wait for the container to be healthy
    info!("Waiting for container to be ready...");
    let max_attempts = 60;
    let mut ready = false;
    for i in 1..=max_attempts {
        std::thread::sleep(Duration::from_secs(5));
        let health_check = std::process::Command::new("curl")
            .args([
                "-sf",
                "--max-time",
                "5",
                "http://localhost:18080/mgmt/health",
            ])
            .output();
        if health_check.is_ok_and(|o| o.status.success()) {
            info!("Container is ready after {} attempts", i);
            ready = true;
            break;
        }
        if i % 6 == 0 {
            info!(
                "Still waiting for container... (attempt {}/{})",
                i, max_attempts
            );
        }
    }

    if !ready {
        run_cmd!(docker logs $CONTAINER_NAME 2>&1 | tail -50)?;
        let _ = run_cmd!(docker stop $CONTAINER_NAME);
        let _ = run_cmd!(docker rm -f $CONTAINER_NAME);
        return Err(Error::other(
            "Timed out waiting for Docker container to be ready",
        ));
    }

    // Upload binaries to local fractalbits S3 service
    info!("Uploading binaries to local fractalbits container...");
    upload_with_endpoint(UploadTarget::OnPrem, Some("localhost:8080"))?;

    // Stop the container (preserves data)
    info!("Stopping container...");
    run_cmd!(docker stop $CONTAINER_NAME)?;

    // Export the pre-populated data from the running container
    info!("Extracting pre-populated data...");
    let data_extract_dir = format!("{}/data-extract", staging_dir);
    run_cmd! {
        rm -rf $data_extract_dir;
        mkdir -p $data_extract_dir;
        docker cp $CONTAINER_NAME:/data $data_extract_dir/;
    }?;

    // Remove the container
    run_cmd!(docker rm -f $CONTAINER_NAME)?;

    // Build final images for each architecture with pre-populated data
    info!("Building final images with pre-populated data...");
    run_cmd!(mkdir -p target/on-prem)?;

    for target in ARCH_TARGETS {
        let arch = target.arch;
        let bin_subdir = format!("bin-{}", arch);
        let final_image = format!("fractalbits:{}", arch);

        // Build final image with pre-populated data using unified function
        let config = DockerBuildConfig {
            image_name: "fractalbits",
            tag: arch,
            arch: Some(arch),
            platform: Some(target.docker_platform),
            staging_dir,
            bin_subdir: &bin_subdir,
            include_volume: false,
            data_source: Some("data-extract/data"),
        };
        build_docker_image(&config)?;

        // Export the image
        let output_file = format!("target/on-prem/fractalbits-{}.tar.gz", arch);
        info!("Exporting {} image to {}...", arch, output_file);
        run_cmd!(docker save $final_image | gzip > $output_file)?;

        // Clean up
        let base_image = format!("{}:{}", IMAGE_NAME, arch);
        let _ = run_cmd!(docker rmi $base_image $final_image 2>/dev/null);
    }

    info!("Multi-arch Docker images exported to target/on-prem/");
    Ok(())
}
