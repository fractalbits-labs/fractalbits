use cmd_lib::*;

pub use xtask_common::DeployTarget;

pub struct VpcConfig {
    pub template: Option<crate::VpcTemplate>,
    pub num_api_servers: u32,
    pub num_bench_clients: u32,
    pub num_bss_nodes: u32,
    pub with_bench: bool,
    pub bss_instance_type: String,
    pub api_server_instance_type: String,
    pub bench_client_instance_type: String,
    pub az: Option<String>,
    pub root_server_ha: bool,
    pub rss_backend: crate::RssBackend,
    pub ssm_bootstrap: bool,
    pub journal_type: crate::JournalType,
    pub watch_bootstrap: bool,
    pub skip_upload: bool,
    pub simulate_on_prem: bool,
    pub use_generic_binaries: bool,
}

#[derive(Clone)]
pub(super) struct ArchTarget {
    pub arch: &'static str,
    pub rust_target: &'static str,
    pub rust_cpu: &'static str,
    pub zig_target: &'static str,
    pub zig_cpu: &'static str,
    pub docker_platform: &'static str,
    pub cpu_name: &'static str,
}

/// Baseline targets for generic builds (used for on-prem and development)
pub(super) const ARCH_TARGETS: &[ArchTarget] = &[
    // aarch64: Neoverse N1 baseline - covers Graviton2/3/4, Ampere Altra, and most modern ARM servers
    // Includes: NEON SIMD, LSE atomics, crypto extensions (AES/SHA), CRC32
    ArchTarget {
        arch: "aarch64",
        rust_target: "aarch64-unknown-linux-gnu",
        rust_cpu: "neoverse-n1",
        zig_target: "aarch64-linux-gnu",
        zig_cpu: "neoverse_n1",
        docker_platform: "linux/arm64",
        cpu_name: "neoverse-n1",
    },
    // x86_64: x86-64-v3 (AVX2, FMA, BMI1/2) - Haswell+ (2013), Excavator+ (2015)
    ArchTarget {
        arch: "x86_64",
        rust_target: "x86_64-unknown-linux-gnu",
        rust_cpu: "x86-64-v3",
        zig_target: "x86_64-linux-gnu",
        zig_cpu: "x86_64_v3",
        docker_platform: "linux/amd64",
        cpu_name: "x86-64-v3",
    },
];

/// CPU-specific targets for AWS deployments (optimized for specific instance types)
pub(super) const AWS_CPU_TARGETS: &[ArchTarget] = &[
    // aarch64: Neoverse N1 (Graviton2 and Graviton3 for compatibility)
    ArchTarget {
        arch: "aarch64",
        rust_target: "aarch64-unknown-linux-gnu",
        rust_cpu: "neoverse-n1",
        zig_target: "aarch64-linux-gnu",
        zig_cpu: "neoverse_n1",
        docker_platform: "linux/arm64",
        cpu_name: "neoverse-n1",
    },
    // aarch64: Neoverse N2 (Graviton4)
    ArchTarget {
        arch: "aarch64",
        rust_target: "aarch64-unknown-linux-gnu",
        rust_cpu: "neoverse-n2",
        zig_target: "aarch64-linux-gnu",
        zig_cpu: "neoverse_n2",
        docker_platform: "linux/arm64",
        cpu_name: "neoverse-n2",
    },
    // x86_64: Broadwell (i3)
    ArchTarget {
        arch: "x86_64",
        rust_target: "x86_64-unknown-linux-gnu",
        rust_cpu: "broadwell",
        zig_target: "x86_64-linux-gnu",
        zig_cpu: "broadwell",
        docker_platform: "linux/amd64",
        cpu_name: "broadwell",
    },
    // x86_64: Skylake (i3en)
    ArchTarget {
        arch: "x86_64",
        rust_target: "x86_64-unknown-linux-gnu",
        rust_cpu: "skylake",
        zig_target: "x86_64-linux-gnu",
        zig_cpu: "skylake",
        docker_platform: "linux/amd64",
        cpu_name: "skylake",
    },
];

pub(super) const RUST_BINS: &[&str] = &[
    "fractalbits-bootstrap",
    "root_server",
    "api_server",
    "nss_role_agent",
    "rss_admin",
    "rewrk_rpc",
];

pub(super) const ZIG_BINS: &[&str] = &["nss_server", "bss_server", "mirrord"];

// Binary constants for Docker image builds

/// Rust binaries built from the main repo (always built from source for Docker)
pub const LOCAL_RUST_BINS: &[&str] = &["api_server", "container-all-in-one"];

/// Rust binaries from external repos (may use prebuilt fallback)
pub const EXTERNAL_RUST_BINS: &[&str] = &["root_server", "rss_admin", "nss_role_agent"];

/// Zig binaries needed for Docker images (subset of ZIG_BINS)
pub const DOCKER_ZIG_BINS: &[&str] = &["bss_server", "nss_server"];

// Dockerfile generation utilities

/// Configuration for Dockerfile generation
pub struct DockerfileConfig {
    /// Source path for binaries (e.g., "bin/" or "bin-aarch64/")
    pub bin_source: String,
    /// Optional: copy pre-populated data from this path
    pub data_source: Option<String>,
    /// Include VOLUME directive (for development images)
    pub include_volume: bool,
}

/// Generate Dockerfile content with the given configuration
pub fn generate_dockerfile_content(config: &DockerfileConfig) -> String {
    let data_copy = config
        .data_source
        .as_ref()
        .map(|src| format!("COPY {}/ /data/\n", src))
        .unwrap_or_default();

    let volume_directive = if config.include_volume {
        "VOLUME /data\n\n"
    } else {
        ""
    };

    format!(
        r#"FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/fractalbits/bin /opt/fractalbits/etc /data

COPY {}/ /opt/fractalbits/bin/
{data_copy}
RUN chmod +x /opt/fractalbits/bin/*

ENV PATH="/opt/fractalbits/bin:$PATH"
ENV RUST_LOG=info

EXPOSE 8080 18080 2379

HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD curl -sf http://localhost:18080/mgmt/health || exit 1

{volume_directive}ENTRYPOINT ["container-all-in-one"]
CMD ["--bin-dir=/opt/fractalbits/bin", "--data-dir=/data"]
"#,
        config.bin_source
    )
}

/// Configuration for unified Docker image build
pub struct DockerBuildConfig<'a> {
    /// Docker image name
    pub image_name: &'a str,
    /// Docker image tag
    pub tag: &'a str,
    /// Target architecture (e.g., "x86_64" or "aarch64"). None = host arch
    pub arch: Option<&'a str>,
    /// Docker platform string (e.g., "linux/amd64"). Derived from arch if None
    pub platform: Option<&'a str>,
    /// Staging directory for Docker build context
    pub staging_dir: &'a str,
    /// Subdirectory name for binaries within staging (e.g., "bin" or "bin-aarch64")
    pub bin_subdir: &'a str,
    /// Include VOLUME directive in Dockerfile
    pub include_volume: bool,
    /// Optional data source directory to copy into image
    pub data_source: Option<&'a str>,
}

/// Source paths for binaries to stage
pub struct BinarySources<'a> {
    /// Directory containing Rust binaries (e.g., "target/release" or "target/aarch64-unknown-linux-gnu/release")
    pub rust_bin_dir: &'a str,
    /// Directory containing Zig binaries (e.g., "target/release/zig-out/bin")
    pub zig_bin_dir: Option<&'a str>,
    /// Directory containing prebuilt binaries for fallback
    pub prebuilt_dir: &'a str,
    /// Directory containing etcd binaries
    pub etcd_dir: &'a str,
    /// If true, prefer built binaries over prebuilt (with fallback)
    pub prefer_built: bool,
}

/// Stage binaries for Docker build
pub fn stage_binaries_for_docker(
    sources: &BinarySources,
    staging_dir: &str,
    bin_subdir: &str,
) -> crate::CmdResult {
    use std::path::Path;

    let bin_staging = format!("{}/{}", staging_dir, bin_subdir);
    run_cmd!(mkdir -p $bin_staging)?;

    // Copy local Rust binaries (always from rust_bin_dir)
    for bin in LOCAL_RUST_BINS {
        let src = format!("{}/{}", sources.rust_bin_dir, bin);
        run_cmd!(cp $src $bin_staging/)?;
    }

    // Copy external Rust binaries
    for bin in EXTERNAL_RUST_BINS {
        if sources.prefer_built {
            let built_path = format!("{}/{}", sources.rust_bin_dir, bin);
            if Path::new(&built_path).exists() {
                run_cmd!(cp $built_path $bin_staging/)?;
            } else {
                info!("{} not found in build, using prebuilt", bin);
                let prebuilt_path = format!("{}/{}", sources.prebuilt_dir, bin);
                run_cmd!(cp $prebuilt_path $bin_staging/)?;
            }
        } else {
            let prebuilt_path = format!("{}/{}", sources.prebuilt_dir, bin);
            run_cmd!(cp $prebuilt_path $bin_staging/)?;
        }
    }

    // Copy Zig binaries
    for bin in DOCKER_ZIG_BINS {
        if sources.prefer_built {
            if let Some(zig_dir) = sources.zig_bin_dir {
                let built_path = format!("{}/{}", zig_dir, bin);
                if Path::new(&built_path).exists() {
                    run_cmd!(cp $built_path $bin_staging/)?;
                    continue;
                }
            }
            info!("{} not found in build, using prebuilt", bin);
        }
        let prebuilt_path = format!("{}/{}", sources.prebuilt_dir, bin);
        run_cmd!(cp $prebuilt_path $bin_staging/)?;
    }

    // Copy etcd binaries
    let etcd_path = format!("{}/etcd", sources.etcd_dir);
    let etcdctl_path = format!("{}/etcdctl", sources.etcd_dir);
    run_cmd! {
        cp $etcd_path $bin_staging/;
        cp $etcdctl_path $bin_staging/;
    }?;

    Ok(())
}

/// Build a Docker image with the given configuration
pub fn build_docker_image(config: &DockerBuildConfig) -> crate::CmdResult {
    // Determine platform
    let platform = match config.platform {
        Some(p) => p.to_string(),
        None => {
            let arch = config.arch.unwrap_or(if cfg!(target_arch = "aarch64") {
                "aarch64"
            } else {
                "x86_64"
            });
            if arch == "aarch64" {
                "linux/arm64".to_string()
            } else {
                "linux/amd64".to_string()
            }
        }
    };

    // Generate and write Dockerfile
    let dockerfile_config = DockerfileConfig {
        bin_source: config.bin_subdir.to_string(),
        data_source: config.data_source.map(|s| s.to_string()),
        include_volume: config.include_volume,
    };
    let dockerfile_content = generate_dockerfile_content(&dockerfile_config);
    let dockerfile_path = format!("{}/Dockerfile", config.staging_dir);
    std::fs::write(&dockerfile_path, dockerfile_content)?;

    // Build the image
    let image_name = config.image_name;
    let tag = config.tag;
    let staging_dir = config.staging_dir;
    info!(
        "Building Docker image {}:{} for {}...",
        image_name, tag, platform
    );

    let image_id = run_fun! {
        docker buildx build --platform $platform --no-cache -q -t "${image_name}:${tag}" -f $dockerfile_path --load $staging_dir
    }?;

    let short_id = image_id
        .trim()
        .trim_start_matches("sha256:")
        .chars()
        .take(12)
        .collect::<String>();

    info!("Docker image built: {}:{} ({})", image_name, tag, short_id);
    Ok(())
}

/// Get host architecture as a string
pub fn get_host_arch() -> String {
    run_fun!(arch)
        .unwrap_or_else(|_| "x86_64".to_string())
        .trim()
        .to_string()
}

pub fn get_bootstrap_bucket_name(deploy_target: DeployTarget) -> FunResult {
    match deploy_target {
        DeployTarget::OnPrem => Ok("fractalbits-bootstrap".to_string()),
        DeployTarget::Aws => {
            let region = run_fun!(aws configure get region)?;
            let account_id = run_fun!(aws sts get-caller-identity --query Account --output text)?;
            Ok(format!("fractalbits-bootstrap-{region}-{account_id}"))
        }
    }
}
