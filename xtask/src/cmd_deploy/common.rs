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
    pub deploy_os: crate::DeployOS,
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
