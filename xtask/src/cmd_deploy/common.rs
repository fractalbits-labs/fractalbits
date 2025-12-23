pub use xtask_common::VpcTarget;

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
}

#[derive(Clone)]
pub(super) struct CpuTarget {
    pub name: &'static str,
    pub arch: &'static str,
    pub rust_target: &'static str,
    pub rust_cpu: &'static str,
    pub zig_target: &'static str,
    pub zig_cpu: &'static str,
}

pub(super) const CPU_TARGETS: &[CpuTarget] = &[
    CpuTarget {
        name: "graviton3",
        arch: "aarch64",
        rust_target: "aarch64-unknown-linux-gnu",
        rust_cpu: "neoverse-v1",
        zig_target: "aarch64-linux-gnu",
        zig_cpu: "neoverse_v1",
    },
    CpuTarget {
        name: "graviton4",
        arch: "aarch64",
        rust_target: "aarch64-unknown-linux-gnu",
        rust_cpu: "neoverse-v2",
        zig_target: "aarch64-linux-gnu",
        zig_cpu: "neoverse_v2",
    },
    CpuTarget {
        name: "i3",
        arch: "x86_64",
        rust_target: "x86_64-unknown-linux-gnu",
        rust_cpu: "", // for bss only, no rust binaries
        zig_target: "x86_64-linux-gnu",
        zig_cpu: "broadwell",
    },
    CpuTarget {
        name: "i3en",
        arch: "x86_64",
        rust_target: "x86_64-unknown-linux-gnu",
        rust_cpu: "", // for bss only, no rust binaries
        zig_target: "x86_64-linux-gnu",
        zig_cpu: "skylake",
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

pub(super) const ZIG_BINS: &[&str] = &["nss_server", "bss_server", "test_fractal_art"];
