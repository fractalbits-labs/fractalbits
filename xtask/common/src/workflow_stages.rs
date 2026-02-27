/// Bootstrap workflow stage definition
pub struct StageInfo {
    pub name: &'static str,
    pub desc: &'static str,
    pub is_global: bool,
}

/// Ordered list of bootstrap workflow stages
pub const STAGES: &[StageInfo] = &[
    StageInfo {
        name: names::INSTANCES_READY,
        desc: "Instances ready",
        is_global: false,
    },
    StageInfo {
        name: names::ETCD_READY,
        desc: "etcd cluster formed",
        is_global: true,
    },
    StageInfo {
        name: names::RSS_INITIALIZED,
        desc: "RSS config published",
        is_global: true,
    },
    StageInfo {
        name: names::METADATA_VG_READY,
        desc: "Metadata VG ready",
        is_global: true,
    },
    StageInfo {
        name: names::NSS_FORMATTED,
        desc: "NSS formatted",
        is_global: false,
    },
    StageInfo {
        name: names::MIRRORD_READY,
        desc: "Mirrord ready",
        is_global: false,
    },
    StageInfo {
        name: names::NSS_JOURNAL_READY,
        desc: "NSS journal ready",
        is_global: false,
    },
    StageInfo {
        name: names::BSS_CONFIGURED,
        desc: "BSS configured",
        is_global: false,
    },
    StageInfo {
        name: names::SERVICES_READY,
        desc: "Services ready",
        is_global: false,
    },
];

/// Stage name constants
pub mod names {
    pub const INSTANCES_READY: &str = "00-instances-ready";
    pub const ETCD_READY: &str = "10-etcd-ready";
    pub const RSS_INITIALIZED: &str = "20-rss-initialized";
    pub const METADATA_VG_READY: &str = "25-metadata-vg-ready";
    pub const NSS_FORMATTED: &str = "30-nss-formatted";
    pub const MIRRORD_READY: &str = "35-mirrord-ready";
    pub const NSS_JOURNAL_READY: &str = "40-nss-journal-ready";
    pub const BSS_CONFIGURED: &str = "50-bss-configured";
    pub const SERVICES_READY: &str = "60-services-ready";
}

/// Timeout constants for each stage (in seconds)
pub mod timeouts {
    pub const INSTANCES_READY: u64 = 120;
    pub const ETCD_READY: u64 = 300;
    pub const RSS_INITIALIZED: u64 = 300;
    pub const METADATA_VG_READY: u64 = 300;
    pub const NSS_FORMATTED: u64 = 300;
    pub const MIRRORD_READY: u64 = 120;
    pub const NSS_JOURNAL_READY: u64 = 120;
    pub const BSS_CONFIGURED: u64 = 300;
    pub const SERVICES_READY: u64 = 60;
}
