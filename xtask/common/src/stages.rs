use std::collections::{HashMap, VecDeque};

/// A stage definition with dependency declarations.
///
/// Instead of hardcoded numeric prefixes (00-, 10-, ...) for ordering,
/// each stage declares its dependencies explicitly. The DAG topological
/// order is used to compute sequence prefixes for cloud storage keys,
/// so that listing output appears in natural execution order.
#[derive(Debug, Clone, Copy)]
pub struct StageDef {
    pub name: &'static str,
    pub desc: &'static str,
    pub depends_on: &'static [&'static StageDef],
    pub is_global: bool,
    pub timeout_secs: u64,
}

/// A global dependency that has been validated against a stage's transitive
/// `depends_on` graph.
#[derive(Debug, Clone, Copy)]
pub struct VerifiedGlobalDep(StageDef);

impl VerifiedGlobalDep {
    pub fn stage(&self) -> StageDef {
        self.0
    }
}

/// A per-node dependency that has been validated against a stage's transitive
/// `depends_on` graph.
#[derive(Debug, Clone, Copy)]
pub struct VerifiedNodeDep(StageDef);

impl VerifiedNodeDep {
    pub fn stage(&self) -> StageDef {
        self.0
    }
}

/// A global stage that has been validated for completion.
#[derive(Debug, Clone, Copy)]
pub struct VerifiedGlobalStage(StageDef);

impl VerifiedGlobalStage {
    pub fn stage(&self) -> StageDef {
        self.0
    }
}

/// A per-node stage that has been validated for completion.
#[derive(Debug, Clone, Copy)]
pub struct VerifiedNodeStage(StageDef);

impl VerifiedNodeStage {
    pub fn stage(&self) -> StageDef {
        self.0
    }
}

/// Const-compatible string equality (no trait methods in const fn).
const fn str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

impl StageDef {
    /// Look up a dependency by name from this stage's transitive `depends_on`
    /// graph.
    ///
    /// This is a `const fn`, so stage helpers can cache validated proofs in
    /// associated `const`s and turn typos or invalid relationships into build
    /// errors.
    /// ```ignore
    /// const ETCD_READY: VerifiedGlobalDep =
    ///     stages::RSS_INITIALIZED.global_dep("etcd-ready");
    /// ```
    const fn lookup_dep(&self, name: &str) -> StageDef {
        if let Some(dep) = find_dep(self, name) {
            return dep;
        }
        panic!("stage is not a declared dependency")
    }

    pub const fn global_dep(&self, name: &str) -> VerifiedGlobalDep {
        let dep = self.lookup_dep(name);
        if !dep.is_global {
            panic!("dependency is not a global stage");
        }
        VerifiedGlobalDep(dep)
    }

    pub const fn node_dep(&self, name: &str) -> VerifiedNodeDep {
        let dep = self.lookup_dep(name);
        if dep.is_global {
            panic!("dependency is not a per-node stage");
        }
        VerifiedNodeDep(dep)
    }

    pub const fn global_stage(&self) -> VerifiedGlobalStage {
        if !self.is_global {
            panic!("stage is not a global stage");
        }
        VerifiedGlobalStage(*self)
    }

    pub const fn node_stage(&self) -> VerifiedNodeStage {
        if self.is_global {
            panic!("stage is not a per-node stage");
        }
        VerifiedNodeStage(*self)
    }

    /// Returns the cloud storage key name with a sequence prefix derived
    /// from topological order, e.g. "00-instances-ready", "10-etcd-ready".
    /// Uses 2-digit zero-padded prefix so lexicographic order is correct.
    pub fn key_name(&self) -> String {
        let order = topological_order();
        let idx = order
            .iter()
            .position(|&n| n == self.name)
            .expect("stage not in ALL_STAGES");
        format!("{:02}-{}", idx * 10, self.name)
    }
}

const fn find_dep(stage: &StageDef, name: &str) -> Option<StageDef> {
    let mut i = 0;
    while i < stage.depends_on.len() {
        let dep = stage.depends_on[i];
        if str_eq(dep.name, name) {
            return Some(*dep);
        }
        if let Some(found) = find_dep(dep, name) {
            return Some(found);
        }
        i += 1;
    }
    None
}

pub const INSTANCES_READY: StageDef = StageDef {
    name: "instances-ready",
    desc: "Instances ready",
    depends_on: &[],
    is_global: false,
    timeout_secs: 120,
};

pub const ETCD_NODES_REGISTERED: StageDef = StageDef {
    name: "etcd-nodes-registered",
    desc: "etcd nodes registered IPs",
    depends_on: &[&INSTANCES_READY],
    is_global: false,
    timeout_secs: 600,
};

pub const ETCD_READY: StageDef = StageDef {
    name: "etcd-ready",
    desc: "etcd cluster formed",
    depends_on: &[&ETCD_NODES_REGISTERED],
    is_global: true,
    timeout_secs: 600,
};

pub const RSS_INITIALIZED: StageDef = StageDef {
    name: "rss-initialized",
    desc: "RSS config published",
    depends_on: &[&ETCD_READY],
    is_global: true,
    timeout_secs: 600,
};

pub const METADATA_VG_READY: StageDef = StageDef {
    name: "metadata-vg-ready",
    desc: "Metadata VG ready",
    depends_on: &[&RSS_INITIALIZED],
    is_global: true,
    timeout_secs: 600,
};

pub const NSS_FORMATTED: StageDef = StageDef {
    name: "nss-formatted",
    desc: "NSS formatted",
    depends_on: &[&RSS_INITIALIZED],
    is_global: false,
    timeout_secs: 600,
};

pub const NSS_JOURNAL_READY: StageDef = StageDef {
    name: "nss-journal-ready",
    desc: "NSS journal ready",
    depends_on: &[&NSS_FORMATTED, &METADATA_VG_READY],
    is_global: false,
    timeout_secs: 600,
};

pub const BSS_CONFIGURED: StageDef = StageDef {
    name: "bss-configured",
    desc: "BSS configured",
    depends_on: &[&RSS_INITIALIZED],
    is_global: false,
    timeout_secs: 1200, // storage_alloc_mode=.write_zero is slower
};

pub const SERVICES_READY: StageDef = StageDef {
    name: "services-ready",
    desc: "Services ready",
    depends_on: &[&NSS_JOURNAL_READY, &BSS_CONFIGURED],
    is_global: false,
    timeout_secs: 60,
};

/// All stage definitions. The blueprint uses this to build the DAG.
pub const ALL_STAGES: &[&StageDef] = &[
    &INSTANCES_READY,
    &ETCD_NODES_REGISTERED,
    &ETCD_READY,
    &RSS_INITIALIZED,
    &METADATA_VG_READY,
    &NSS_FORMATTED,
    &NSS_JOURNAL_READY,
    &BSS_CONFIGURED,
    &SERVICES_READY,
];

/// Compute topological order of all stages via Kahn's algorithm.
/// Panics if the DAG has cycles (should be caught by tests).
fn topological_order() -> Vec<&'static str> {
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for &stage in ALL_STAGES {
        in_degree.entry(stage.name).or_insert(0);
        for dep in stage.depends_on {
            *in_degree.entry(stage.name).or_insert(0) += 1;
            dependents.entry(dep.name).or_default().push(stage.name);
        }
    }

    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(name, _)| *name)
        .collect();
    let mut order = Vec::new();

    while let Some(name) = queue.pop_front() {
        order.push(name);
        if let Some(deps) = dependents.get(name) {
            for &dep in deps {
                let deg = in_degree.get_mut(dep).expect("in_degree missing");
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(dep);
                }
            }
        }
    }

    assert_eq!(order.len(), ALL_STAGES.len(), "cycle detected in stage DAG");
    order
}

/// Validate that the stage DAG is well-formed:
/// - All depends_on references exist
/// - No cycles (topological sort succeeds)
pub fn validate_dag() -> Result<Vec<&'static str>, String> {
    let by_name: HashMap<&str, &StageDef> = ALL_STAGES.iter().map(|&s| (s.name, s)).collect();

    // Check all depends_on references resolve
    for &stage in ALL_STAGES {
        for dep in stage.depends_on {
            if !by_name.contains_key(dep.name) {
                return Err(format!(
                    "stage '{}' depends on '{}' which does not exist",
                    stage.name, dep.name
                ));
            }
        }
    }

    Ok(topological_order())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dag_is_valid() {
        let order = validate_dag().expect("DAG validation failed");
        assert_eq!(order.len(), ALL_STAGES.len());
    }

    #[test]
    fn instances_ready_comes_first() {
        let order = validate_dag().expect("DAG validation failed");
        assert_eq!(order[0], INSTANCES_READY.name);
    }

    #[test]
    fn services_ready_comes_last() {
        let order = validate_dag().expect("DAG validation failed");
        assert_eq!(*order.last().unwrap(), SERVICES_READY.name);
    }

    #[test]
    fn dependencies_precede_dependents() {
        let order = validate_dag().expect("DAG validation failed");
        let pos = |name: &str| order.iter().position(|&n| n == name).unwrap();

        // ETCD_NODES_REGISTERED depends on INSTANCES_READY
        assert!(pos(INSTANCES_READY.name) < pos(ETCD_NODES_REGISTERED.name));

        // ETCD_READY depends on ETCD_NODES_REGISTERED
        assert!(pos(ETCD_NODES_REGISTERED.name) < pos(ETCD_READY.name));

        // RSS_INITIALIZED depends on ETCD_READY
        assert!(pos(ETCD_READY.name) < pos(RSS_INITIALIZED.name));

        // NSS_JOURNAL_READY depends on both NSS_FORMATTED and METADATA_VG_READY
        assert!(pos(NSS_FORMATTED.name) < pos(NSS_JOURNAL_READY.name));
        assert!(pos(METADATA_VG_READY.name) < pos(NSS_JOURNAL_READY.name));

        // BSS_CONFIGURED and NSS_FORMATTED both depend on RSS_INITIALIZED (parallel branches)
        assert!(pos(RSS_INITIALIZED.name) < pos(BSS_CONFIGURED.name));
        assert!(pos(RSS_INITIALIZED.name) < pos(NSS_FORMATTED.name));
    }

    #[test]
    fn dep_returns_declared_dependency() {
        // const { } forces compile-time evaluation — a typo here is a build error
        let dep = const { RSS_INITIALIZED.global_dep("etcd-ready") };
        assert_eq!(dep.stage().name, ETCD_READY.name);
    }

    #[test]
    fn dep_returns_transitive_dependency() {
        let dep = const { SERVICES_READY.global_dep("etcd-ready") };
        assert_eq!(dep.stage().name, ETCD_READY.name);
    }

    #[test]
    #[should_panic(expected = "stage is not a declared dependency")]
    fn dep_panics_on_undeclared_dependency() {
        // Runtime call (without const { }) still panics for dynamic lookups
        RSS_INITIALIZED.global_dep("metadata-vg-ready");
    }

    #[test]
    fn node_dep_returns_declared_dependency() {
        let dep = const { SERVICES_READY.node_dep("nss-journal-ready") };
        assert_eq!(dep.stage().name, NSS_JOURNAL_READY.name);
    }

    #[test]
    #[should_panic(expected = "dependency is not a global stage")]
    fn global_dep_panics_on_node_stage() {
        SERVICES_READY.global_dep("nss-formatted");
    }

    #[test]
    #[should_panic(expected = "dependency is not a per-node stage")]
    fn node_dep_panics_on_global_stage() {
        SERVICES_READY.node_dep("rss-initialized");
    }

    #[test]
    fn global_stage_returns_stage() {
        let stage = const { ETCD_READY.global_stage() };
        assert_eq!(stage.stage().name, ETCD_READY.name);
    }

    #[test]
    fn node_stage_returns_stage() {
        let stage = const { SERVICES_READY.node_stage() };
        assert_eq!(stage.stage().name, SERVICES_READY.name);
    }

    #[test]
    #[should_panic(expected = "stage is not a global stage")]
    fn global_stage_panics_on_node_stage() {
        SERVICES_READY.global_stage();
    }

    #[test]
    #[should_panic(expected = "stage is not a per-node stage")]
    fn node_stage_panics_on_global_stage() {
        ETCD_READY.node_stage();
    }

    #[test]
    fn key_names_have_sequence_prefixes() {
        // Cloud storage key names should have numeric prefixes for natural ordering
        let key = INSTANCES_READY.key_name();
        assert!(key.starts_with("00-"), "got: {key}");
        assert!(key.ends_with("instances-ready"), "got: {key}");

        // Services-ready should have the highest prefix
        let last = SERVICES_READY.key_name();
        let first = INSTANCES_READY.key_name();
        assert!(
            last > first,
            "services-ready ({last}) should sort after instances-ready ({first})"
        );
    }
}
