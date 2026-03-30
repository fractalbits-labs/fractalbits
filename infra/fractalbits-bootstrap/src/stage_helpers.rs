use crate::workflow::{WorkflowBarrier, stages};
use cmd_lib::CmdResult;
use std::io::Error;
use xtask_common::stages::{VerifiedGlobalDep, VerifiedNodeDep, VerifiedNodeStage};

pub struct InstancesReadyStage;

impl InstancesReadyStage {
    const STAGE: VerifiedNodeStage = const { stages::INSTANCES_READY.node_stage() };

    pub fn complete(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.complete_node_stage(Self::STAGE, None)
    }

    pub fn complete_with_metadata(
        barrier: &WorkflowBarrier,
        metadata: serde_json::Value,
    ) -> CmdResult {
        barrier.complete_node_stage(Self::STAGE, Some(metadata))
    }
}

pub struct CommonServicesReadyStage;

impl CommonServicesReadyStage {
    const STAGE: VerifiedNodeStage = const { stages::SERVICES_READY.node_stage() };

    pub fn complete(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.complete_node_stage(Self::STAGE, None)
    }
}

pub struct ServicesReadyStageDef;

impl ServicesReadyStageDef {
    const STAGE: VerifiedNodeStage = const { stages::SERVICES_READY.node_stage() };

    pub fn complete(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.complete_node_stage(Self::STAGE, None)
    }

    pub fn wait_for_global_dep(barrier: &WorkflowBarrier, dep: VerifiedGlobalDep) -> CmdResult {
        barrier.wait_for_global(dep)
    }

    pub fn wait_for_node_dep(
        barrier: &WorkflowBarrier,
        dep: VerifiedNodeDep,
        expected: usize,
    ) -> Result<Vec<crate::workflow::StageCompletion>, Error> {
        barrier.wait_for_nodes(dep, expected)
    }
}

pub struct BenchServicesReadyStage;

impl BenchServicesReadyStage {
    const ETCD_READY: VerifiedGlobalDep = const { stages::SERVICES_READY.global_dep("etcd-ready") };

    pub fn wait_for_etcd_ready(barrier: &WorkflowBarrier) -> CmdResult {
        ServicesReadyStageDef::wait_for_global_dep(barrier, Self::ETCD_READY)
    }

    pub fn complete(barrier: &WorkflowBarrier) -> CmdResult {
        ServicesReadyStageDef::complete(barrier)
    }
}
