import * as cdk from "aws-cdk-lib";
import * as TOML from "@iarna/toml";

export type DataBlobStorage =
  | "all_in_bss_single_az"
  | "s3_hybrid_single_az"
  | "s3_express_multi_az";

export type DeployTarget = "aws" | "on_prem";

// Helper to create a TOML key-value line with a CFN token value
function tomlLine(key: string, value: string): string {
  return cdk.Fn.join("", [key, ' = "', value, '"']);
}

// Instance configuration with optional private IP
export interface InstanceProps {
  id: string;
  privateIp?: string;
}

// Build config with CFN tokens (hybrid approach)
// Static parts use TOML.stringify, dynamic parts use cdk.Fn.join
// Uses new [[nodes]] format instead of [instances."id"]
export function createConfigWithCfnTokens(props: {
  deployTarget?: DeployTarget; // defaults to "aws"
  region: string; // AWS region or custom region identifier
  forBench: boolean;
  dataBlobStorage: DataBlobStorage;
  rssHaEnabled: boolean;
  rssBackend: "etcd" | "ddb";
  journalType: "ebs" | "nvme";
  numBssNodes?: number;
  numApiServers?: number;
  numBenchClients?: number;
  cpuTarget?: string; // e.g., "i3", "graviton3"
  dataBlobBucket?: string;
  localAz: string;
  remoteAz?: string;
  nssEndpoint: string;
  mirrordEndpoint?: string;
  apiServerEndpoint: string;
  nssA: InstanceProps;
  nssB?: InstanceProps;
  volumeAId: string;
  volumeBId?: string;
  rssA: InstanceProps;
  rssB?: InstanceProps;
  guiServer?: InstanceProps;
  benchServer?: InstanceProps;
  benchClientNum?: number;
  workflowClusterId?: string;
  etcdEndpoints?: string; // for static etcd endpoints
  bootstrapBucket?: string; // S3 bucket name for bootstrap files
}): string {
  // Build static config using TOML library
  const deployTarget = props.deployTarget ?? "aws";
  const staticConfig: Record<string, TOML.JsonMap> = {
    global: {
      deploy_target: deployTarget,
      region: props.region,
      for_bench: props.forBench,
      data_blob_storage: props.dataBlobStorage,
      rss_ha_enabled: props.rssHaEnabled,
      rss_backend: props.rssBackend,
      journal_type: props.journalType,
    },
  };

  if (props.numBssNodes !== undefined) {
    (staticConfig.global as TOML.JsonMap).num_bss_nodes = props.numBssNodes;
  }
  if (props.numApiServers !== undefined) {
    (staticConfig.global as TOML.JsonMap).num_api_servers = props.numApiServers;
  }
  if (props.numBenchClients !== undefined) {
    (staticConfig.global as TOML.JsonMap).num_bench_clients =
      props.numBenchClients;
  }
  if (props.cpuTarget) {
    (staticConfig.global as TOML.JsonMap).cpu_target = props.cpuTarget;
  }

  // Add workflow_cluster_id for S3-based workflow barriers
  if (props.workflowClusterId) {
    (staticConfig.global as TOML.JsonMap).workflow_cluster_id =
      props.workflowClusterId;
  }

  if (props.dataBlobBucket || props.remoteAz) {
    staticConfig.aws = { local_az: props.localAz };
    if (props.remoteAz) {
      (staticConfig.aws as TOML.JsonMap).remote_az = props.remoteAz;
    }
  }

  // Add bootstrap_bucket at root level (before any sections)
  const rootConfig: TOML.JsonMap = {};
  if (props.bootstrapBucket) {
    rootConfig.bootstrap_bucket = props.bootstrapBucket;
  }

  const staticPart =
    "# Auto-generated bootstrap configuration\n# Do not edit manually\n\n" +
    (Object.keys(rootConfig).length > 0
      ? TOML.stringify(rootConfig) + "\n"
      : "") +
    TOML.stringify(staticConfig as TOML.JsonMap);

  // Dynamic parts with CFN tokens
  const lines: string[] = [staticPart.trimEnd()];

  if (props.dataBlobBucket) {
    lines.push(tomlLine("data_blob_bucket", props.dataBlobBucket));
  }

  // [endpoints] section with CFN tokens
  lines.push("");
  lines.push("[endpoints]");
  lines.push(tomlLine("nss_endpoint", props.nssEndpoint));
  if (props.mirrordEndpoint) {
    lines.push(tomlLine("mirrord_endpoint", props.mirrordEndpoint));
  }
  lines.push(tomlLine("api_server_endpoint", props.apiServerEndpoint));

  // [resources] section with CFN tokens
  lines.push("");
  lines.push("[resources]");
  lines.push(tomlLine("nss_a_id", props.nssA.id));
  if (props.nssB) {
    lines.push(tomlLine("nss_b_id", props.nssB.id));
  }
  // Only include volume IDs for ebs journal type
  if (props.journalType === "ebs" && props.volumeAId) {
    lines.push(tomlLine("volume_a_id", props.volumeAId));
    if (props.volumeBId) {
      lines.push(tomlLine("volume_b_id", props.volumeBId));
    }
  }

  // [etcd] section with S3-based dynamic cluster discovery (when using etcd backend)
  if (props.rssBackend === "etcd" && props.numBssNodes) {
    lines.push("");
    lines.push("[etcd]");
    lines.push("enabled = true");
    lines.push(`cluster_size = ${props.numBssNodes}`);
    // Add static endpoints for on-prem
    if (props.etcdEndpoints) {
      lines.push(tomlLine("endpoints", props.etcdEndpoints));
    }
  }

  // Helper function to add a node entry with [[nodes.service_type]] format
  const addNode = (
    instance: InstanceProps,
    serviceType: string,
    role?: string,
    volumeId?: string,
    benchClientNum?: number,
  ) => {
    lines.push("");
    lines.push(`[[nodes.${serviceType}]]`);
    lines.push(cdk.Fn.join("", ['id = "', instance.id, '"']));
    if (role) {
      lines.push(`role = "${role}"`);
    }
    if (instance.privateIp) {
      lines.push(cdk.Fn.join("", ['private_ip = "', instance.privateIp, '"']));
    }
    if (volumeId) {
      lines.push(cdk.Fn.join("", ['volume_id = "', volumeId, '"']));
    }
    if (benchClientNum !== undefined) {
      lines.push(`bench_client_num = ${benchClientNum}`);
    }
  };

  // Add RSS nodes
  addNode(props.rssA, "root_server", "leader");
  if (props.rssB) {
    addNode(props.rssB, "root_server", "follower");
  }

  // Add NSS nodes
  const nssAVolumeId =
    props.journalType === "ebs" ? props.volumeAId : undefined;
  addNode(props.nssA, "nss_server", undefined, nssAVolumeId);
  if (props.nssB) {
    const nssBVolumeId =
      props.journalType === "ebs" ? props.volumeBId : undefined;
    addNode(props.nssB, "nss_server", undefined, nssBVolumeId);
  }

  // Add GUI server
  if (props.guiServer) {
    addNode(props.guiServer, "gui_server");
  }

  // Add Bench server
  if (props.benchServer && props.benchClientNum !== undefined) {
    addNode(
      props.benchServer,
      "bench_server",
      undefined,
      undefined,
      props.benchClientNum,
    );
  }

  lines.push("");

  return cdk.Fn.join("\n", lines);
}
