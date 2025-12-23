import * as cdk from "aws-cdk-lib";
import * as TOML from "@iarna/toml";

export type DataBlobStorage =
  | "all_in_bss_single_az"
  | "s3_hybrid_single_az"
  | "s3_express_multi_az";

export type VpcTarget = "aws" | "on_prem";

// Helper to create a TOML key-value line with a CFN token value
function tomlLine(key: string, value: string): string {
  return cdk.Fn.join("", [key, ' = "', value, '"']);
}

// Helper to create a TOML instance section header with CFN token
function instanceHeader(instanceId: string): string {
  return cdk.Fn.join("", ['[instances."', instanceId, '"]']);
}

// Instance configuration with optional private IP
export interface InstanceProps {
  id: string;
  privateIp?: string;
}

// Build config with CFN tokens (hybrid approach)
// Static parts use TOML.stringify, dynamic parts use cdk.Fn.join
export function createConfigWithCfnTokens(props: {
  target?: VpcTarget; // defaults to "aws"
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
  const target = props.target ?? "aws";
  const staticConfig: Record<string, TOML.JsonMap> = {
    global: {
      target: target,
      for_bench: props.forBench,
      data_blob_storage: props.dataBlobStorage,
      rss_ha_enabled: props.rssHaEnabled,
      rss_backend: props.rssBackend,
      journal_type: props.journalType,
    },
  };

  if (props.bootstrapBucket) {
    (staticConfig.global as TOML.JsonMap).bootstrap_bucket =
      props.bootstrapBucket;
  }

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

  const staticPart =
    "# Auto-generated bootstrap configuration\n# Do not edit manually\n\n" +
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

  // Helper to add private_ip if present
  const addPrivateIp = (instance: InstanceProps) => {
    if (instance.privateIp) {
      lines.push(tomlLine("private_ip", instance.privateIp));
    }
  };

  // Instance sections with CFN tokens
  lines.push("");
  lines.push(instanceHeader(props.rssA.id));
  lines.push('service_type = "root_server"');
  lines.push('role = "leader"');
  addPrivateIp(props.rssA);

  if (props.rssB) {
    lines.push("");
    lines.push(instanceHeader(props.rssB.id));
    lines.push('service_type = "root_server"');
    lines.push('role = "follower"');
    addPrivateIp(props.rssB);
  }

  lines.push("");
  lines.push(instanceHeader(props.nssA.id));
  lines.push('service_type = "nss_server"');
  // Only include volume_id for ebs journal type
  if (props.journalType === "ebs" && props.volumeAId) {
    lines.push(tomlLine("volume_id", props.volumeAId));
  }
  addPrivateIp(props.nssA);

  if (props.nssB) {
    lines.push("");
    lines.push(instanceHeader(props.nssB.id));
    lines.push('service_type = "nss_server"');
    // Only include volume_id for ebs journal type
    if (props.journalType === "ebs" && props.volumeBId) {
      lines.push(tomlLine("volume_id", props.volumeBId));
    }
    addPrivateIp(props.nssB);
  }

  if (props.guiServer) {
    lines.push("");
    lines.push(instanceHeader(props.guiServer.id));
    lines.push('service_type = "gui_server"');
    addPrivateIp(props.guiServer);
  }

  if (props.benchServer && props.benchClientNum !== undefined) {
    lines.push("");
    lines.push(instanceHeader(props.benchServer.id));
    lines.push('service_type = "bench_server"');
    lines.push(`bench_client_num = ${props.benchClientNum}`);
    addPrivateIp(props.benchServer);
  }

  lines.push("");

  return cdk.Fn.join("\n", lines);
}
