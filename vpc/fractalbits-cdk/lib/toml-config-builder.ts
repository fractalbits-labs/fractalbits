import * as cdk from "aws-cdk-lib";
import * as TOML from "@iarna/toml";

export type DataBlobStorage =
  | "all_in_bss_single_az"
  | "s3_hybrid_single_az"
  | "s3_express_multi_az";

// Helper to create a TOML key-value line with a CFN token value
function tomlLine(key: string, value: string): string {
  return cdk.Fn.join("", [key, ' = "', value, '"']);
}

// Helper to create a TOML instance section header with CFN token
function instanceHeader(instanceId: string): string {
  return cdk.Fn.join("", ['[instances."', instanceId, '"]']);
}

// Build config with CFN tokens (hybrid approach)
// Static parts use TOML.stringify, dynamic parts use cdk.Fn.join
export function createConfigWithCfnTokens(props: {
  forBench: boolean;
  dataBlobStorage: DataBlobStorage;
  rssHaEnabled: boolean;
  rssBackend: "etcd" | "ddb";
  journalType: "ebs" | "nvme";
  numBssNodes?: number;
  numApiServers?: number;
  numBenchClients?: number;
  dataBlobBucket?: string;
  localAz: string;
  remoteAz?: string;
  nssEndpoint: string;
  mirrordEndpoint?: string;
  apiServerEndpoint: string;
  nssAId: string;
  nssBId?: string;
  volumeAId: string;
  volumeBId?: string;
  rssAId: string;
  rssBId?: string;
  guiServerId?: string;
  benchServerId?: string;
  benchClientNum?: number;
  workflowClusterId?: string;
}): string {
  // Build static config using TOML library
  const staticConfig: Record<string, TOML.JsonMap> = {
    global: {
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
  lines.push(tomlLine("nss_a_id", props.nssAId));
  if (props.nssBId) {
    lines.push(tomlLine("nss_b_id", props.nssBId));
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
  }

  // Instance sections with CFN tokens
  lines.push("");
  lines.push(instanceHeader(props.rssAId));
  lines.push('service_type = "root_server"');
  lines.push('role = "leader"');

  if (props.rssBId) {
    lines.push("");
    lines.push(instanceHeader(props.rssBId));
    lines.push('service_type = "root_server"');
    lines.push('role = "follower"');
  }

  lines.push("");
  lines.push(instanceHeader(props.nssAId));
  lines.push('service_type = "nss_server"');
  // Only include volume_id for ebs journal type
  if (props.journalType === "ebs" && props.volumeAId) {
    lines.push(tomlLine("volume_id", props.volumeAId));
  }

  if (props.nssBId) {
    lines.push("");
    lines.push(instanceHeader(props.nssBId));
    lines.push('service_type = "nss_server"');
    // Only include volume_id for ebs journal type
    if (props.journalType === "ebs" && props.volumeBId) {
      lines.push(tomlLine("volume_id", props.volumeBId));
    }
  }

  if (props.guiServerId) {
    lines.push("");
    lines.push(instanceHeader(props.guiServerId));
    lines.push('service_type = "gui_server"');
  }

  if (props.benchServerId && props.benchClientNum !== undefined) {
    lines.push("");
    lines.push(instanceHeader(props.benchServerId));
    lines.push('service_type = "bench_server"');
    lines.push(`bench_client_num = ${props.benchClientNum}`);
  }

  lines.push("");

  return cdk.Fn.join("\n", lines);
}
