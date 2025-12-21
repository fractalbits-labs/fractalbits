import * as cdk from "aws-cdk-lib";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as TOML from "@iarna/toml";

export type DataBlobStorage =
  | "all_in_bss_single_az"
  | "s3_hybrid_single_az"
  | "s3_express_multi_az";

export interface BootstrapConfigProps {
  forBench: boolean;
  dataBlobStorage: DataBlobStorage;
  rssHaEnabled: boolean;
  numBssNodes?: number;
  metaStackTesting?: boolean;
  bucket: string;
  localAz: string;
  remoteAz?: string;
  iamRole: string;
  nssEndpoint: string;
  mirrordEndpoint?: string;
  apiServerEndpoint: string;
  instances: Record<string, ec2.Instance>;
  nssAId: string;
  nssBId?: string;
  volumeAId: string;
  volumeBId?: string;
  benchClientNum?: number;
}

export interface InstanceConfigEntry {
  instanceId: string;
  serviceType: string;
  role?: string;
  leaderId?: string;
  volumeId?: string;
  benchClientNum?: number;
}

// Static config structure (values known at synthesis time)
interface StaticConfig {
  global: {
    for_bench: boolean;
    data_blob_storage: string;
    rss_ha_enabled: boolean;
    num_bss_nodes?: number;
    meta_stack_testing?: boolean;
  };
  aws: {
    bucket: string;
    local_az: string;
    remote_az?: string;
    iam_role: string;
  };
  endpoints: {
    nss_endpoint: string;
    mirrord_endpoint?: string;
    api_server_endpoint: string;
  };
  resources: {
    nss_a_id: string;
    nss_b_id?: string;
    volume_a_id: string;
    volume_b_id?: string;
  };
}

// Build static config using TOML library (for configs without CFN tokens)
export function buildBootstrapConfig(props: BootstrapConfigProps): string {
  const config: StaticConfig = {
    global: {
      for_bench: props.forBench,
      data_blob_storage: props.dataBlobStorage,
      rss_ha_enabled: props.rssHaEnabled,
    },
    aws: {
      bucket: props.bucket,
      local_az: props.localAz,
      iam_role: props.iamRole,
    },
    endpoints: {
      nss_endpoint: props.nssEndpoint,
      api_server_endpoint: props.apiServerEndpoint,
    },
    resources: {
      nss_a_id: props.nssAId,
      volume_a_id: props.volumeAId,
    },
  };

  if (props.numBssNodes !== undefined) {
    config.global.num_bss_nodes = props.numBssNodes;
  }
  if (props.metaStackTesting) {
    config.global.meta_stack_testing = true;
  }
  if (props.remoteAz) {
    config.aws.remote_az = props.remoteAz;
  }
  if (props.mirrordEndpoint) {
    config.endpoints.mirrord_endpoint = props.mirrordEndpoint;
  }
  if (props.nssBId) {
    config.resources.nss_b_id = props.nssBId;
  }
  if (props.volumeBId) {
    config.resources.volume_b_id = props.volumeBId;
  }

  return (
    "# Auto-generated bootstrap configuration\n# Do not edit manually\n\n" +
    TOML.stringify(config as unknown as TOML.JsonMap)
  );
}

// Build instance configs (for static instance IDs known at synthesis time)
export function buildInstanceConfigs(entries: InstanceConfigEntry[]): string {
  const instances: Record<string, TOML.JsonMap> = {};

  for (const entry of entries) {
    const instanceConfig: TOML.JsonMap = {
      service_type: entry.serviceType,
    };
    if (entry.role) {
      instanceConfig.role = entry.role;
    }
    if (entry.volumeId) {
      instanceConfig.volume_id = entry.volumeId;
    }
    if (entry.benchClientNum !== undefined) {
      instanceConfig.bench_client_num = entry.benchClientNum;
    }
    instances[entry.instanceId] = instanceConfig;
  }

  return TOML.stringify({ instances } as TOML.JsonMap);
}

export function buildFullConfig(
  baseConfig: string,
  instanceConfigs: string,
): string {
  return `${baseConfig}\n${instanceConfigs}`;
}

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
export function createConfigWithCfnTokens(
  scope: cdk.Stack,
  props: {
    forBench: boolean;
    dataBlobStorage: DataBlobStorage;
    rssHaEnabled: boolean;
    rssBackend: "etcd" | "ddb";
    journalType: "ebs" | "nvme";
    numBssNodes?: number;
    dataBlobBucket?: string;
    localAz: string;
    remoteAz?: string;
    iamRole: string;
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
  },
): string {
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
