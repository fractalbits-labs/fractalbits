#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { FractalbitsVpcStack } from "../lib/fractalbits-vpc-stack";
import { FractalbitsBenchVpcStack } from "../lib/fractalbits-bench-vpc-stack";
import { PeeringStack } from "../lib/fractalbits-peering-stack";
import { FractalbitsMetaStack } from "../lib/fractalbits-meta-stack";
import { getAzNameFromIdAtBuildTime } from "../lib/ec2-utils";

const app = new cdk.App();

// Meta-stack mode: deploy a standalone BSS or NSS node for testing
// Usage: npx cdk deploy --context metaStack=bss --context bssInstanceTypes=i8g.2xlarge
const metaStack = app.node.tryGetContext("metaStack") ?? null;
if (metaStack) {
  const env = {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  };

  let metaAz = app.node.tryGetContext("az");
  if (!metaAz) {
    metaAz = env.region === "us-east-1" ? "use1-az4" : "usw2-az3";
  }
  const resolvedAz = getAzNameFromIdAtBuildTime(metaAz, env.region);

  new FractalbitsMetaStack(app, "FractalbitsMetaStack", {
    env: env,
    serviceName: metaStack,
    availabilityZone: resolvedAz,
    nssInstanceType: app.node.tryGetContext("nssInstanceType") ?? undefined,
    bssInstanceTypes: app.node.tryGetContext("bssInstanceTypes") ?? undefined,
  });
} else {
  // Get template type to determine default configurations
  const vpcTemplate = app.node.tryGetContext("vpcTemplate") ?? null;

  // Get context values (may be overridden by template)
  const benchType = app.node.tryGetContext("benchType") ?? null;
  const bssInstanceTypes =
    app.node.tryGetContext("bssInstanceTypes") ?? "i8g.2xlarge";
  const apiServerInstanceType =
    app.node.tryGetContext("apiServerInstanceType") ?? "c8g.xlarge";
  const benchClientInstanceType =
    app.node.tryGetContext("benchClientInstanceType") ?? "c8g.xlarge";
  const dataBlobStorage =
    app.node.tryGetContext("dataBlobStorage") ?? "all_in_bss_single_az";
  const rssBackend = app.node.tryGetContext("rssBackend") ?? "ddb";
  const journalType = app.node.tryGetContext("journalType") ?? "ebs";
  const browserIp = app.node.tryGetContext("browserIp") ?? null;
  // Note: Context values from CLI are always strings, so convert to numbers
  let numApiServers = Number(app.node.tryGetContext("numApiServers")) || 1;
  let numBenchClients = Number(app.node.tryGetContext("numBenchClients")) || 1;
  let numBssNodes = Number(app.node.tryGetContext("numBssNodes")) || 1;
  let ebsVolumeIops = Number(app.node.tryGetContext("ebsVolumeIops")) || 10000;
  let ebsVolumeSize = Number(app.node.tryGetContext("ebsVolumeSize")) || 20;
  let rootServerHa = app.node.tryGetContext("rootServerHa") || false;

  // Configure based on template type
  let nssInstanceType = "m7gd.4xlarge";
  if (vpcTemplate === "mini") {
    nssInstanceType = "m7gd.2xlarge";
    ebsVolumeSize = 5;
    ebsVolumeIops = 1000;
    rootServerHa = false;
    numApiServers = 1;
    numBssNodes = 1;
    numBenchClients = 1;
  } else if (vpcTemplate === "perf_demo") {
    nssInstanceType = "m7gd.4xlarge";
    ebsVolumeSize = 20;
    ebsVolumeIops = 10000;
    rootServerHa = true;
    numApiServers = 14;
    numBssNodes = 6;
    numBenchClients = 42;
  }

  // Get the current region - CDK will auto-detect from AWS config/credentials
  const env = {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  };

  // Determine default AZ based on deployment mode and region
  // For single-AZ modes: single AZ ID (e.g., "usw2-az3")
  // For multi-AZ: AZ pair (e.g., "usw2-az3,usw2-az4")
  const isMultiAz = dataBlobStorage === "s3_express_multi_az";
  let az = app.node.tryGetContext("az");
  if (!az) {
    if (!isMultiAz) {
      // Default single AZ based on region
      az = env.region === "us-east-1" ? "use1-az4" : "usw2-az3";
    } else {
      // Default AZ pair for multi-AZ based on region
      az =
        env.region === "us-east-1" ? "use1-az4,use1-az6" : "usw2-az3,usw2-az4";
    }
  }

  const vpcStack = new FractalbitsVpcStack(app, "FractalbitsVpcStack", {
    env: env,
    browserIp: browserIp,
    numApiServers: numApiServers,
    numBenchClients: numBenchClients,
    numBssNodes: numBssNodes,
    benchType: benchType,
    az: az,
    bssInstanceTypes: bssInstanceTypes,
    apiServerInstanceType: apiServerInstanceType,
    benchClientInstanceType: benchClientInstanceType,
    nssInstanceType: nssInstanceType,
    dataBlobStorage: dataBlobStorage,
    rssBackend: rssBackend,
    journalType: journalType,
    rootServerHa: rootServerHa,
    ebsVolumeSize: ebsVolumeSize,
    ebsVolumeIops: ebsVolumeIops,
  });

  if (benchType === "service_endpoint") {
    const benchClientCount = app.node.tryGetContext("benchClientCount") ?? 1;

    const benchVpcStack = new FractalbitsBenchVpcStack(
      app,
      "FractalbitsBenchVpcStack",
      {
        env: env,
        serviceEndpoint: vpcStack.nlbLoadBalancerDnsName,
        benchClientCount: benchClientCount,
        benchClientInstanceType: benchClientInstanceType,
        benchType: benchType,
      },
    );

    new PeeringStack(app, "PeeringStack", {
      vpcA: vpcStack.vpc,
      vpcB: benchVpcStack.vpc,
      env: env,
    });
  }
} // end of else (non-meta-stack path)
