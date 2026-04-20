#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { FractalbitsVpcStack } from "../lib/fractalbits-vpc-stack";
import { FractalbitsBenchVpcStack } from "../lib/fractalbits-bench-vpc-stack";
import { PeeringStack } from "../lib/fractalbits-peering-stack";
import { FractalbitsMetaStack } from "../lib/fractalbits-meta-stack";
import { getAzNameFromIdAtBuildTime, DeployOS } from "../lib/ec2-utils";

const app = new cdk.App();

// Get the current region - CDK will auto-detect from AWS config/credentials
const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

// Meta-stack: deploy a standalone BSS node for testing.
// Only serviceName="bss" is supported; standalone NSS bootstrap now requires
// RSS and BSS and is no longer a meaningful meta-stack target.
// Usage: npx cdk deploy --context metaStack=bss --context bssInstanceTypes=i8g.2xlarge FractalbitsMetaStack
const metaStackContext = app.node.tryGetContext("metaStack") ?? null;
if (metaStackContext) {
  let metaAz = app.node.tryGetContext("az");
  if (!metaAz) {
    metaAz = env.region === "us-east-1" ? "use1-az4" : "usw2-az3";
  }
  const resolvedAz = getAzNameFromIdAtBuildTime(metaAz, env.region);

  new FractalbitsMetaStack(app, "FractalbitsMetaStack", {
    env: env,
    serviceName: metaStackContext,
    availabilityZone: resolvedAz,
    bssInstanceTypes: app.node.tryGetContext("bssInstanceTypes") ?? undefined,
  });
} else {
  // Empty placeholder so `cdk destroy --all` can discover the meta stack
  new cdk.Stack(app, "FractalbitsMetaStack", { env });
}

// All template defaults are resolved in Rust (xtask vpc.rs) before CDK is invoked.
// Context values here are the final resolved values passed explicitly from Rust.
const benchType = app.node.tryGetContext("benchType") ?? null;
const bssInstanceTypes =
  app.node.tryGetContext("bssInstanceTypes") ?? "i8g.2xlarge";
const nssInstanceType =
  app.node.tryGetContext("nssInstanceType") ?? "r7g.4xlarge";
const apiServerInstanceType =
  app.node.tryGetContext("apiServerInstanceType") ?? "c8g.xlarge";
const benchClientInstanceType =
  app.node.tryGetContext("benchClientInstanceType") ?? "c8g.xlarge";
const dataBlobStorage =
  app.node.tryGetContext("dataBlobStorage") ?? "all_in_bss_single_az";
const rssBackend = app.node.tryGetContext("rssBackend") ?? "ddb";
const journalType = app.node.tryGetContext("journalType") ?? "remote";
const browserIp = app.node.tryGetContext("browserIp") ?? null;
// Note: Context values from CLI are always strings, so convert to numbers
const numApiServers = Number(app.node.tryGetContext("numApiServers")) || 1;
const numBenchClients = Number(app.node.tryGetContext("numBenchClients")) || 1;
const numBssNodes = Number(app.node.tryGetContext("numBssNodes")) || 1;
const rootServerHa = app.node.tryGetContext("rootServerHa") || false;
const deployOS = (app.node.tryGetContext("deployOS") ?? "al2023") as DeployOS;

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
    az = env.region === "us-east-1" ? "use1-az4,use1-az6" : "usw2-az3,usw2-az4";
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
  deployOS: deployOS,
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
