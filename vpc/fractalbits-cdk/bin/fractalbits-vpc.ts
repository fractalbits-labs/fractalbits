#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { FractalbitsVpcStack } from "../lib/fractalbits-vpc-stack";
import { FractalbitsBenchVpcStack } from "../lib/fractalbits-bench-vpc-stack";
import { PeeringStack } from "../lib/fractalbits-peering-stack";

const app = new cdk.App();

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
const dataBlobStorage = app.node.tryGetContext("dataBlobStorage") ?? "singleAz";
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
// For singleAz: single AZ ID (e.g., "usw2-az3")
// For multiAz: AZ pair (e.g., "usw2-az3,usw2-az4")
let az = app.node.tryGetContext("az");
if (!az) {
  if (dataBlobStorage === "singleAz") {
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
