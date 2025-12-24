import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as elbv2 from "aws-cdk-lib/aws-elasticloadbalancingv2";
import * as elbv2_targets from "aws-cdk-lib/aws-elasticloadbalancingv2-targets";
import {
  createVpcEndpoints,
  createEc2Role,
  getAzNameFromIdAtBuildTime,
} from "./ec2-utils";
import { Instance } from "aws-cdk-lib/aws-ec2";
import { AccountRootPrincipal } from "aws-cdk-lib/aws-iam";

interface VpcWithPrivateLinkStackProps extends cdk.StackProps {}

export class VpcWithPrivateLinkStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;

  constructor(
    scope: Construct,
    id: string,
    props: VpcWithPrivateLinkStackProps,
  ) {
    super(scope, id, props);

    // Resolve AZ IDs to actual AZ names for us-east-2
    const az1Name = getAzNameFromIdAtBuildTime("use2-az1", "us-east-2");
    const az2Name = getAzNameFromIdAtBuildTime("use2-az2", "us-east-2");

    this.vpc = new ec2.Vpc(this, "VpcWithPrivateLink", {
      vpcName: "vpc-with-private-link",
      ipAddresses: ec2.IpAddresses.cidr("10.0.0.0/16"),
      availabilityZones: [az1Name, az2Name],
      natGateways: 0,
      subnetConfiguration: [
        {
          name: "PrivateSubnet",
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        },
      ],
      enableDnsHostnames: true,
      enableDnsSupport: true,
    });

    const ec2Role = createEc2Role(this);
    createVpcEndpoints(this.vpc);

    // Security Group for instances
    const sg = new ec2.SecurityGroup(this, "InstanceSG", {
      vpc: this.vpc,
      securityGroupName: "VpcWithPrivateLinkInstanceSG",
      description: "Allow all outbound",
      allowAllOutbound: true,
    });
    const servicePort = 5201; // iperf3 service port
    sg.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(servicePort),
      `Allow port ${servicePort} access from anywhere`,
    );

    const machineImage = ec2.MachineImage.latestAmazonLinux2023({
      cpuType: ec2.AmazonLinuxCpuType.ARM_64,
    });

    const az1 = az1Name;
    const az2 = az2Name;

    const instanceType = new ec2.InstanceType("m7g.4xlarge");
    const provider = new Instance(this, "Provider", {
      vpc: this.vpc,
      vpcSubnets: {
        subnets: [this.vpc.isolatedSubnets[0]],
        availabilityZones: [az1],
      },
      role: ec2Role,
      securityGroup: sg,
      instanceType,
      machineImage,
    });
    provider.addUserData(
      "yum install -y iperf3",
      `echo "To start iperf3, run iperf3 -s"`,
    );

    const nlb = new elbv2.NetworkLoadBalancer(this, "VpcWithPrivateLinkNlb", {
      vpc: this.vpc,
      internetFacing: false,
      crossZoneEnabled: true,
      vpcSubnets: {
        subnets: this.vpc.isolatedSubnets,
      },
    });
    const listener = nlb.addListener("listener", { port: servicePort });
    listener.addTargets("target", {
      port: servicePort,
      targets: [new elbv2_targets.InstanceTarget(provider)],
    });

    const endpointService = new ec2.VpcEndpointService(
      this,
      "EndpointService",
      {
        vpcEndpointServiceLoadBalancers: [nlb],
        allowedPrincipals: [new AccountRootPrincipal()],
        acceptanceRequired: false,
      },
    );

    const endpoint = new ec2.InterfaceVpcEndpoint(this, "Endpoint", {
      vpc: this.vpc,
      service: {
        name: endpointService.vpcEndpointServiceName,
        port: servicePort,
      },
      privateDnsEnabled: false,
      subnets: {
        subnets: this.vpc.isolatedSubnets,
      },
    });

    const consumer = new Instance(this, "Consumer", {
      vpc: this.vpc,
      vpcSubnets: {
        subnets: [this.vpc.isolatedSubnets[1]],
        availabilityZones: [az2],
      },
      role: ec2Role,
      securityGroup: sg,
      instanceType,
      machineImage,
    });
    const endpointDnsEntry = cdk.Fn.select(0, endpoint.vpcEndpointDnsEntries);
    const endpointDns = cdk.Fn.select(1, cdk.Fn.split(":", endpointDnsEntry));
    consumer.addUserData(
      "yum install -y iperf3",
      `echo "To send traffic, run iperf3 -c ${endpointDns}"`,
    );

    new cdk.CfnOutput(this, "ProviderInstanceId", {
      value: provider.instanceId,
      description: "Provider instance ID",
    });
    new cdk.CfnOutput(this, "ConsumerInstanceId", {
      value: consumer.instanceId,
      description: "Consumer instance ID",
    });
    new cdk.CfnOutput(this, "NlbName", {
      value: nlb.loadBalancerName,
      description: "NLB name",
    });
    new cdk.CfnOutput(this, "EndpointDns", {
      value: endpointDns,
      description: "Endpoint DNS",
    });
    new cdk.CfnOutput(this, "ProviderAZ", {
      value: az1,
      description: "Provider availability zone",
    });
    new cdk.CfnOutput(this, "ConsumerAZ", {
      value: az2,
      description: "Consumer availability zone",
    });
  }
}
