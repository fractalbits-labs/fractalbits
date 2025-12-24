import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as TOML from "@iarna/toml";
import { createInstance, createUserData, createEc2Asg } from "./ec2-utils";

interface FractalbitsBenchVpcStackProps extends cdk.StackProps {
  serviceEndpoint: string;
  benchClientCount: number;
  benchClientInstanceType: string;
  benchType?: "service_endpoint" | "internal" | "external" | null;
}

export class FractalbitsBenchVpcStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;

  constructor(
    scope: Construct,
    id: string,
    props: FractalbitsBenchVpcStackProps,
  ) {
    super(scope, id, props);

    // === VPC Configuration ===
    this.vpc = new ec2.Vpc(this, "FractalbitsBenchVpc", {
      vpcName: "fractalbits-bench-vpc",
      ipAddresses: ec2.IpAddresses.cidr("10.1.0.0/16"),
      maxAzs: 1,
      natGateways: 0,
      subnetConfiguration: [
        {
          name: "PrivateSubnet",
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        },
      ],
    });

    // Add Gateway Endpoint for S3
    this.vpc.addGatewayEndpoint("S3Endpoint", {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // Add Interface Endpoint for EC2 and SSM
    ["SSM", "SSM_MESSAGES", "EC2", "EC2_MESSAGES"].forEach((service) => {
      this.vpc.addInterfaceEndpoint(`${service}Endpoint`, {
        service: (ec2.InterfaceVpcEndpointAwsService as any)[service],
        subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      });
    });

    // IAM Role for EC2
    const ec2Role = new iam.Role(this, "BenchInstanceRole", {
      roleName: "FractalbitsBenchInstanceRole",
      assumedBy: new iam.ServicePrincipal("ec2.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMFullAccess"),
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess"),
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "AmazonDynamoDBFullAccess_v2",
        ),
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonEC2FullAccess"),
        iam.ManagedPolicy.fromAwsManagedPolicyName("AWSCloudMapFullAccess"),
      ],
    });

    const privateSg = new ec2.SecurityGroup(this, "BenchPrivateInstanceSG", {
      vpc: this.vpc,
      securityGroupName: "FractalbitsBenchPrivateInstanceSG",
      description: "Allow outbound for SSM",
      allowAllOutbound: true,
    });

    // Allow incoming traffic on port 7761 for bench clients
    privateSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(7761),
      "Allow incoming on port 7761 from VPC",
    );

    // Bench Server Instance
    const benchServerInstance = createInstance(
      this,
      this.vpc,
      "BenchServerInstance",
      this.vpc.isolatedSubnets[0],
      ec2.InstanceType.of(ec2.InstanceClass.C7G, ec2.InstanceSize.MEDIUM),
      privateSg,
      ec2Role,
    );

    // Build bootstrap config for bench stack
    const region = cdk.Stack.of(this).region;
    const account = cdk.Stack.of(this).account;
    const buildsBucket = s3.Bucket.fromBucketName(
      this,
      "BuildsBucket",
      `fractalbits-bootstrap-${region}-${account}`,
    );

    // Static config using TOML library
    const staticConfig = {
      global: {
        for_bench: true,
        data_blob_storage: "all_in_bss_single_az",
        rss_ha_enabled: false,
      },
      aws: {
        bucket: "unused",
        local_az: "unused",
        iam_role: ec2Role.roleName,
      },
      endpoints: {
        nss_endpoint: "unused",
        api_server_endpoint: props.serviceEndpoint,
      },
      resources: {
        nss_a_id: "unused",
      },
    };

    const staticPart =
      "# Auto-generated bootstrap configuration for bench stack\n\n" +
      TOML.stringify(staticConfig as TOML.JsonMap);

    // Dynamic instance section with CFN token
    const configContent = cdk.Fn.join("\n", [
      staticPart.trimEnd(),
      "",
      cdk.Fn.join("", ['[instances."', benchServerInstance.instanceId, '"]']),
      'service_type = "bench_server"',
      `bench_client_num = ${props.benchClientCount}`,
      "",
    ]);

    new s3deploy.BucketDeployment(this, "ConfigDeployment", {
      sources: [s3deploy.Source.data("bootstrap.toml", configContent)],
      destinationBucket: buildsBucket,
    });

    benchServerInstance.addUserData(createUserData(this).render());

    // Bench Client ASG - instances discover their role from EC2 tags
    const benchClientAsg = createEc2Asg(
      this,
      "BenchClientAsg",
      this.vpc,
      this.vpc.isolatedSubnets[0],
      privateSg,
      ec2Role,
      [props.benchClientInstanceType],
      props.benchClientCount,
      props.benchClientCount,
      "bench_client",
    );

    // Outputs
    new cdk.CfnOutput(this, "BenchServerInstanceId", {
      value: benchServerInstance.instanceId,
      description: "EC2 instance ID for the bench server",
    });

    new cdk.CfnOutput(this, "BenchClientAsgName", {
      value: benchClientAsg.autoScalingGroupName,
      description: "Auto Scaling Group Name for bench clients",
    });
  }
}
