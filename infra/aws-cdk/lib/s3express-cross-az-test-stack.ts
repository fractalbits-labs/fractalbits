import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import {
  createVpcEndpoints,
  createEc2Role,
  createInstance,
  getAzNameFromIdAtBuildTime,
} from "./ec2-utils";

interface S3ExpressCrossAzTestStackProps extends cdk.StackProps {
  targetAz?: string;
}

export class S3ExpressCrossAzTestStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;
  public readonly instance: ec2.Instance;

  constructor(
    scope: Construct,
    id: string,
    props: S3ExpressCrossAzTestStackProps,
  ) {
    super(scope, id, props);

    const targetAzId = props.targetAz ?? "use2-az1";
    const targetAzName = getAzNameFromIdAtBuildTime(targetAzId, "us-east-2");

    this.vpc = new ec2.Vpc(this, "S3ExpressTestVpc", {
      vpcName: "s3express-cross-az-test-vpc",
      ipAddresses: ec2.IpAddresses.cidr("10.2.0.0/16"),
      availabilityZones: [targetAzName],
      natGateways: 0,
      subnetConfiguration: [
        {
          name: "IsolatedSubnet",
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        },
      ],
      enableDnsHostnames: true,
      enableDnsSupport: true,
    });

    createVpcEndpoints(this.vpc, ec2.SubnetType.PRIVATE_ISOLATED);

    const ec2Role = createEc2Role(this);

    const sg = new ec2.SecurityGroup(this, "InstanceSG", {
      vpc: this.vpc,
      securityGroupName: "S3ExpressTestInstanceSG",
      description: "Security group for S3 Express test instance",
      allowAllOutbound: true,
    });

    const instanceType = new ec2.InstanceType("m7g.4xlarge");

    this.instance = createInstance(
      this,
      this.vpc,
      "S3ExpressTestInstance",
      this.vpc.isolatedSubnets[0],
      instanceType,
      sg,
      ec2Role,
    );

    new cdk.CfnOutput(this, "InstanceId", {
      value: this.instance.instanceId,
      description: "S3 Express test instance ID",
    });

    new cdk.CfnOutput(this, "AvailabilityZone", {
      value: this.instance.instanceAvailabilityZone,
      description: "Instance availability zone",
    });

    new cdk.CfnOutput(this, "VpcId", {
      value: this.vpc.vpcId,
      description: "VPC ID",
    });

    new cdk.CfnOutput(this, "SubnetId", {
      value: this.vpc.isolatedSubnets[0].subnetId,
      description: "Isolated subnet ID",
    });

    new cdk.CfnOutput(this, "Region", {
      value: this.region,
      description: "Stack region",
    });

    new cdk.CfnOutput(this, "SSMConnectCommand", {
      value: `aws ssm start-session --target ${this.instance.instanceId} --region ${this.region}`,
      description: "Command to connect to instance via SSM",
    });
  }
}
