import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as TOML from "@iarna/toml";
import {
  createEbsVolume,
  createEc2Asg,
  createInstance,
  createUserData,
  createDynamoDbTable,
  createEc2Role,
  createVpcEndpoints,
} from "./ec2-utils";

interface FractalbitsMetaStackProps extends cdk.StackProps {
  serviceName: string;
  availabilityZone?: string;
  nssInstanceType?: string;
  bssInstanceTypes?: string;
}

export class FractalbitsMetaStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: FractalbitsMetaStackProps) {
    super(scope, id, props);

    const az =
      props.availabilityZone ??
      this.availabilityZones[this.availabilityZones.length - 1];
    this.vpc = new ec2.Vpc(this, "FractalbitsMetaStackVpc", {
      vpcName: "fractalbits-meta-stack-vpc",
      availabilityZones: [az],
      natGateways: 0,
      subnetConfiguration: [
        {
          name: "PrivateSubnet",
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        },
      ],
    });

    createVpcEndpoints(this.vpc);

    createDynamoDbTable(
      this,
      "ServiceDiscoveryTable",
      "fractalbits-service-discovery",
      "service_id",
    );

    const ec2Role = createEc2Role(this);

    // Security Group
    const sg = new ec2.SecurityGroup(this, "InstanceSG", {
      vpc: this.vpc,
      securityGroupName: "FractalbitsInstanceSG",
      description: "Allow all outbound",
      allowAllOutbound: true,
    });

    let targetIdOutput: cdk.CfnOutput;

    // Build bootstrap config for meta stack testing
    const buildMetaStackConfig = (instanceEntries: string[]): string => {
      // Static config using TOML library
      const staticConfig = {
        global: {
          for_bench: false,
          data_blob_storage: "all_in_bss_single_az",
          rss_ha_enabled: false,
          meta_stack_testing: true,
        },
        aws: {
          bucket: "unused",
          local_az: az,
          iam_role: ec2Role.roleName,
        },
        endpoints: {
          nss_endpoint: "unused",
          api_server_endpoint: "unused",
        },
        resources: {
          nss_a_id: "unused",
        },
      };

      const staticPart =
        "# Auto-generated bootstrap configuration for meta stack testing\n\n" +
        TOML.stringify(staticConfig as TOML.JsonMap);

      // Combine static part with dynamic instance entries
      return cdk.Fn.join("\n", [staticPart.trimEnd(), "", ...instanceEntries]);
    };

    // Upload config to builds bucket
    const region = cdk.Stack.of(this).region;
    const account = cdk.Stack.of(this).account;
    const buildsBucket = s3.Bucket.fromBucketName(
      this,
      "BuildsBucket",
      `fractalbits-bootstrap-${region}-${account}`,
    );

    if (props.serviceName == "nss") {
      const nssInstanceType = new ec2.InstanceType(
        props.nssInstanceType ?? "m7gd.2xlarge",
      );
      const instance = createInstance(
        this,
        this.vpc,
        `${props.serviceName}_bench`,
        this.vpc.isolatedSubnets[0],
        nssInstanceType,
        sg,
        ec2Role,
      );
      const ebsVolume = createEbsVolume(
        this,
        "MultiAttachVolume",
        az,
        instance.instanceId,
        20,
        10000,
      );

      // Build instance config entries using CloudFormation tokens
      const instanceEntries = [
        cdk.Fn.join("", ['[instances."', instance.instanceId, '"]']),
        'service_type = "nss_server"',
        cdk.Fn.join("", ['volume_id = "', ebsVolume.volumeId, '"']),
        "",
      ];

      const configContent = buildMetaStackConfig(instanceEntries);
      new s3deploy.BucketDeployment(this, "ConfigDeployment", {
        sources: [s3deploy.Source.data("bootstrap.toml", configContent)],
        destinationBucket: buildsBucket,
      });

      instance.addUserData(createUserData(this).render());

      targetIdOutput = new cdk.CfnOutput(this, "instanceId", {
        value: instance.instanceId,
        description: `EC2 instance ID`,
      });
    } else {
      if (!props.bssInstanceTypes) {
        console.error(
          "Error: bssInstanceTypes must be provided for the 'bss' service type in the meta stack. Please specify it via --context bssInstanceTypes='type1,type2'",
        );
        process.exit(1);
      }
      const bssInstanceTypes = props.bssInstanceTypes.split(",");

      // For BSS ASG, instances discover their role from EC2 tags
      const configContent = buildMetaStackConfig([]);
      new s3deploy.BucketDeployment(this, "ConfigDeployment", {
        sources: [s3deploy.Source.data("bootstrap.toml", configContent)],
        destinationBucket: buildsBucket,
      });

      const bssAsg = createEc2Asg(
        this,
        "BssAsg",
        this.vpc,
        this.vpc.isolatedSubnets[0],
        sg,
        ec2Role,
        bssInstanceTypes,
        1,
        1,
        "bss_server",
      );

      targetIdOutput = new cdk.CfnOutput(this, "bssAsgName", {
        value: bssAsg.autoScalingGroupName,
        description: `Bss Auto Scaling Group Name`,
      });
    }

    // Output the relevant ID (instance ID or ASG name)
    targetIdOutput;
  }
}
