import * as cdk from "aws-cdk-lib";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as iam from "aws-cdk-lib/aws-iam";
import * as autoscaling from "aws-cdk-lib/aws-autoscaling";
import { Construct } from "constructs";
import * as servicediscovery from "aws-cdk-lib/aws-servicediscovery";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as hooktargets from "aws-cdk-lib/aws-autoscaling-hooktargets";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as elbv2 from "aws-cdk-lib/aws-elasticloadbalancingv2";
import * as elbv2_targets from "aws-cdk-lib/aws-elasticloadbalancingv2-targets";
import * as path from "path";
import { execSync } from "child_process";

export const getAzNameFromIdAtBuildTime = (
  azId: string,
  region?: string,
): string => {
  try {
    const targetRegion =
      region ||
      process.env.AWS_REGION ||
      process.env.AWS_DEFAULT_REGION ||
      "us-west-2";
    const result = execSync(
      `aws ec2 describe-availability-zones --region ${targetRegion} --zone-ids ${azId} --query 'AvailabilityZones[0].ZoneName' --output text`,
      { encoding: "utf-8" },
    ).trim();
    if (!result || result === "None") {
      throw new Error(`Could not find AZ name for zone ID: ${azId}`);
    }
    return result;
  } catch (error) {
    console.error(`Error resolving AZ ID ${azId}:`, error);
    throw error;
  }
};

export const createVpcEndpoints = (vpc: ec2.Vpc) => {
  // Add Gateway Endpoint for S3
  vpc.addGatewayEndpoint("S3Endpoint", {
    service: ec2.GatewayVpcEndpointAwsService.S3,
  });

  // Add Gateway Endpoint for S3 Express One Zone
  // S3 Express requires a separate gateway endpoint for optimal performance
  vpc.addGatewayEndpoint("S3ExpressEndpoint", {
    service: new ec2.GatewayVpcEndpointAwsService("s3express"),
  });

  // Add Gateway Endpoint for DynamoDB
  vpc.addGatewayEndpoint("DynamoDbEndpoint", {
    service: ec2.GatewayVpcEndpointAwsService.DYNAMODB,
  });

  // Add Interface Endpoint for EC2, SSM, and CloudWatch
  [
    "SSM",
    "SSM_MESSAGES",
    "EC2",
    "EC2_MESSAGES",
    "CLOUDWATCH",
    "CLOUDWATCH_LOGS",
  ].forEach((service) => {
    vpc.addInterfaceEndpoint(`${service}Endpoint`, {
      service: (ec2.InterfaceVpcEndpointAwsService as any)[service],
      subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      privateDnsEnabled: true,
    });
  });
};

export const createEc2Role = (scope: Construct): iam.Role => {
  const role = new iam.Role(scope, "InstanceRole", {
    assumedBy: new iam.ServicePrincipal("ec2.amazonaws.com"),
    managedPolicies: [
      iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMFullAccess"),
      iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess"),
      iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonDynamoDBFullAccess_v2"),
      iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonEC2FullAccess"),
      iam.ManagedPolicy.fromAwsManagedPolicyName("CloudWatchAgentServerPolicy"),
    ],
  });

  // Add S3 Express One Zone permissions
  role.addToPolicy(
    new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        "s3express:CreateBucket",
        "s3express:DeleteBucket",
        "s3express:CreateSession",
        "s3express:DeleteSession",
        "s3express:PutObject",
        "s3express:GetObject",
        "s3express:DeleteObject",
        "s3express:ListBucket",
      ],
      resources: ["*"],
    }),
  );

  return role;
};

export const createDynamoDbTable = (
  scope: Construct,
  id: string,
  tableName: string,
  partitionKeyName: string,
  partitionKeyType: dynamodb.AttributeType = dynamodb.AttributeType.STRING,
): dynamodb.Table => {
  return new dynamodb.Table(scope, id, {
    partitionKey: {
      name: partitionKeyName,
      type: partitionKeyType,
    },
    removalPolicy: cdk.RemovalPolicy.DESTROY,
    billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    tableName: tableName,
  });
};

export const createInstance = (
  scope: Construct,
  vpc: ec2.Vpc,
  id: string,
  specificSubnet: ec2.ISubnet,
  instanceType: ec2.InstanceType,
  sg: ec2.SecurityGroup,
  role: iam.Role,
): ec2.Instance => {
  const arch =
    instanceType.architecture === ec2.InstanceArchitecture.ARM_64
      ? "arm64"
      : "x86_64";
  const machineImage = ec2.MachineImage.fromSsmParameter(
    `/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.12-${arch}`,
  );

  return new ec2.Instance(scope, id, {
    vpc: vpc,
    instanceType: instanceType,
    machineImage: machineImage,
    vpcSubnets: { subnets: [specificSubnet] },
    securityGroup: sg,
    role: role,
  });
};

export const createUserData = (scope: Construct): ec2.UserData => {
  const region = cdk.Stack.of(scope).region;
  const account = cdk.Stack.of(scope).account;
  const bucketName = `fractalbits-bootstrap-${region}-${account}`;
  const userData = ec2.UserData.forLinux();
  userData.addCommands(
    `aws s3 cp --no-progress s3://${bucketName}/bootstrap.sh - | sh`,
  );
  return userData;
};

export interface CreateEc2AsgOptions {
  scope: Construct;
  id: string;
  vpc: ec2.Vpc;
  specificSubnet: ec2.ISubnet;
  sg: ec2.SecurityGroup;
  role: iam.Role;
  instanceTypeNames: string[];
  minCapacity: number;
  maxCapacity: number;
  serviceType?: string;
}

export const createEc2Asg = (
  scope: Construct,
  id: string,
  vpc: ec2.Vpc,
  specificSubnet: ec2.ISubnet,
  sg: ec2.SecurityGroup,
  role: iam.Role,
  instanceTypeNames: string[],
  minCapacity: number,
  maxCapacity: number,
  serviceType?: string,
): autoscaling.AutoScalingGroup => {
  if (instanceTypeNames.length === 0) {
    throw new Error("instanceTypeNames must not be empty.");
  }

  const isArmInstance = (name: string): boolean => {
    const family = name.split(".")[0];
    // Graviton (arm) instances have a 'g' after the generation number, e.g. m6g, t4g, c7g.
    // The 'a1' family is also arm.
    // G-family instances (e.g. g4dn) are for GPU, not graviton, and are x86, but g5g is arm.
    return family === "a1" || /\d[g]/.test(family);
  };

  const isArm = instanceTypeNames.every(isArmInstance);
  const isX86 = instanceTypeNames.every((name) => !isArmInstance(name));
  if (!isArm && !isX86) {
    console.error(
      "Error: both x86 and arm instance types are found, which is not supported for now.",
    );
    process.exit(1);
  }

  const arch = isArm ? "arm64" : "x86_64";
  const machineImage = ec2.MachineImage.fromSsmParameter(
    `/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.12-${arch}`,
  );

  const launchTemplate = new ec2.LaunchTemplate(scope, `${id}Template`, {
    instanceType: new ec2.InstanceType(instanceTypeNames[0]),
    machineImage: machineImage,
    securityGroup: sg,
    role: role,
    userData: createUserData(scope),
  });

  if (serviceType) {
    cdk.Tags.of(launchTemplate).add("fractalbits:ServiceType", serviceType);
  }

  const launchTemplateOverrides = instanceTypeNames.map((typeName) => ({
    instanceType: new ec2.InstanceType(typeName),
  }));

  return new autoscaling.AutoScalingGroup(scope, id, {
    vpc: vpc,
    minCapacity: minCapacity,
    maxCapacity: maxCapacity,
    vpcSubnets: { subnets: [specificSubnet] },
    newInstancesProtectedFromScaleIn: false,
    mixedInstancesPolicy: {
      instancesDistribution: {
        onDemandPercentageAboveBaseCapacity: 100,
      },
      launchTemplate: launchTemplate,
      launchTemplateOverrides: launchTemplateOverrides,
    },
  });
};

export const createEbsVolume = (
  scope: Construct,
  id: string,
  az: string,
  instanceId: string,
  volumeSize: number,
  volumeIops: number,
): ec2.Volume => {
  const ebsVolume = new ec2.Volume(scope, id, {
    removalPolicy: cdk.RemovalPolicy.DESTROY,
    availabilityZone: az,
    size: cdk.Size.gibibytes(volumeSize),
    volumeType: ec2.EbsDeviceVolumeType.IO2,
    iops: volumeIops,
    enableMultiAttach: true,
  });

  new ec2.CfnVolumeAttachment(scope, `${id}Attachment`, {
    instanceId: instanceId,
    device: "/dev/xvdf",
    volumeId: ebsVolume.volumeId,
  });

  return ebsVolume;
};

export function addAsgDeregistrationLifecycleHook(
  scope: Construct,
  id: string,
  asg: autoscaling.AutoScalingGroup,
  service: servicediscovery.Service,
) {
  const deregisterLambdaRole = new iam.Role(scope, `${id}DeregisterRole`, {
    assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
    managedPolicies: [
      iam.ManagedPolicy.fromAwsManagedPolicyName(
        "service-role/AWSLambdaBasicExecutionRole",
      ),
      iam.ManagedPolicy.fromAwsManagedPolicyName("AWSCloudMapFullAccess"),
    ],
  });

  deregisterLambdaRole.addToPolicy(
    new iam.PolicyStatement({
      actions: ["autoscaling:CompleteLifecycleAction"],
      resources: [asg.autoScalingGroupArn],
    }),
  );

  const deregisterInstanceLambda = new lambda.Function(
    scope,
    `${id}DeregisterInstanceLifecycleLambda`,
    {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(
        path.join(__dirname, "lambda/deregister-instance-lifecycle"),
      ),
      environment: {
        SERVICE_ID: service.serviceId,
      },
      role: deregisterLambdaRole,
    },
  );

  new autoscaling.LifecycleHook(scope, `${id}LifecycleHook`, {
    autoScalingGroup: asg,
    lifecycleTransition: autoscaling.LifecycleTransition.INSTANCE_TERMINATING,
    heartbeatTimeout: cdk.Duration.minutes(5),
    notificationTarget: new hooktargets.FunctionHook(deregisterInstanceLambda),
  });
}

export function addAsgDynamoDbDeregistrationLifecycleHook(
  scope: Construct,
  id: string,
  asg: autoscaling.AutoScalingGroup,
  serviceId: string,
  tableName: string = "fractalbits-service-discovery",
) {
  const deregisterLambdaRole = new iam.Role(scope, `${id}DdbDeregisterRole`, {
    assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
    managedPolicies: [
      iam.ManagedPolicy.fromAwsManagedPolicyName(
        "service-role/AWSLambdaBasicExecutionRole",
      ),
      iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonDynamoDBFullAccess_v2"),
    ],
  });

  deregisterLambdaRole.addToPolicy(
    new iam.PolicyStatement({
      actions: ["autoscaling:CompleteLifecycleAction"],
      resources: [asg.autoScalingGroupArn],
    }),
  );

  const deregisterInstanceLambda = new lambda.Function(
    scope,
    `${id}DdbDeregisterInstanceLifecycleLambda`,
    {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(
        path.join(__dirname, "lambda/deregister-instance-lifecycle"),
      ),
      environment: {
        SERVICE_ID: serviceId,
        TABLE_NAME: tableName,
      },
      role: deregisterLambdaRole,
    },
  );

  new autoscaling.LifecycleHook(scope, `${id}DdbLifecycleHook`, {
    autoScalingGroup: asg,
    lifecycleTransition: autoscaling.LifecycleTransition.INSTANCE_TERMINATING,
    heartbeatTimeout: cdk.Duration.minutes(5),
    notificationTarget: new hooktargets.FunctionHook(deregisterInstanceLambda),
  });
}

export interface PrivateLinkSetup {
  nlb: elbv2.NetworkLoadBalancer;
  endpointService: ec2.VpcEndpointService;
  endpoint: ec2.InterfaceVpcEndpoint;
  endpointDns: string;
}

export function createPrivateLinkNlb(
  scope: Construct,
  id: string,
  vpc: ec2.Vpc,
  targetInstances: ec2.Instance[],
  servicePort: number,
): PrivateLinkSetup {
  // Create Network Load Balancer
  const nlb = new elbv2.NetworkLoadBalancer(scope, `${id}Nlb`, {
    vpc,
    internetFacing: false,
    crossZoneEnabled: true,
    vpcSubnets: {
      subnets: vpc.isolatedSubnets,
    },
  });

  // Add listener and targets
  const listener = nlb.addListener(`${id}Listener`, { port: servicePort });
  listener.addTargets(`${id}Targets`, {
    port: servicePort,
    targets: targetInstances.map(
      (instance) => new elbv2_targets.InstanceTarget(instance),
    ),
  });

  // Create VPC Endpoint Service
  const endpointService = new ec2.VpcEndpointService(
    scope,
    `${id}EndpointService`,
    {
      vpcEndpointServiceLoadBalancers: [nlb],
      allowedPrincipals: [new iam.AccountRootPrincipal()],
      acceptanceRequired: false,
    },
  );

  // Create VPC Endpoint
  const endpoint = new ec2.InterfaceVpcEndpoint(scope, `${id}Endpoint`, {
    vpc,
    service: {
      name: endpointService.vpcEndpointServiceName,
      port: servicePort,
    },
    privateDnsEnabled: false,
    subnets: {
      subnets: vpc.isolatedSubnets,
    },
  });

  // Extract endpoint DNS name
  const endpointDnsEntry = cdk.Fn.select(0, endpoint.vpcEndpointDnsEntries);
  const endpointDns = cdk.Fn.select(1, cdk.Fn.split(":", endpointDnsEntry));

  return {
    nlb,
    endpointService,
    endpoint,
    endpointDns,
  };
}
