import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as elbv2 from "aws-cdk-lib/aws-elasticloadbalancingv2";
import * as autoscaling from "aws-cdk-lib/aws-autoscaling";
import * as cr from "aws-cdk-lib/custom-resources";
import * as crypto from "crypto";

import {
  createInstance,
  createUserData,
  createEc2Asg,
  createEbsVolume,
  createDynamoDbTable,
  createEc2Role,
  createVpcEndpoints,
  createPrivateLinkNlb,
  addAsgDynamoDbDeregistrationLifecycleHook,
  getAzNameFromIdAtBuildTime,
} from "./ec2-utils";
import {
  createConfigWithCfnTokens,
  DataBlobStorage,
} from "./toml-config-builder";

export interface FractalbitsVpcStackProps extends cdk.StackProps {
  numApiServers: number;
  numBenchClients: number;
  numBssNodes: number;
  benchType?: "service_endpoint" | "external" | null;
  az: string;
  bssInstanceTypes: string;
  apiServerInstanceType: string;
  benchClientInstanceType: string;
  nssInstanceType: string;
  browserIp?: string;
  dataBlobStorage: DataBlobStorage;
  rootServerHa: boolean;
  ebsVolumeSize: number;
  ebsVolumeIops: number;
  rssBackend: "etcd" | "ddb";
  journalType: "ebs" | "nvme";
}

function isSingleAzMode(mode: DataBlobStorage): boolean {
  return mode === "all_in_bss_single_az" || mode === "s3_hybrid_single_az";
}

export class FractalbitsVpcStack extends cdk.Stack {
  public readonly nlbLoadBalancerDnsName: string;
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: FractalbitsVpcStackProps) {
    super(scope, id, props);
    const dataBlobStorage = props.dataBlobStorage;
    const singleAz = isSingleAzMode(dataBlobStorage);
    const multiAz = dataBlobStorage === "s3_express_multi_az";

    // === VPC Configuration ===
    // Parse az based on deployment mode
    // singleAz: single AZ ID (e.g., "usw2-az3")
    // multiAz: AZ pair (e.g., "usw2-az3,usw2-az4")
    const azArray = props.az.split(",");

    // Validate az format based on deployment mode
    if (singleAz && azArray.length !== 1) {
      throw new Error(
        `Single-AZ mode requires single AZ ID (e.g., "usw2-az3"), got: "${props.az}"`,
      );
    }
    if (multiAz && azArray.length !== 2) {
      throw new Error(
        `Multi-AZ mode requires AZ pair (e.g., "usw2-az3,usw2-az4"), got: "${props.az}"`,
      );
    }

    // Resolve AZ IDs to actual AZ names
    const az1 = getAzNameFromIdAtBuildTime(azArray[0]);
    // Only resolve second AZ for multi-AZ mode
    const az2 = multiAz ? getAzNameFromIdAtBuildTime(azArray[1]) : "";

    // Determine availability zones based on storage mode
    const availabilityZones = singleAz
      ? [az1] // Single AZ for single-AZ mode
      : [az1, az2]; // Multi-AZ for multi-AZ mode

    // Create VPC with specific availability zones using resolved zone names
    this.vpc = new ec2.Vpc(this, "FractalbitsVpc", {
      vpcName: "fractalbits-vpc",
      ipAddresses: ec2.IpAddresses.cidr("10.0.0.0/16"),
      availabilityZones,
      natGateways: 0,
      enableDnsHostnames: true,
      enableDnsSupport: true,
      subnetConfiguration: [
        {
          name: "PrivateSubnet",
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        },
        {
          name: "PublicSubnet",
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
      ],
    });

    const ec2Role = createEc2Role(this);
    createVpcEndpoints(this.vpc);

    const publicSg = new ec2.SecurityGroup(this, "PublicInstanceSG", {
      vpc: this.vpc,
      securityGroupName: "FractalbitsPublicInstanceSG",
      description:
        "Allow inbound on port 80 for public access, and all outbound",
      allowAllOutbound: true,
    });
    publicSg.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(80),
      "Allow HTTP access from anywhere",
    );

    const privateSg = new ec2.SecurityGroup(this, "PrivateInstanceSG", {
      vpc: this.vpc,
      securityGroupName: "FractalbitsPrivateInstanceSG",
      description:
        "Allow inbound on port 8088 (e.g., from internal sources), and all outbound",
      allowAllOutbound: true,
    });
    privateSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(80),
      "Allow access to port 80 from within VPC",
    );
    privateSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(8088),
      "Allow access to port 8088 from within VPC",
    );
    privateSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(18088),
      "Allow access to port 18088 (management) from within VPC",
    );
    privateSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(9999),
      "Allow access to port 9999 from within VPC",
    );
    if (props.benchType == "external") {
      // Allow incoming traffic on port 7761 for bench clients
      privateSg.addIngressRule(
        ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
        ec2.Port.tcp(7761),
        "Allow access to port 7761 from within VPC",
      );
    }

    // Add etcd ports when using etcd backend
    if (props.rssBackend === "etcd") {
      privateSg.addIngressRule(
        ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
        ec2.Port.tcp(2379),
        "Allow etcd client access from within VPC",
      );
      privateSg.addIngressRule(
        ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
        ec2.Port.tcp(2380),
        "Allow etcd peer-to-peer communication within VPC",
      );
    }

    // Add ports for simulate-on-prem mode
    const simulateOnPrem = this.node.tryGetContext("simulateOnPrem") === "true";
    if (simulateOnPrem) {
      privateSg.addIngressRule(
        ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
        ec2.Port.tcp(22),
        "Allow SSH access from within VPC (simulate-on-prem)",
      );
      privateSg.addIngressRule(
        ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
        ec2.Port.tcp(8080),
        "Allow Docker S3 API access from within VPC (simulate-on-prem)",
      );
      privateSg.addIngressRule(
        ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
        ec2.Port.tcp(18080),
        "Allow Docker mgmt API access from within VPC (simulate-on-prem)",
      );
      // simulate-on-prem always uses etcd
      privateSg.addIngressRule(
        ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
        ec2.Port.tcp(2379),
        "Allow etcd client access from within VPC (simulate-on-prem)",
      );
      privateSg.addIngressRule(
        ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
        ec2.Port.tcp(2380),
        "Allow etcd peer-to-peer communication within VPC (simulate-on-prem)",
      );
    }

    // Create data blob bucket only for s3_hybrid_single_az mode
    let dataBlobBucket: s3.Bucket | undefined;
    if (dataBlobStorage === "s3_hybrid_single_az") {
      dataBlobBucket = new s3.Bucket(this, "DataBlobBucket", {
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        autoDeleteObjects: true,
      });
    }

    // Create DynamoDB tables
    createDynamoDbTable(
      this,
      "FractalbitsTable",
      "fractalbits-api-keys-and-buckets",
      "id",
    );
    createDynamoDbTable(
      this,
      "ServiceDiscoveryTable",
      "fractalbits-service-discovery",
      "service_id",
    );
    createDynamoDbTable(
      this,
      "LeaderElectionTable",
      "fractalbits-leader-election",
      "key",
    );

    // Define instance metadata, and create instances
    const nssInstanceType = new ec2.InstanceType(props.nssInstanceType);
    const rssInstanceType = new ec2.InstanceType(
      simulateOnPrem ? "c7i.4xlarge" : "c7g.medium",
    );
    const benchInstanceType = new ec2.InstanceType("c7g.large");

    // Get specific subnets for instances to ensure correct AZ placement
    const privateSubnets = this.vpc.isolatedSubnets;
    const publicSubnets = this.vpc.publicSubnets;
    const subnet1 = privateSubnets[0]; // First AZ (private)
    // Only get second subnet for multi-AZ mode
    const subnet2 =
      multiAz && privateSubnets.length > 1
        ? privateSubnets[1] // Second AZ (private)
        : subnet1; // Use first subnet for single-AZ mode
    const publicSubnet1 = publicSubnets[0]; // First AZ (public)

    const instanceConfigs = [
      {
        id: "rss-A",
        instanceType: rssInstanceType,
        specificSubnet: subnet1,
        sg: privateSg,
      },
      {
        id: "nss-A",
        instanceType: nssInstanceType,
        specificSubnet: subnet1,
        sg: privateSg,
      },
    ];

    // Only create rss-B when rootServerHa is enabled
    if (props.rootServerHa) {
      instanceConfigs.splice(1, 0, {
        id: "rss-B",
        instanceType: rssInstanceType,
        // For singleAz, place rss-B in same AZ as rss-A
        specificSubnet: singleAz ? subnet1 : subnet2,
        sg: privateSg,
      });
    }

    // Create nss-B for multiAz mode OR when using nvme journal type (active/standby)
    if (multiAz || props.journalType === "nvme") {
      instanceConfigs.push({
        id: "nss-B",
        instanceType: nssInstanceType,
        // For single-AZ nvme, place nss-B in same subnet as nss-A
        specificSubnet: multiAz ? subnet2 : subnet1,
        sg: privateSg,
      });
    }

    if (props.browserIp) {
      const guiServerSg = new ec2.SecurityGroup(this, "GuiServerSG", {
        vpc: this.vpc,
        securityGroupName: "FractalbitsGuiServerSG",
        description: "Allow inbound on port 80 from a specific IP address",
        allowAllOutbound: true,
      });
      guiServerSg.addIngressRule(
        ec2.Peer.ipv4(`${props.browserIp}/32`),
        ec2.Port.tcp(80),
        "Allow access to port 80 from specific IP",
      );
      instanceConfigs.push({
        id: "gui_server",
        instanceType: new ec2.InstanceType("c8g.large"),
        specificSubnet: publicSubnet1,
        sg: guiServerSg,
      });
    }

    // Read skipUserData context for ASGs (SSM-based bootstrap)
    const skipUserDataCtx = this.node.tryGetContext("skipUserData") === "true";
    // Read useGenericBinaries context (for using on-prem style generic binaries)
    const useGenericBinaries =
      this.node.tryGetContext("useGenericBinaries") === "true";

    let benchClientAsg: autoscaling.AutoScalingGroup | undefined;
    if (props.benchType === "external") {
      // Create bench_server
      instanceConfigs.push({
        id: "bench_server",
        instanceType: benchInstanceType,
        specificSubnet: subnet1,
        sg: privateSg,
      });
      // Create bench_clients in a ASG group
      benchClientAsg = createEc2Asg(
        this,
        "benchClientAsg",
        this.vpc,
        subnet1,
        privateSg,
        ec2Role,
        [props.benchClientInstanceType],
        props.numBenchClients,
        props.numBenchClients,
        "bench_client",
        skipUserDataCtx,
      );
      // Add lifecycle hook for bench_client ASG
      addAsgDynamoDbDeregistrationLifecycleHook(
        this,
        "BenchClient",
        benchClientAsg,
        "bench-client",
        "fractalbits-service-discovery",
      );
    }
    const instances: Record<string, ec2.Instance> = {};
    instanceConfigs.forEach(({ id, instanceType, sg, specificSubnet }) => {
      instances[id] = createInstance(
        this,
        this.vpc,
        id,
        specificSubnet,
        instanceType,
        sg,
        ec2Role,
      );
    });

    // Create BSS nodes in ASG (dynamic cluster discovery via S3)
    let bssAsg: autoscaling.AutoScalingGroup | undefined;
    if (singleAz) {
      bssAsg = createEc2Asg(
        this,
        "BssAsg",
        this.vpc,
        subnet1,
        privateSg,
        ec2Role,
        [props.bssInstanceTypes.split(",")[0]],
        props.numBssNodes,
        props.numBssNodes,
        "bss_server",
        skipUserDataCtx,
      );
    }

    // Create PrivateLink setup for NSS and RSS services
    const servicePort = 8088;
    const mirrordPort = 9999;

    // NSS PrivateLink - only for multiAz mode
    // For single-AZ, use direct instance IP to avoid VPC endpoint latency
    let nssPrivateLink: any;
    if (multiAz) {
      const nssTargets = [instances["nss-A"], instances["nss-B"]];
      nssPrivateLink = createPrivateLinkNlb(
        this,
        "Nss",
        this.vpc,
        nssTargets,
        servicePort,
      );
    }

    // Create mirrord PrivateLink only for multiAz mode
    // For single-AZ nvme, use direct IP instead
    let mirrordPrivateLink: any;
    if (multiAz) {
      mirrordPrivateLink = createPrivateLinkNlb(
        this,
        "Mirrord",
        this.vpc,
        [instances["nss-A"], instances["nss-B"]],
        mirrordPort,
      );
    }

    // Determine NSS endpoint based on mode
    const nssEndpoint = multiAz
      ? nssPrivateLink.endpointDns
      : `${instances["nss-A"].instancePrivateIp}`;

    // Create api_server(s) in a ASG group
    const apiServerAsg = createEc2Asg(
      this,
      "ApiServerAsg",
      this.vpc,
      subnet1,
      privateSg,
      ec2Role,
      [props.apiServerInstanceType],
      props.numApiServers,
      props.numApiServers,
      "api_server",
      skipUserDataCtx,
    );

    // Add lifecycle hook for api_server ASG
    addAsgDynamoDbDeregistrationLifecycleHook(
      this,
      "ApiServer",
      apiServerAsg,
      "api-server",
      "fractalbits-service-discovery",
    );

    // NLB for API servers - always create regardless of benchType
    const nlb = new elbv2.NetworkLoadBalancer(this, "ApiNLB", {
      vpc: this.vpc,
      internetFacing: false,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      crossZoneEnabled: multiAz, // Only enable cross-zone for multi-AZ
    });
    const listener = nlb.addListener("ApiListener", { port: 80 });
    listener.addTargets("ApiTargets", {
      port: 80,
      targets: [apiServerAsg],
      healthCheck: {
        enabled: true,
        healthyThresholdCount: 2,
        unhealthyThresholdCount: 2,
        interval: cdk.Duration.seconds(5),
        timeout: cdk.Duration.seconds(2),
      },
    });

    // Create EBS Volumes for nss_servers only when journalType is "ebs"
    let ebsVolumeAId: string = "";
    let ebsVolumeBId: string = "";
    if (props.journalType === "ebs") {
      const ebsVolumeA = createEbsVolume(
        this,
        "MultiAttachVolumeA",
        subnet1.availabilityZone,
        instances["nss-A"].instanceId,
        props.ebsVolumeSize,
        props.ebsVolumeIops,
      );
      ebsVolumeAId = ebsVolumeA.volumeId;

      // Only create volume B for multiAz mode
      if (multiAz) {
        const ebsVolumeB = createEbsVolume(
          this,
          "MultiAttachVolumeB",
          subnet2.availabilityZone,
          instances["nss-B"].instanceId,
          props.ebsVolumeSize,
          props.ebsVolumeIops,
        );
        ebsVolumeBId = ebsVolumeB.volumeId;
      }
    }

    // Add UserData to all instances - they will discover their role from TOML config
    // Skip if skipUserData context is set (for SSM-based bootstrap)
    const skipUserData = this.node.tryGetContext("skipUserData") === "true";
    if (!skipUserData) {
      for (const instance of Object.values(instances)) {
        instance.addUserData(createUserData(this).render());
      }
    }

    // Outputs
    if (dataBlobBucket) {
      new cdk.CfnOutput(this, "DataBlobBucketName", {
        value: dataBlobBucket.bucketName,
        description: "S3 bucket for data blob storage (hybrid mode)",
      });
    }

    for (const [id, instance] of Object.entries(instances)) {
      new cdk.CfnOutput(this, `${id} Id`, {
        value: instance.instanceId,
        description: `EC2 instance ${id} ID`,
      });
    }

    new cdk.CfnOutput(this, "ApiNLBDnsName", {
      value: nlb.loadBalancerDnsName,
      description: "DNS name of the API NLB",
    });

    this.nlbLoadBalancerDnsName = nlb.loadBalancerDnsName;

    // Only output EBS volume IDs when journalType is "ebs"
    if (ebsVolumeAId) {
      new cdk.CfnOutput(this, "VolumeAId", {
        value: ebsVolumeAId,
        description: "EBS volume A ID",
      });
    }

    // Only output volume B for multiAz mode with EBS
    if (multiAz && ebsVolumeBId) {
      new cdk.CfnOutput(this, "VolumeBId", {
        value: ebsVolumeBId,
        description: "EBS volume B ID",
      });
    }

    new cdk.CfnOutput(this, "apiServerAsgName", {
      value: apiServerAsg.autoScalingGroupName,
      description: `Api Server Auto Scaling Group Name`,
    });

    if (benchClientAsg) {
      new cdk.CfnOutput(this, "benchClientAsgName", {
        value: benchClientAsg.autoScalingGroupName,
        description: `Bench Client Auto Scaling Group Name`,
      });
    }

    if (bssAsg) {
      new cdk.CfnOutput(this, "bssAsgName", {
        value: bssAsg.autoScalingGroupName,
        description: `BSS Auto Scaling Group Name`,
      });
    }

    if (props.browserIp) {
      new cdk.CfnOutput(this, "GuiServerPublicIp", {
        value: instances["gui_server"].instancePublicIp,
        description: "Public IP of the GUI Server",
      });
    }

    // Generate and upload bootstrap TOML config to S3
    const bootstrapBucket = `fractalbits-bootstrap-${this.region}-${this.account}`;

    // Generate unique workflow cluster ID for S3-based bootstrap coordination
    // Each deployment gets fresh workflow state to avoid stale barrier objects
    const workflowClusterId = `fractalbits-${Date.now()}`;

    // Generate shared journal UUID for EBS volumes (used for filesystem UUID and mount point)
    // Both NSS and mirrord use the same journal UUID
    const journalUuid =
      props.journalType === "ebs" ? crypto.randomUUID() : undefined;

    const bootstrapConfigContent = createConfigWithCfnTokens({
      deployTarget: "aws",
      region: cdk.Stack.of(this).region,
      forBench: !!props.benchType,
      dataBlobStorage: dataBlobStorage,
      rssHaEnabled: props.rootServerHa,
      rssBackend: props.rssBackend,
      journalType: props.journalType,
      numBssNodes: singleAz ? props.numBssNodes : undefined,
      numApiServers: props.numApiServers,
      numBenchClients: props.numBenchClients,
      dataBlobBucket: dataBlobBucket?.bucketName,
      localAz: azArray[0],
      remoteAz: multiAz ? azArray[1] : undefined,
      nssEndpoint: nssEndpoint,
      apiServerEndpoint: nlb.loadBalancerDnsName,
      nssA: { id: instances["nss-A"].instanceId },
      nssB: instances["nss-B"]
        ? { id: instances["nss-B"].instanceId }
        : undefined,
      volumeAId: ebsVolumeAId,
      volumeBId: ebsVolumeBId || undefined,
      journalUuid: journalUuid,
      rssA: { id: instances["rss-A"].instanceId },
      rssB:
        props.rootServerHa && instances["rss-B"]
          ? { id: instances["rss-B"].instanceId }
          : undefined,
      guiServer:
        props.browserIp && instances["gui_server"]
          ? { id: instances["gui_server"].instanceId }
          : undefined,
      benchServer:
        props.benchType === "external" && instances["bench_server"]
          ? { id: instances["bench_server"].instanceId }
          : undefined,
      benchClientNum:
        props.benchType === "external" ? props.numBenchClients : undefined,
      workflowClusterId: workflowClusterId,
      bootstrapBucket: bootstrapBucket,
      useGenericBinaries: useGenericBinaries,
    });

    new cr.AwsCustomResource(this, "UploadBootstrapConfig", {
      onCreate: {
        service: "S3",
        action: "putObject",
        parameters: {
          Bucket: bootstrapBucket,
          Key: "bootstrap_cluster.toml",
          Body: bootstrapConfigContent,
          ContentType: "text/plain",
        },
        physicalResourceId: cr.PhysicalResourceId.of("bootstrap-config"),
      },
      onUpdate: {
        service: "S3",
        action: "putObject",
        parameters: {
          Bucket: bootstrapBucket,
          Key: "bootstrap_cluster.toml",
          Body: bootstrapConfigContent,
          ContentType: "text/plain",
        },
        physicalResourceId: cr.PhysicalResourceId.of("bootstrap-config"),
      },
      policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
        resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE,
      }),
    });
  }
}
