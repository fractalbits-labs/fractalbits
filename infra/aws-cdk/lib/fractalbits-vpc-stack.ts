import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as elbv2 from "aws-cdk-lib/aws-elasticloadbalancingv2";
import * as autoscaling from "aws-cdk-lib/aws-autoscaling";

import {
  createInstance,
  createEc2Asg,
  createDynamoDbTable,
  createEc2Role,
  createVpcEndpoints,
  addAsgDynamoDbDeregistrationLifecycleHook,
  getAzNameFromIdAtBuildTime,
  createUserData,
  DeployOS,
} from "./ec2-utils";

export type DataBlobStorage =
  | "all_in_bss_single_az"
  | "s3_hybrid_single_az"
  | "s3_express_multi_az";

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
  rssBackend: "etcd" | "ddb";
  journalType: "remote";
  deployOS?: DeployOS;
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

    // AL2023 (default): no NAT needed, repos are S3-hosted, AWS CLI pre-installed.
    // Ubuntu: needs NAT gateway for apt-get access to public repos.
    const deployOS = props.deployOS ?? "al2023";
    const useNatGateway = deployOS === "ubuntu";
    const privateSubnetType = useNatGateway
      ? ec2.SubnetType.PRIVATE_WITH_EGRESS
      : ec2.SubnetType.PRIVATE_ISOLATED;

    // Create VPC with specific availability zones using resolved zone names
    this.vpc = new ec2.Vpc(this, "FractalbitsVpc", {
      vpcName: "fractalbits-vpc",
      ipAddresses: ec2.IpAddresses.cidr("10.0.0.0/16"),
      availabilityZones,
      natGateways: useNatGateway ? 1 : 0,
      enableDnsHostnames: true,
      enableDnsSupport: true,
      subnetConfiguration: [
        {
          name: "PrivateSubnet",
          subnetType: privateSubnetType,
          cidrMask: 24,
        },
        ...(useNatGateway
          ? [
              {
                name: "PublicSubnet",
                subnetType: ec2.SubnetType.PUBLIC,
                cidrMask: 24,
              },
            ]
          : []),
      ],
    });

    const ec2Role = createEc2Role(this);
    createVpcEndpoints(this.vpc, privateSubnetType);

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
    const rssInstanceType = new ec2.InstanceType("c7g.xlarge");
    const benchInstanceType = new ec2.InstanceType("c7g.large");

    // Get specific subnets for instances to ensure correct AZ placement
    const privateSubnets = useNatGateway
      ? this.vpc.privateSubnets
      : this.vpc.isolatedSubnets;
    const publicSubnets = this.vpc.publicSubnets;
    const subnet1 = privateSubnets[0]; // First AZ (private)
    // Only get second subnet for multi-AZ mode
    const subnet2 =
      multiAz && privateSubnets.length > 1
        ? privateSubnets[1] // Second AZ (private)
        : subnet1; // Use first subnet for single-AZ mode
    const publicSubnet1 = publicSubnets[0]; // First AZ (public)

    const instanceConfigs: {
      id: string;
      instanceType: ec2.InstanceType;
      specificSubnet: ec2.ISubnet;
      sg: ec2.SecurityGroup;
      rootVolumeSize?: number;
    }[] = [
      {
        id: "rss-A",
        instanceType: rssInstanceType,
        specificSubnet: subnet1,
        sg: privateSg,
        rootVolumeSize: 30,
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
    }

    // Per-service UserData: each instance gets --role (and optional sub-args) appended.
    // RSS no longer needs --nss-id/--nss-ip — NSS self-registers in DynamoDB
    // service discovery on boot, and RSS leader polls that entry.
    const perServiceUserData: Record<string, ec2.UserData> = {
      "rss-A": createUserData(
        this,
        deployOS,
        "--role root_server --rss-role leader",
      ),
      "rss-B": createUserData(
        this,
        deployOS,
        "--role root_server --rss-role follower",
      ),
      gui_server: createUserData(this, deployOS, "--role gui_server"),
      // bench_server UserData is set after NLB creation so we can embed the NLB DNS name
    };

    const instances: Record<string, ec2.Instance> = {};

    // Create non-NSS, non-bench_server instances. bench_server is deferred to
    // after NLB creation so its UserData can include the NLB DNS.
    instanceConfigs
      .filter(({ id }) => id !== "bench_server")
      .forEach(({ id, instanceType, sg, specificSubnet, rootVolumeSize }) => {
        const userData =
          perServiceUserData[id] ?? createUserData(this, deployOS);
        instances[id] = createInstance(
          this,
          this.vpc,
          id,
          specificSubnet,
          instanceType,
          sg,
          ec2Role,
          deployOS,
          rootVolumeSize,
          userData,
        );
      });

    // NSS as a managed-singleton ASG (min=max=1). NSS is stateless — its
    // journal lives on the BSS quorum VG — so a replacement instance can
    // recover by re-registering and rejoining the journal.
    const nssAsg = createEc2Asg(
      this,
      "NssAsg",
      this.vpc,
      subnet1,
      privateSg,
      ec2Role,
      [props.nssInstanceType],
      1,
      1,
      "nss_server",
      deployOS,
      createUserData(this, deployOS, "--role nss_server --nss-role primary"),
    );
    addAsgDynamoDbDeregistrationLifecycleHook(
      this,
      "Nss",
      nssAsg,
      "nss-server",
      "fractalbits-service-discovery",
    );

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
        deployOS,
        createUserData(this, deployOS, "--role bss_server"),
      );
    }

    // Create api_server(s) in ASG
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
      deployOS,
      createUserData(this, deployOS, "--role api_server"),
    );

    // Add lifecycle hook for api_server ASG
    addAsgDynamoDbDeregistrationLifecycleHook(
      this,
      "ApiServer",
      apiServerAsg,
      "api-server",
      "fractalbits-service-discovery",
    );

    // Create bench_client ASG (after instances so we can pass worker UserData)
    if (props.benchType === "external") {
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
        deployOS,
        createUserData(this, deployOS, "--role bench_client"),
      );
      addAsgDynamoDbDeregistrationLifecycleHook(
        this,
        "BenchClient",
        benchClientAsg,
        "bench-client",
        "fractalbits-service-discovery",
      );
    }

    // NLB for API servers - always create regardless of benchType
    const nlb = new elbv2.NetworkLoadBalancer(this, "ApiNLB", {
      vpc: this.vpc,
      internetFacing: false,
      vpcSubnets: { subnetType: privateSubnetType },
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

    // Third pass: create bench_server after NLB so UserData can embed the NLB DNS name.
    if (props.benchType === "external") {
      const benchServerConfig = instanceConfigs.find(
        ({ id }) => id === "bench_server",
      );
      if (benchServerConfig) {
        const benchServerUserData = createUserData(
          this,
          deployOS,
          `--role bench_server --api-server-endpoint ${nlb.loadBalancerDnsName}`,
        );
        instances["bench_server"] = createInstance(
          this,
          this.vpc,
          "bench_server",
          benchServerConfig.specificSubnet,
          benchServerConfig.instanceType,
          benchServerConfig.sg,
          ec2Role,
          deployOS,
          benchServerConfig.rootVolumeSize,
          benchServerUserData,
        );
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

    new cdk.CfnOutput(this, "nssAsgName", {
      value: nssAsg.autoScalingGroupName,
      description: `NSS Auto Scaling Group Name`,
    });

    if (props.browserIp) {
      new cdk.CfnOutput(this, "GuiServerPublicIp", {
        value: instances["gui_server"].instancePublicIp,
        description: "Public IP of the GUI Server",
      });
    }

    new cdk.CfnOutput(this, "region", {
      value: this.region,
      description: "AWS region",
    });
  }
}
