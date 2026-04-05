import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";

interface PeeringStackProps extends cdk.StackProps {
  readonly vpcA: ec2.Vpc;
  readonly vpcB: ec2.Vpc;
}

export class PeeringStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: PeeringStackProps) {
    super(scope, id, props);

    const vpcPeeringConnection = new ec2.CfnVPCPeeringConnection(
      this,
      "VpcPeeringConnection",
      {
        vpcId: props.vpcA.vpcId,
        peerVpcId: props.vpcB.vpcId,
      },
    );

    // Add routes to the route tables of both VPCs to enable communication
    // over the peering connection.
    // new ec2.CfnRoute(this, 'RouteFromAToBInPublic', {
    //   routeTableId: props.vpcA.selectSubnets({ subnetType: ec2.SubnetType.PUBLIC }).subnets[0].routeTable.routeTableId,
    //   destinationCidrBlock: props.vpcB.vpcCidrBlock,
    //   vpcPeeringConnectionId: vpcPeeringConnection.ref,
    // });

    new ec2.CfnRoute(this, "RouteFromAToB", {
      routeTableId: props.vpcA.selectSubnets({
        subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      }).subnets[0].routeTable.routeTableId,
      destinationCidrBlock: props.vpcB.vpcCidrBlock,
      vpcPeeringConnectionId: vpcPeeringConnection.ref,
    });

    new ec2.CfnRoute(this, "RouteFromBToA", {
      routeTableId: props.vpcB.selectSubnets({
        subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      }).subnets[0].routeTable.routeTableId,
      destinationCidrBlock: props.vpcA.vpcCidrBlock,
      vpcPeeringConnectionId: vpcPeeringConnection.ref,
    });
  }
}
