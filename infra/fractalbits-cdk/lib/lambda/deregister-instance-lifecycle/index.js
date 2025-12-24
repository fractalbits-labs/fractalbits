const {
  DynamoDBClient,
  UpdateItemCommand,
} = require("@aws-sdk/client-dynamodb");
const {
  AutoScalingClient,
  CompleteLifecycleActionCommand,
} = require("@aws-sdk/client-auto-scaling");
const dynamodb = new DynamoDBClient({});
const autoscaling = new AutoScalingClient({});
exports.handler = async (event) => {
  console.log("Event: ", JSON.stringify(event, null, 2));
  const message = JSON.parse(event.Records[0].Sns.Message);
  const lifecycleHookName = message.LifecycleHookName;
  const autoScalingGroupName = message.AutoScalingGroupName;
  const lifecycleActionToken = message.LifecycleActionToken;
  const ec2InstanceId = message.EC2InstanceId;
  const serviceId = process.env.SERVICE_ID;
  const tableName = process.env.TABLE_NAME || "fractalbits-service-discovery";
  if (!serviceId) {
    console.error("SERVICE_ID not found in environment variables.");
    await completeLifecycleAction(
      lifecycleHookName,
      autoScalingGroupName,
      lifecycleActionToken,
      "ABANDON",
      ec2InstanceId,
    );
    return;
  }
  try {
    console.log(
      `Deregistering instance ${ec2InstanceId} from service ${serviceId} in DynamoDB table ${tableName}`,
    );

    // Remove the instance from the instances map in DynamoDB
    const params = {
      TableName: tableName,
      Key: {
        service_id: { S: serviceId },
      },
      UpdateExpression: "REMOVE instances.#instance_id",
      ExpressionAttributeNames: {
        "#instance_id": ec2InstanceId,
      },
      ConditionExpression: "attribute_exists(instances.#instance_id)",
      ReturnValues: "UPDATED_NEW",
    };

    try {
      const result = await dynamodb.send(new UpdateItemCommand(params));
      console.log("Successfully deregistered instance from DynamoDB:", result);
    } catch (error) {
      if (error.name === "ConditionalCheckFailedException") {
        console.log(
          `Instance ${ec2InstanceId} not found in service ${serviceId}, nothing to deregister. It might have been deregistered already.`,
        );
      } else {
        throw error;
      }
    }

    await completeLifecycleAction(
      lifecycleHookName,
      autoScalingGroupName,
      lifecycleActionToken,
      "CONTINUE",
      ec2InstanceId,
    );
  } catch (error) {
    console.error("Error during deregistration process:", error);
    await completeLifecycleAction(
      lifecycleHookName,
      autoScalingGroupName,
      lifecycleActionToken,
      "ABANDON",
      ec2InstanceId,
    );
  }
};
async function completeLifecycleAction(
  hookName,
  groupName,
  token,
  result,
  instanceId,
) {
  const params = {
    LifecycleHookName: hookName,
    AutoScalingGroupName: groupName,
    LifecycleActionToken: token,
    LifecycleActionResult: result,
    InstanceId: instanceId,
  };
  console.log(
    `Completing lifecycle action for instance ${instanceId} with result ${result}`,
  );
  try {
    await autoscaling.send(new CompleteLifecycleActionCommand(params));
  } catch (err) {
    console.error(`Failed to complete lifecycle action: ${err}`);
  }
}
