# API Key Management on VPC

This guide explains how to create and manage API keys on a deployed VPC stack.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Access to the deployed VPC stack
- SSM (Systems Manager) access to the RSS instance

## Step 1: Get VPC Stack Information

First, identify the RSS instance where API keys are managed:

```bash
just describe-stack
```

This will output information about all instances in the stack:

```
Name                                      InstanceId           State    InstanceType  AvailabilityZone  ZoneId    PrivateIP
FractalbitsVpcStack/S3GatewayAsgTemplate  i-0dc3f71d451869872  running  c8g.xlarge    us-west-2c        usw2-az3  10.0.0.30
FractalbitsVpcStack/BssAsgTemplate        i-091d633bef683dd77  running  i8g.2xlarge   us-west-2c        usw2-az3  10.0.0.47
FractalbitsVpcStack/nss-A                 i-05efb2e685f5133e7  running  m7gd.2xlarge  us-west-2c        usw2-az3  10.0.0.51
FractalbitsVpcStack/rss-A                 i-0b97a853c3e223dc0  running  c7g.medium    us-west-2c        usw2-az3  10.0.0.239

S3 Gateway NLB Endpoint: Fracta-ApiNL-MjXkOlrQipZk-46642fe5831d2804.elb.us-west-2.amazonaws.com
```

Note the Instance ID of the RSS instance (look for the row with name ending in `/rss-A`).

## Step 2: Connect to RSS Instance

Start an SSM session to the RSS instance:

```bash
ec2=<RSS_INSTANCE_ID>
aws ssm start-session --target=$ec2
```

Example:
```bash
ec2=i-0b97a853c3e223dc0
aws ssm start-session --target=$ec2
```

## Step 3: Create an API Key

Once connected to the RSS instance, use the `rss_admin` tool to create a new API key:

```bash
/opt/fractalbits/bin/rss_admin --rss-addr=localhost:8088 api-key create --name <KEY_NAME>
```

Example:
```bash
/opt/fractalbits/bin/rss_admin --rss-addr=localhost:8088 api-key create --name my_cloud_api_key
```

The output will display the credentials:

```
build info: main:main-7c287ed/root_server:main-16682ab, build time: 1763997568
 INFO rss_admin: Creating RSS RPC client for localhost:8088
 INFO rss_admin: Successfully created API key for my_cloud_api_key
Access Key name: my_cloud_api_key
Access Key ID: 52ff329f0b4c97dc8726f7cf
Secret Access Key: 994e92eb3a020bce69ecc05243b627707f0fc40e2bb2be6732c092e2cf51a4ea
```

**Important:** Save the Secret Access Key immediately, as it cannot be retrieved later.

## Step 4: List Existing API Keys

To verify the API key was created or to list all existing keys:

```bash
/opt/fractalbits/bin/rss_admin --rss-addr=localhost:8088 api-key list
```

Example output:
```
build info: main:main-7c287ed/root_server:main-16682ab, build time: 1763997568
 INFO rss_admin: Creating RSS RPC client for localhost:8088
API Keys:
=========
- my_cloud_api_key (52ff329f0b4c97dc8726f7cf)
```

Note that the list command only shows the Access Key ID, not the Secret Access Key.

## Using the API Key

Once created, use the credentials to authenticate S3 API requests against the S3 Gateway NLB endpoint shown in the stack description.

Configure your S3 client with:
- **Endpoint URL**: The S3 Gateway NLB Endpoint from `just describe-stack`
- **Access Key ID**: From the create command output
- **Secret Access Key**: From the create command output

## Additional rss_admin Commands

For other API key management operations, run:

```bash
/opt/fractalbits/bin/rss_admin --rss-addr=localhost:8088 api-key --help
```
