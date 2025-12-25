# On-Premises Cluster Creation Guide

This guide explains how to deploy a Fractalbits cluster on bare-metal or on-premises infrastructure.

## Overview

On-prem deployment uses:
- **Fractalbits all-in-one docker image** as an S3-compatible bootstrap coordination store
- **etcd** for service discovery and metadata storage
- **SSH** for remote node bootstrapping

## Prerequisites

### On Each Node

1. Linux OS (Ubuntu 22.04+ or RHEL 8+)
2. SSH server running with key-based authentication
3. Passwordless sudo access for bootstrap user
4. Network connectivity between all nodes
5. AWS CLI v2 installed (used for S3-compatible API calls)

### On Deployment Machine

1. SSH access to all cluster nodes
2. Rust toolchain (1.88+) with `cargo` installed
3. AWS CLI v2 installed

## Step 1: Start Fractalbits Docker for Bootstrap Coordination

Start a Fractalbits container to serve as the bootstrap S3 endpoint. See [Quick Start - Docker](../README.md#quick-start---docker) for detailed Docker usage.

```bash
# Run the public image (recommended)
docker run -d --privileged --name fractalbits-bootstrap \
    -p 8080:8080 -p 18080:18080 \
    -v fractalbits-data:/data \
    ghcr.io/fractalbits-labs/fractalbits-main:latest

# Verify container is running
curl http://localhost:18080/mgmt/health

# Create the bootstrap bucket
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret
export AWS_ENDPOINT_URL_S3=http://localhost:8080
export AWS_DEFAULT_REGION=localdev

aws s3 mb s3://fractalbits-bootstrap
```

## Step 2: Build and Upload Bootstrap Artifacts

Build the fractalbits binaries for your target architecture:

```bash
# Build for on-prem (use appropriate CPU target)
cargo xtask build --cpu-target i3  # or graviton3 for ARM

# Upload bootstrap artifacts to the bootstrap container
cargo xtask deploy upload --deploy-target on-prem
```

## Step 3: Create Cluster Configuration

Create a `cluster.toml` file describing your cluster topology.

### Configuration File Format

```toml
[global]
# Number of BSS (Blob Storage Server) nodes
num_bss_nodes = 6

# Enable RSS high availability (requires 2 root_server nodes)
rss_ha_enabled = true

# CPU target for binary selection (must match build target)
# cpu_target = "i3"

# Enable benchmarking mode (deploys bench_server and bench_client nodes)
# for_bench = false

# Optional: Number of API server nodes (auto-detected from nodes list if not set)
# num_api_servers = 2

# Optional: Number of benchmark client nodes (only used when for_bench = true)
# num_bench_clients = 4

[endpoints]
# NSS endpoint (IP or hostname of the NSS server)
# If not set, auto-detected from nodes list
nss_endpoint = "10.0.1.10"

# API server endpoint (load balancer or single API server IP)
# Required if for_bench = true
api_server_endpoint = "api.local:8080"

# Node definitions grouped by service type
# Each node entry has:
#   - ip: Node IP address (required)
#   - hostname: Optional hostname (defaults to IP)
#   - role: For root_server only - "leader" or "follower"
#   - volume_id: For nss_server only - logical volume identifier
#   - bench_client_num: For bench_server only - number of benchmark clients

# Root State Store servers
[[nodes.root_server]]
ip = "10.0.1.5"
hostname = "rss-leader"
role = "leader"

[[nodes.root_server]]
ip = "10.0.1.6"
hostname = "rss-follower"
role = "follower"

# Namespace Server
[[nodes.nss_server]]
ip = "10.0.1.10"
hostname = "nss-1"
volume_id = "vol-nss-1"

# Blob Storage Servers
[[nodes.bss_server]]
ip = "10.0.1.20"
hostname = "bss-1"

[[nodes.bss_server]]
ip = "10.0.1.21"
hostname = "bss-2"

[[nodes.bss_server]]
ip = "10.0.1.22"
hostname = "bss-3"

[[nodes.bss_server]]
ip = "10.0.1.23"
hostname = "bss-4"

[[nodes.bss_server]]
ip = "10.0.1.24"
hostname = "bss-5"

[[nodes.bss_server]]
ip = "10.0.1.25"
hostname = "bss-6"

# API Server
[[nodes.api_server]]
ip = "10.0.1.30"
hostname = "api-1"
```

### Service Types

| Service Type | Description | Required Count |
|--------------|-------------|----------------|
| `root_server` | Root State Store server (leader election, metadata coordination) | 1 (or 2 with HA) |
| `nss_server` | Namespace Server (handles S3 API routing) | 1 |
| `bss_server` | Blob Storage Server (stores actual data) | 3-12 (recommend 6) |
| `api_server` | S3 API endpoint server | 1+ |
| `bench_server` | Benchmark coordinator (optional) | 0-1 |
| `bench_client` | Benchmark workers (optional) | 0+ |

### Minimal Cluster Example

A minimal cluster for testing:

```toml
[global]
num_bss_nodes = 3

[[nodes.root_server]]
ip = "10.0.1.5"
role = "leader"

[[nodes.nss_server]]
ip = "10.0.1.10"

[[nodes.bss_server]]
ip = "10.0.1.20"

[[nodes.bss_server]]
ip = "10.0.1.21"

[[nodes.bss_server]]
ip = "10.0.1.22"

[[nodes.api_server]]
ip = "10.0.1.30"
```

## Step 4: Create the Cluster

Run the cluster creation command:

```bash
cargo xtask deploy create-cluster \
  --config ./cluster.toml \
  --bootstrap-s3-url <BOOTSTRAP_IP>:8080
```

Replace `<BOOTSTRAP_IP>` with the IP address of the Fractalbits bootstrap container accessible from all nodes.

### What Happens During Cluster Creation

1. **Config Generation**: The `cluster.toml` is transformed into `bootstrap_cluster.toml` format
2. **S3 Upload**: Bootstrap config is uploaded to `s3://fractalbits-bootstrap/bootstrap_cluster.toml`
3. **Node Bootstrap**: For each node, SSH runs the bootstrap script:
   ```bash
   aws s3 cp s3://fractalbits-bootstrap/bootstrap.sh - | sh
   ```
4. **Service Discovery**: Nodes register with etcd and discover each other
5. **Service Start**: Each node starts its assigned service (systemd units)

## SSH Requirements

The deployment machine must be able to SSH to all nodes:

```bash
# Test SSH connectivity to all nodes
ssh 10.0.1.5 "hostname"
ssh 10.0.1.6 "hostname"
# ... etc
```

SSH configuration recommendations:

```bash
# ~/.ssh/config
Host 10.0.1.*
    User fractalbits
    IdentityFile ~/.ssh/fractalbits-cluster.pem
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
```

Ensure the SSH user has passwordless sudo:

```bash
# On each node, as root:
echo "fractalbits ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/fractalbits
```

## Environment Variables on Nodes

The bootstrap process sets these environment variables for S3 access:

| Variable | Value | Description |
|----------|-------|-------------|
| `AWS_DEFAULT_REGION` | `on-prem` | Region for S3 API calls |
| `AWS_ENDPOINT_URL_S3` | `http://<bootstrap-ip>:8080` | Bootstrap container S3 endpoint |
| `AWS_ACCESS_KEY_ID` | `test_api_key` | Bootstrap container credentials |
| `AWS_SECRET_ACCESS_KEY` | `test_api_secret` | Bootstrap container credentials |

## Verifying the Cluster

After cluster creation completes:

```bash
# Check service status on each node
ssh 10.0.1.5 "systemctl status rss"
ssh 10.0.1.10 "systemctl status nss"
ssh 10.0.1.20 "systemctl status bss"
ssh 10.0.1.30 "systemctl status api_server"

# Check etcd cluster health (from any BSS node)
ssh 10.0.1.20 "etcdctl endpoint health --cluster"

# Test S3 API (from deployment machine)
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>
export AWS_ENDPOINT_URL=http://10.0.1.30:8080

aws s3 mb s3://test-bucket
aws s3 cp /etc/hosts s3://test-bucket/test-file
aws s3 ls s3://test-bucket/
```

## Troubleshooting

### Check Bootstrap Logs

```bash
# On any node
ssh <node-ip> "journalctl -u fractalbits-bootstrap"
```

### Check Service Logs

```bash
# Root Server logs
ssh <rss-ip> "journalctl -u rss"

# NSS logs
ssh <nss-ip> "journalctl -u nss"

# BSS logs
ssh <bss-ip> "journalctl -u bss"

# API Server logs
ssh <api-ip> "journalctl -u api_server"
```

### Common Issues

1. **SSH connection refused**: Ensure SSH server is running and firewall allows port 22
2. **S3 access denied**: Check bootstrap container credentials and bucket permissions
3. **Service failed to start**: Check node has required dependencies (see Prerequisites)
4. **etcd cluster unhealthy**: Ensure all BSS nodes can reach each other on ports 2379/2380

## Firewall Rules

Ensure these ports are open between nodes:

| Port | Protocol | Service | Direction |
|------|----------|---------|-----------|
| 22 | TCP | SSH | Deployment -> All nodes |
| 80 | TCP | S3 API | Clients -> API Server |
| 2379 | TCP | etcd client | BSS <-> BSS, RSS <-> BSS |
| 2380 | TCP | etcd peer | BSS <-> BSS |
| 8088 | TCP | Service port | RSS <-> NSS <-> BSS <-> API Server |
| 18088 | TCP | Management | Internal health checks |
