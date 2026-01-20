# On-Premises Cluster Creation Guide

This guide explains how to deploy a Fractalbits cluster on bare-metal or on-premises infrastructure.

## Overview

On-prem deployment uses:
- **Fractalbits all-in-one docker image** as an S3-compatible bootstrap coordination store
- **etcd** for service discovery and metadata storage (runs on BSS nodes)
- **SSH** for remote node bootstrapping

## Prerequisites

### On Each Node

1. Linux OS (Ubuntu 22.04+ or RHEL 8+)
2. **Supported architectures**: x86_64 (Intel/AMD) or aarch64 (ARM64)
3. SSH server running with key-based authentication
4. Passwordless sudo access for bootstrap user
5. Network connectivity between all nodes
6. AWS CLI v2 installed (used for S3-compatible API calls)
7. NVMe storage device for NSS and BSS nodes (for journal/data persistence)

### On Deployment Machine

1. SSH access to all cluster nodes
2. Rust toolchain (1.88+) with `cargo` installed
3. Docker with buildx plugin (for building multi-architecture images)

## Step 1: Build Bootstrap Artifacts

Build the fractalbits binaries and Docker images for both architectures:

```bash
# Build for on-prem deployment (builds both x86_64 and aarch64 images)
cargo xtask deploy build --for-on-prem
```

This creates:
- `target/on-prem/fractalbits-x86_64.tar.gz` - Docker image for x86_64 nodes
- `target/on-prem/fractalbits-aarch64.tar.gz` - Docker image for ARM64 nodes (e.g., AWS Graviton)

Copy the appropriate Docker image to the root_server node based on its architecture and load it:

```bash
# Determine target architecture
ssh <rss-ip> "arch"  # Returns x86_64 or aarch64

# On deployment machine - copy the correct image
# For x86_64:
scp target/on-prem/fractalbits-x86_64.tar.gz <rss-ip>:/var/fractalbits-image.tar.gz
# For aarch64 (ARM64/Graviton):
scp target/on-prem/fractalbits-aarch64.tar.gz <rss-ip>:/var/fractalbits-image.tar.gz

# On root_server node
ARCH=$(arch)
gunzip -c /var/fractalbits-image.tar.gz | docker load
docker tag fractalbits:$ARCH fractalbits:latest
docker run -d --privileged --name fractalbits-bootstrap \
    -p 8080:8080 -p 18080:18080 \
    -v fractalbits-data:/data \
    fractalbits:latest
```

## Step 2: Create Cluster Configuration

Create a `cluster.toml` file describing your cluster topology.

### Configuration File Format

```toml
[global]
# Number of BSS (Blob Storage Server) nodes
num_bss_nodes = 6

# Enable benchmarking mode (deploys bench_server and bench_client nodes)
for_bench = false

# Number of benchmark client nodes (only used when for_bench = true)
# num_bench_clients = 4

# Optional: Number of API server nodes (auto-detected from nodes list if not set)
# num_api_servers = 2

[endpoints]
# API server endpoint (load balancer or single API server IP)
# If not set, auto-detected from first api_server node
# Required for bench_server to connect
# api_server_endpoint = "10.0.1.30"

# Node definitions grouped by service type
# Each node entry has:
#   - ip: Node IP address (required)
#   - hostname: Optional hostname (defaults to IP)
#   - role: Service-specific role assignment
#   - bench_client_num: For bench_client/bench_server - client index

# Root State Store server
[[nodes.root_server]]
ip = "10.0.1.5"
hostname = "rss-leader"
role = "leader"

# Namespace Servers (requires 2 nodes: active + standby for NVMe journal replication)
[[nodes.nss_server]]
ip = "10.0.1.10"
hostname = "nss-active"
role = "active"

[[nodes.nss_server]]
ip = "10.0.1.11"
hostname = "nss-standby"
role = "standby"

# Blob Storage Servers (also run etcd cluster)
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
| `root_server` | Root State Store server (leader election, metadata coordination) | 1 |
| `nss_server` | Namespace Server (handles S3 API routing, metadata) | 2 (active + standby) |
| `bss_server` | Blob Storage Server (stores actual data, runs etcd) | 1-12 (recommend 6) |
| `api_server` | S3 API endpoint server | 1+ |
| `bench_server` | Benchmark coordinator (optional) | 0-1 |
| `bench_client` | Benchmark workers (optional) | 0+ |

### NSS Active/Standby Configuration

The NSS uses NVMe journal for metadata persistence. Two NSS nodes are required:
- **active**: Runs the primary NSS server and nss_role_agent
- **standby**: Runs mirrord for journal replication

The bootstrap process automatically generates a shared journal UUID for both nodes to coordinate replication.

### Minimal Cluster Example

A minimal cluster for testing (single BSS node):

```toml
[global]
num_bss_nodes = 1

[[nodes.root_server]]
ip = "10.0.1.5"
role = "leader"

[[nodes.nss_server]]
ip = "10.0.1.10"
role = "active"

[[nodes.nss_server]]
ip = "10.0.1.11"
role = "standby"

[[nodes.bss_server]]
ip = "10.0.1.20"

[[nodes.api_server]]
ip = "10.0.1.30"
```

### Cluster with Benchmarking

To enable benchmarking, add bench_client and bench_server nodes:

```toml
[global]
num_bss_nodes = 1
for_bench = true
num_bench_clients = 1

[[nodes.root_server]]
ip = "10.0.1.5"
role = "leader"

[[nodes.nss_server]]
ip = "10.0.1.10"
role = "active"

[[nodes.nss_server]]
ip = "10.0.1.11"
role = "standby"

[[nodes.bss_server]]
ip = "10.0.1.20"

[[nodes.api_server]]
ip = "10.0.1.30"

[[nodes.bench_client]]
ip = "10.0.1.40"
bench_client_num = 0

[[nodes.bench_server]]
ip = "10.0.1.41"
bench_client_num = 1
```

## Step 3: Create the Cluster

Run the cluster creation command:

```bash
cargo xtask deploy create-cluster \
  --config ./cluster.toml \
  --bootstrap-s3-url <RSS_IP>:8080
```

Replace `<RSS_IP>` with the IP address of the root_server running the bootstrap container.
If you have customized ssh config, you can add `--ssh-config my-ssh-config.conf`.

## Environment Variables on Nodes

The bootstrap process sets these environment variables for S3 access:

| Variable | Value | Description |
|----------|-------|-------------|
| `AWS_DEFAULT_REGION` | `on-prem` | Region for S3 API calls |
| `AWS_ENDPOINT_URL_S3` | `http://<rss-ip>:8080` | Bootstrap container S3 endpoint |
| `AWS_ACCESS_KEY_ID` | `test_api_key` | Bootstrap container credentials |
| `AWS_SECRET_ACCESS_KEY` | `test_api_secret` | Bootstrap container credentials |

## Verifying the Cluster

After cluster creation completes:

```bash
# Test S3 API (from deployment machine)
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret
export AWS_ENDPOINT_URL_S3=http://10.0.1.30
export AWS_DEFAULT_REGION=on-prem

aws s3 mb s3://test-bucket
aws s3 cp /etc/hosts s3://test-bucket/test-file
aws s3 ls s3://test-bucket/
```

## Running Benchmarks

If the cluster was created with `for_bench = true`:

```bash
# SSH to bench_server and run PUT 4K benchmark
ssh 10.0.1.41 '/opt/fractalbits/bin/bench_start.sh'

# Run GET 4K benchmark
ssh 10.0.1.41 'WORKLOAD=get_4k /opt/fractalbits/bin/bench_start.sh'
```

## Troubleshooting

### Check Bootstrap Logs

```bash
# On any node
ssh <node-ip> "tail /var/log/fractalbits-bootstrap.log"
```

### Check Service Logs

```bash
# Root Server logs
ssh <rss-ip> "journalctl -u rss"

# NSS logs (active node)
ssh <nss-active-ip> "journalctl -u nss"

# Mirrord logs (standby node)
ssh <nss-standby-ip> "journalctl -u mirrord"

# BSS logs
ssh <bss-ip> "journalctl -u bss"

# API Server logs
ssh <api-ip> "journalctl -u api_server"

# etcd logs
ssh <bss-ip> "journalctl -u etcd"
```

### Common Issues

1. **SSH connection refused**: Ensure SSH server is running and firewall allows port 22
2. **S3 access denied**: Check bootstrap container credentials and bucket permissions
3. **Service failed to start**: Check node has required dependencies (see Prerequisites)
4. **etcd cluster unhealthy**: Ensure all BSS nodes can reach each other on ports 2379/2380
5. **NSS journal format failed**: Ensure NVMe device is available and not mounted
6. **Mirrord not ready**: Check standby NSS can reach active NSS on port 9999
7. **Workflow stage timeout**: Check S3 connectivity from nodes to bootstrap container

## Firewall Rules

Ensure these ports are open between nodes:

| Port | Protocol | Service | Direction |
|------|----------|---------|-----------|
| 22 | TCP | SSH | Deployment -> All nodes |
| 80 | TCP | S3 API | Clients -> API Server |
| 2379 | TCP | etcd client | All nodes -> BSS nodes |
| 2380 | TCP | etcd peer | BSS <-> BSS |
| 8088 | TCP | Service RPC | RSS <-> NSS <-> BSS <-> API Server |
| 9999 | TCP | Mirrord | NSS active <-> NSS standby |
| 18088 | TCP | Management | Internal health checks |
