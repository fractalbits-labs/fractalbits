# FractalBits Architecture

## Overview

FractalBits uses a multi-tier architecture optimized for performance:

```
┌─────────────┐
│   S3 Client │
└──────┬──────┘
       │ HTTP(S)
       ▼
┌───────────────────────────────────┐
│┌──────────────────────────────────┴┐
││┌──────────────────────────────────┴┐
│││  API Server (Actix/Axum)          │  ← S3 API Frontend (N instances)
│││  - AWS SigV4 Auth                 │
│││  - Request Routing                │
│││  - Connection Pooling             │
│││  - Hybrid Storage Decision        │
└┤└──┬────┬─────────────┬───────┬────┬┘
 └───┼────┼─────────────┼───────┼────┘
     │    │             │       │
     │    │      ┌──────▼─────┐ │
     │    │      │┌───────────┴┐│       ← Coordination (HA pair)
     │    │      ││    RSS     ││
     │    │      ││   (Rust)   ││
     │    │      ││  - Leader  ││
     │    │      │└  Election ─┘│
     │    │      └─────────────┘│
     │    │                     │
     │    │    ┌────────────────┘
     │    │    │
     │  ┌─▼────▼───────────┐
     │  │┌─────────────────┴┐
     │  ││┌─────────────────┴┐          ← Metadata (N instances)
     │  │││     NSS          │           (Full path, infinitely Splittable)
     │  │││    (Zig)         │
     │  │││  - FractalART    │
     │  │││    Index         │
     │  └┤└────┬─────────────┘
     │   └─────┼─────────────┘
     │         │
     │         │  ┌──────────────────────────────────┐
     │         │  │  Hybrid Storage (based on size)  │
     │         │  └──────────────────────────────────┘
     │         │       │                      │
     │         │       │ < 1MB                │ >= 1MB
     │         │       ▼                      ▼
     └─────────┼──┐ ┌────────────┐    ┌──────────────┐
               │  │ │┌───────────┴┐   │   S3 Cloud   │
               │  │ ││┌───────────┴┐  │  (AWS S3)    │
               │  └─│││    BSS     │  │  - Durable   │
               └───►│││   (Zig)    │  │  - Scalable  │
                    │││  io_uring  │  └──────────────┘
                    └┤└────────────┘        ▲
                     └─────────────┘        │
                          ▲                 │
                          │ Data Plane      │ Data Plane
                          │ (Local NVMe)    │ (Cloud Storage)
```

## Components

### **NSS - Namespace Service Server** (Zig)
Metadata management with:
- Fractal ART based metadata engine, with efficient checkpoint and blob compaction
- Enhanced tree operations to support atomic directory and object renaming
- Physiological journaling for efficient crash recovery
- SIMD acceleration for Fractal ART metadata engine
- [LeanStore](https://www.cs.cit.tum.de/dis/research/leanstore/) inspired lightweight buffer manager for efficient memory management
- Lock coupling (crab-latching) for concurrent access

### **API Server** (Rust)
The S3-compatible HTTP frontend that handles:
- All S3 API requests (GET, PUT, DELETE, HEAD, POST)
- AWS Signature V4 authentication
- Per-core request handling with dedicated worker pools
- Connection pooling to backend services

### **BSS - Blob Storage Server** (Zig)
High-performance blob data storage engine:
- io_uring-based async I/O with IOPOLL mode for NVMe
- Direct I/O bypassing page cache
- Zero-copy data paths
- Configurable I/O concurrency and sharding

### **RSS - Root Service Server** (Rust)
Cluster coordination providing:
- Leader election using DynamoDB
- API key management
- Bucket management
- Volume group configuration

## Storage Architecture

### **Hybrid Storage Backend**
FractalBits uses a tiered storage approach to optimize performance and cost:

**Small Blobs (< 1MB):**
- Stored locally in BSS (Blob Storage Server)
- Ultra-low latency access via io_uring
- Direct NVMe storage with zero-copy paths

**Large Blobs (>= 1MB):**
- Automatically stored in S3 cloud storage
- Cost-effective for larger objects
- Leverages S3's durability and scalability
- Transparent to S3 API clients

**Benefits:**
- Hot path optimization: small objects get local NVMe performance
- Cost efficiency: large objects stored in cheaper S3 storage
- Seamless S3 API compatibility with automatic tiering
- No client-side changes required

## Technology Stack

**Languages:**
- **Zig**: Data plane (BSS, NSS) for maximum performance
- **Rust**: Control plane (API Server, RSS) for safety and productivity
- **TypeScript**: AWS CDK for infrastructure deployment

**Key Technologies:**
- **Fractal ART (Adaptive Radix Tree)**: Efficient metadata engine
- **io_uring**: Linux async I/O (custom library based on TigerBeetle's IO)
- **Actix/Axum**: Modern async web framework for API server
- **High Performance RPC Protocols**:
  - Data: Zero-copy RPC from network to disk, without serialization
  - Metadata: Fixed small size header + ProtoBuf Protocol

**Performance Optimizations:**
- Thread-per-core architecture (avoiding lock contention)
- SIMD speedup
- io_uring IOPOLL mode for NVMe SSDs
- Zero-copy authentication and data transfer
- Direct I/O bypassing page cache
- CPU affinity pinning
- Header batching for RPC efficiency
- Connection pooling and reuse
