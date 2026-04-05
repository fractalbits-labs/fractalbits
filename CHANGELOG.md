# Changelog

All notable changes to the FractalBits project across all repositories
(main, core, crates/ha, crates/root_server).

## April 2026

**Summary:** April focused on **data reliability**. The major thrust was **BSS repair for erasure-coded volumes**, completing the scan-and-repair pipeline for both data and metadata blobs with EC support. This is critical for production durability guarantees: the system can now autonomously detect and reconstruct degraded EC shards, which is a prerequisite for offering strong durability SLAs. Version-aware tombstones and metadata delete guards in the Zig BSS core further harden data correctness during concurrent repair and normal I/O.

### BSS Repair & Erasure Coding
- feat(bss_repair): add EC erasure-coded scan and repair (ha: ffcbcb2)
- feat(bss_repair): add metadata volume scan and repair (ha: 5790253)
- test(bss_repair): add e2e tests for EC scan/repair support ([80976b2b])
- fix(bss_repair): handle tied shard sizes and prioritize data shards (ha: 8b63c71)
- fix(bss): set version=1 for data blob RPCs ([025fdfab])
- fix(bss_repair): add version param to delete_metadata_blob RPC ([59d36ce2])
- feat(bss_repair): add metadata types, RPC methods, and e2e tests ([131c9e8e])

### BSS Core (Zig)
- feat(bss): add skip_fence_token flag to BssHeader for repair bypass (core: 796abf6)
- fix(bss): version-aware tombstones and version-guarded metadata deletes (core: 9393a5f)
- chore(journal): add version to BufMeta (core: 99b8f8c)
- tool: add tool to dump meta blob from bss for easy debugging (core: b25a91d)

### Tooling & Cleanup
- chore(tool): rename fbs to nss_tool and bss_tool ([760a1f3b])
- chore(precheckin): update tests ([96602bc8])
- fix(precheckin): fix regression on fractal art tree tests ([36ef14f4])
- perf(bss_rpc): reduce per core connection from 8 to 2 ([ff9f0c58])
- cleanup(docs): update content related to our storage engine ([a1146cd0])

## March 2026

**Summary:** March was the most feature-dense month, with two transformative additions. **fs_server** landed as a full POSIX filesystem layer -- a FUSE/NFS server built on io_uring with erasure coding, NVMe disk caching, FUSE_PASSTHROUGH for zero-copy cached reads, writeback cache, and cross-instance cache invalidation via WatchChanges. This unlocks a native filesystem interface to FractalBits storage, enabling workloads (ML training, analytics, HPC) that cannot use S3 APIs, and the fractal-fuse crate was published to crates.io for broader adoption. The second major feature is the **new Fractal Art-based BSS storage engine** in Zig, replacing the old BSS with a chunk-allocator design backed by the shared buffer manager -- this unifies the NSS and BSS storage paths and sets the stage for better space efficiency and crash recovery. On the infrastructure side, **GCP deployment** was completed end-to-end (Terraform, Firestore backend, SSH transport, direct cloud storage bootstrap), making FractalBits truly multi-cloud. The bootstrap system was heavily refactored with compile-time stage correctness, dependency-based ordering, and provider-agnostic abstractions. Pre-signed URL support was also added to the S3 API.

### fs_server - FUSE & NFS Filesystem
- feat(fs_server): add FUSE over io_uring fs server with erasure coding ([0eeac0c4])
- feat(fs_server): add native NFSv3 server support ([4f44e645])
- feat(fs_server): add DiskCache for local NVMe block caching ([1d68650d])
- feat(fs_server): enable FUSE_PASSTHROUGH for fully-cached files ([046b4bd9])
- feat(fs_server): replace atime-based eviction with in-memory LRU tracker ([324f9e1c])
- feat(fs_server): cross-instance FUSE cache invalidation via NSS WatchChanges ([b02c818f])
- feat(fs_server): zero-copy FUSE read path with direct-to-payload I/O ([fa8da8d0])
- feat(fs_server): enable FUSE writeback cache ([0647cee6])
- feat(fs_server): add READDIR pagination and COMMIT support ([7f478485])
- feat(fractal-fuse): add backing_id field to ReplyOpen ([be829afc])
- refactor(fs_server): async DiskCache and use compio_macros::test ([d50aac3f])
- chore(fractal-fuse): prepare crate for crates.io publishing ([bc0eeb88])

### BSS New Storage Engine (Zig)
- feat(bss_server): art based bss server with test (core: 9bb433f)
- feat(bss): use BlobMeta to store information needed (core: dfd4563)
- feat(bss_server): add chunk allocator and simulation (core: a488be6)
- feat(bss): rename new_bss_server to bss_server, old to old_bss_server (core: 652d854)
- feat(bss): add list blobs rpc for bss (core: f661bb4)
- feat(bss): add FLAG_STORAGE_SIZE env var override (core: 8de2537)
- feat(bss): add valued based check for bss art insert (core: 9847204)
- feat(bss): enable format_zero for bss (core: 4931c4f)
- feat(bss): add is_deleted support to ListBlobs RPC (core: 24adf02)
- refactor(bss): reduce BlobBuffer reserved_size from 1MB to 128KB (core: b7911d7)
- refactor(bss): move buffer_manager to shared lib between nss and bss (core: bb6248d)
- refactor(art): extract common fractal art related zig files to common (core: e0b15d4, 52846e5, 41114e3)

### BSS Repair (Rust)
- feat(bss_repair): add initial bss_repair scan crate (ha: 68f89ea)
- feat(bss_repair): complete replicated data scan and repair (ha: 05a4a98)
- feat(bss): Add Rust support for BSS ListBlobs RPC ([03456b0a])
- refactor(bss): sync Rust MessageHeader with Zig BssHeader layout ([40d2dc8f])

### GCP Deployment
- feat(deploy): add GCP VPC commands, SSH transport, and Firestore emulator local dev support ([8feacdd0])
- feat(infra): add Terraform infrastructure for GCP deployment ([41cc36df])
- feat(rss): add Firestore backend for GCP deployment (root_server: 9536880)
- feat(deploy): add GCP support to describe-stack command ([8d824117])
- refactor(deploy): reorganize cmd_deploy into aws/ and gcp/ submodules ([c87a9aed])
- refactor(deploy): eliminate Docker and SSM/SSH from bootstrap, all nodes download directly from cloud storage ([19b55a6b])

### Bootstrap & Deploy Refactoring
- refactor(bootstrap): enforce global/per-node stage correctness at compile time ([e03f60c3])
- refactor(bootstrap): replace etcd-specific workflow code with generic stage mechanism ([94121183])
- refactor(bootstrap): replace hardcoded stage sequence numbers with dependency declarations ([be81fc70])
- refactor(bootstrap): extract deploy-target code into aws/gcp/etcd modules ([cd1a994b])
- feat(deploy): pre-deploy bootstrap config with per-instance role args ([e7e4374b])
- feat(bootstrap): reduce fs journal commits via lazytime mount option ([3332000c])
- feat(bss): update bootstrap for new BSS server with format and serve command ([54cc5535])
- feat(bss): remove hardcoded storage size overrides, preserve data on restart ([e87a9803])

### API Server & S3
- feat(api_server, fractal-s3): add pre-signed URL support ([61578f92])
- fix(api): properly clean up parts and inode on abort_multipart_upload ([1f406299])
- fix(api): improve delete handling and NSS RPC retry logic ([5375559e])
- fix(rss): clean up orphaned MPU part inodes during bucket deletion (root_server: 26f08b8)

### NSS Server (Zig)
- feat(nss_server): add WatchChanges RPC and change log ring buffer (core: 93ce5a0)
- feat(s3_integrity_test_rs): port S3 integrity test from Python to Rust (core: cd5aa4e)
- security(assert): strip source file paths from release binaries (core: 43255df)

### Testing & CI
- feat: add coverage instrumentation support for service binaries ([2ddb1c12])
- feat: include data_blob_resync_server and fs-server in run-tests all ([d2be160b])
- fix(tests): multi-AZ service setup and test reliability improvements ([3b5e7871])
- test(bss_repair): add bss_repair e2e suite ([fe0fe4d8])
- refactor(file_ops): extract file_ops crate and consolidate NSS response parsing ([146483a0])
- refactor(build): isolate fs_server compio-runtime from workspace feature unification ([2e820f6c])

## February 2026

**Summary:** February laid the groundwork for two pillars of production readiness: **erasure coding** and **EBS-based HA failover**. Erasure-coded data volumes were implemented in DataVgProxy with per-volume mode configuration, enabling FractalBits to tolerate multiple disk/node failures with far less storage overhead than full replication -- this is essential for cost-effective large-scale deployments. The **EBS HA with NVMe reservations** work replaced the previous AWS EC2 API-based failover mechanism with kernel-level NVMe reservation fencing, delivering sub-second failover times and eliminating the dependency on AWS control-plane availability during failures. The observer state machine in the root server gained an EBS-specific path, and the nss_role_agent was extended for active/standby EBS modes. Early FUSE filesystem prototyping also began this month (read-only client, then write/delete/rename), which would mature into the full fs_server in March.

### Erasure Coding
- feat(ec): implement erasure-coded data volumes in DataVgProxy ([df03e4cb])
- refactor(ec): unify volume types with per-volume mode config ([4a98237d])
- test(ec): add EC degraded read/write test for BSS node failures ([8316552a])

### FUSE Filesystem (Early Work)
- feat(fuse): add read-only FUSE filesystem client ([985d306c])
- feat(fuse): add write, delete, and rename support ([77d469d8])
- perf(fuse): eliminate unnecessary data copies in read and write paths ([d8a73c45])
- refactor: consolidate ObjectLayout and ChecksumValue into data_types ([f36d05de])

### EBS HA & NVMe Reservations
- feat(infra): add NVMe reservation node sequence ID mapping ([e2cb5230])
- feat(ha): replace xxhash NVMe reservation keys with RSS node sequence IDs (ha: 2c93340)
- feat(infra): add single-AZ EBS multi-attach for NVMe reservation failover ([d408267e])
- feat(infra): add EBS HA active/standby support ([754e470d])
- feat(nss_role_agent): replace AWS EC2 API failover with NVMe reservations (ha: 10dcaa7)
- feat(nss_role_agent): add EBS HA failover support (ha: e9cd182)
- feat(nss_role_agent): add EBS standby mode support (ha: f8e0a53)
- feat(rss): return nss_node_id and peer_nss_node_id in GetNssRole RPC (root_server: 76ba052)
- feat(observer): add EBS state machine path (root_server: 8ffedf2)

### NSS/BSS Core (Zig)
- feat(nss): make mirrord_host optional for EBS HA mode (core: 5de3596)
- feat(nss_failover_test): add EBS active/standby mode support (core: e5816ee)

### Bootstrap & Deploy
- refactor(infra): extract workflow stages into shared xtask_common module ([5c08d48e])
- refactor(infra): replace shared stage definitions with S3 stage blueprint ([3b077052])
- feat(infra): use head-object instead of s3 ls in wait_for_global ([12070ee2])
- feat(infra): always synthesize all stacks so cdk destroy --all works ([d72516ef])
- feat(infra): fix meta-stack due to bootstrap workflow changes ([12a1ee40])

### Testing
- feat(xtask): add journal type selection for nightly tests ([178c3917])
- feat(xtask): add EBS HA failover test ([96e3d421])
- feat(xtask): enable nss_role_agent_b for EBS active/standby ([7821a282])
- refactor(xtask): nss ha failover tests renaming ([3262435f])

## January 2026

**Summary:** January established the **HA failover architecture** that all subsequent reliability work builds on. The push-based health model replaced polling-based heartbeats -- the nss_role_agent now actively reports health to the root server's observer state machine, which drives failover decisions. This is a fundamental shift that reduces detection latency and eliminates false-positive failovers from network partitions. **Fence tokens** were introduced between NSS and BSS to prevent split-brain writes during failover, and the **blob merge mechanism** in the Fractal Art tree (empty leaf merges, checkpointer integration) improves space reclamation after deletes. The **Docker containerization** effort matured with multi-arch builds, precheckin Docker test mode, and nss_role_agent integration, making local development and CI far more portable. ARM64 CI support and the fractal-s3 CLI tool (an AWS CLI replacement) were also delivered. On the Zig side, several critical bug fixes landed for BSS body corruption, NoSpaceInBlob panics, and undefined memory access.

### HA Failover & Observer State Machine
- feat(ha): add NSS HA failover tests and push-based health model ([50f5485a])
- feat(root_server): integrate observer state machine with push-based health (root_server: 28a68a3)
- feat(rss): add GetActiveNssAddress RPC and push-based health model (root_server: 62538da)
- feat(nss_role_agent): implement push-based health reporting (ha: 0f6b06e)
- feat(nss_role_agent): add network_address reporting to health updates (ha: 7c0dbe5)
- feat(observer): add DynamoDB backend support for observer state storage (root_server: 6c84185)
- feat(api_server): add automatic NSS address refresh on connection failure ([efcbad99])
- feat(api_server): dynamic NSS client initialization via RSS ([178b7433])

### Fence Token & BSS RPC
- feat(fence_token): add nss-bss handshake for fence token exchange (core: 631b5ba)
- feat(fence): add fence token handling (core: be786e2)
- feat(bss rpc): adjust bss rpc message header ([fbd62d12])
- feat(merge): make empty leaf blob merge working (core: d7ab646)
- feat(merge): add merge handling point in checkpointer (core: c7efad5)

### Docker & Container
- feat(xtask): add Docker test mode to precheckin command ([91705a28])
- refactor(xtask): optimize docker build to compile only necessary binaries ([de21f86e])
- perf(container): optimize Docker container startup time ([df2be75c])
- feat(docker): integrate nss_role_agent into Docker container ([714c476c])
- refactor(docker): unify Docker build for multi-arch support ([48747521])

### Deployment & CI
- feat(ci): add ARM64 support using matrix strategy ([613b948b])
- feat(deploy): add simulate-on-prem mode for testing on-prem bootstrap ([99b75ea9])
- feat(deploy): add CPU-specific binary build and deployment for AWS env ([e446ca2a])
- feat(deploy): auto-upload binaries in create-vpc and create-cluster ([cd0a8dd4])
- feat(journal): implement journal_uuid for UUID-based log device mounting ([f8f5ab1f])
- feat(rss): return journal_uuid in GetNssRole response (root_server: f62a0ca)
- feat(journal): add journal_uuid support (core: cc53392)

### Tooling & Build
- feat(fractal-s3): add CLI tool as AWS S3 CLI replacement ([55de5493])
- feat(xtask): add coredump support for nightly ([be6276ea])
- feat(xtask): add repo manifest subcommand ([ceac5ebd])
- feat(service): add LOGS env file support with timestamps ([defaa50f])
- perf(cargo): change lto from "thin" to "fat" ([fb3263c5])
- refactor(build): require system protoc instead of auto-download ([8bcc549a])

### Bug Fixes (Zig)
- fix(bss): fix body corruption on consecutive partial sends (core: 548101e)
- fix(nss): prevent NoSpaceInBlob panic during merge of empty non-leaf blobs (core: 4837153)
- fix(merge): fix parent_bf NULL case for evictBf and mergeBf (core: 8bdd753)
- fix(bss): make recv_buf optional to prevent use of undefined memory (core: 3738a20)
- fix(mirrord): set body checksum for mirrord server (core: d3827a1)

### Root Server Refactoring
- refactor(root_server): move rpc_server and observer code into modules (root_server: 1fec7ed)
- refactor(leader_election): reorganize into module directory structure (root_server: 7797bd8)
- refactor(observer): remove config.observer.enabled and always run observer (root_server: 307d3c6)

## Pre-2026 (up to December 2025)

**Summary:** The pre-2026 period covers the project's foundational build-out from mid-2025. Key milestones include: the **Multi-BSS / Multi-AZ architecture** (Aug-Sep 2025), which introduced quorum-based data operations via MetadataVgProxy and DynamoDB-backed leader election in the root server -- this was the architectural leap from single-node to distributed storage. **RPC security hardening** (Oct-Nov 2025) added XXH3-64 checksums across all BSS and NSS protocols, circuit breakers, and zero-copy authentication in the API server, addressing data integrity and performance simultaneously. The **Fractal Art merge transactions** (Oct 2025) enabled efficient rename operations and space reclamation in the tree structure. In December 2025, **on-premises deployment** was added with etcd-based service discovery, Docker support, and S3-based cluster bootstrap, removing the hard dependency on AWS and opening the door to private cloud and edge deployments. The project was open-sourced under Apache 2.0 in December 2025.

### On-Premises & Etcd Support (Dec 2025)
- feat(bootstrap): add on-prem support with etcd service discovery ([f8bda384])
- feat(deploy): add SSM bootstrap and on-prem cluster creation ([8a02ab13])
- feat(rss): add etcd backend support with runtime selection (root_server: 3c51ac4)
- feat(deploy): implement ASG-based BSS with dynamic etcd cluster ([92b1cee1])
- feat: add Docker support ([e31d0be9])
- feat(deploy): add single-AZ NVMe journal support with active/standby mode ([b4137e5d])

### NSS Mirroring & Failover (Dec 2025)
- feat(mirrord): add mirrord test back with structure refactoring (core: d668e71)
- chore(failover): add failover engine to test nss/mirrord failover (root_server: edefb54)
- feat(bootstrap): implement S3-based workflow for cluster bootstrap ([e463fcd6])

### Performance & RPC (Nov 2025)
- perf(api_server): implement zero-copy authentication with streaming signature verification ([d5077905])
- perf(api_server): add thread-local cache for signing key computation ([694dd3df])
- feat(rpc): migrate to XXH3-64 and add security checksum verification (core: 6d4c304)
- feat(rpc): implement XXH3 checksums for BSS protocols (core: 63c4eff)
- feat(rpc): add header checksum verification in NSS protocol (core: 567ce15)
- feat(rpc): add circuit breaker and connection timeout for BSS nodes ([a8103b46])
- perf(bss_server): optimize message body recv with non-blocking fast path (core: 42b6a27)
- perf(bss): optimize header checksum computing and memset (core: 2a89714)

### Art Merge Support (Oct 2025)
- feat(art merge): add merge txn type and support in memory (core: c8f84e7, b207b2d)
- feat(art): support merge in rename case with txn support (core: 3abcb68)
- feat(replay): construct txn dependency graph for replay (core: 8607227)

### Multi-BSS & Multi-AZ (Aug-Sep 2025)
- feat(multi-bss): implement Phase 3 MetadataVgProxy (ha: 744cce6)
- feat(multi-bss): Phase 1 metadata blob protocol & infrastructure (root_server: 0ff730b)
- feat(rss): replace hard-coded BSS config with DynamoDB JSON storage (root_server: 735d7b3)
- feat(root_server): implement DDB-based leader election (root_server: ee22b88)
- refactor(rpc): complete RPC client architecture overhaul (root_server: 722b6c5)
- feat(bucket apis): move create_bucket and delete_bucket logic to rss (root_server: 31b1f9f)

### Infrastructure & Deployment (Nov 2025)
- feat(deploy): update template-based VPC configuration ([5135ecd0])
- feat(xtask): add configurable parameters and templates to create-vpc ([5fe8974a])
- feat(api_server): add configurable worker threads and thread affinity ([1a68968e])
- feat(api_server): include EC2 instance ID in S3 error responses ([d0586e81])

### Project Foundation (Jul-Aug 2025)
- chore: add Apache 2.0 license ([9feef39d])
- feat: add README file and other dev related docs ([753806fa])
- feat(ci): add basic ci workflow ([9f924308])
- refactor: project directories re-org (ha: 420247c, root_server: 8f020ac)

[80976b2b]: https://github.com/user/fractalbits/commit/80976b2b
[025fdfab]: https://github.com/user/fractalbits/commit/025fdfab
[59d36ce2]: https://github.com/user/fractalbits/commit/59d36ce2
[131c9e8e]: https://github.com/user/fractalbits/commit/131c9e8e
[760a1f3b]: https://github.com/user/fractalbits/commit/760a1f3b
[96602bc8]: https://github.com/user/fractalbits/commit/96602bc8
[36ef14f4]: https://github.com/user/fractalbits/commit/36ef14f4
[ff9f0c58]: https://github.com/user/fractalbits/commit/ff9f0c58
[a1146cd0]: https://github.com/user/fractalbits/commit/a1146cd0
[0f9d67e2]: https://github.com/user/fractalbits/commit/0f9d67e2
[da81fec0]: https://github.com/user/fractalbits/commit/da81fec0
[0eeac0c4]: https://github.com/user/fractalbits/commit/0eeac0c4
[4f44e645]: https://github.com/user/fractalbits/commit/4f44e645
[1d68650d]: https://github.com/user/fractalbits/commit/1d68650d
[046b4bd9]: https://github.com/user/fractalbits/commit/046b4bd9
[324f9e1c]: https://github.com/user/fractalbits/commit/324f9e1c
[b02c818f]: https://github.com/user/fractalbits/commit/b02c818f
[fa8da8d0]: https://github.com/user/fractalbits/commit/fa8da8d0
[0647cee6]: https://github.com/user/fractalbits/commit/0647cee6
[7f478485]: https://github.com/user/fractalbits/commit/7f478485
[be829afc]: https://github.com/user/fractalbits/commit/be829afc
[d50aac3f]: https://github.com/user/fractalbits/commit/d50aac3f
[bc0eeb88]: https://github.com/user/fractalbits/commit/bc0eeb88
[8feacdd0]: https://github.com/user/fractalbits/commit/8feacdd0
[41cc36df]: https://github.com/user/fractalbits/commit/41cc36df
[8d824117]: https://github.com/user/fractalbits/commit/8d824117
[c87a9aed]: https://github.com/user/fractalbits/commit/c87a9aed
[19b55a6b]: https://github.com/user/fractalbits/commit/19b55a6b
[e03f60c3]: https://github.com/user/fractalbits/commit/e03f60c3
[94121183]: https://github.com/user/fractalbits/commit/94121183
[be81fc70]: https://github.com/user/fractalbits/commit/be81fc70
[cd1a994b]: https://github.com/user/fractalbits/commit/cd1a994b
[e7e4374b]: https://github.com/user/fractalbits/commit/e7e4374b
[3332000c]: https://github.com/user/fractalbits/commit/3332000c
[54cc5535]: https://github.com/user/fractalbits/commit/54cc5535
[e87a9803]: https://github.com/user/fractalbits/commit/e87a9803
[61578f92]: https://github.com/user/fractalbits/commit/61578f92
[1f406299]: https://github.com/user/fractalbits/commit/1f406299
[5375559e]: https://github.com/user/fractalbits/commit/5375559e
[2ddb1c12]: https://github.com/user/fractalbits/commit/2ddb1c12
[d2be160b]: https://github.com/user/fractalbits/commit/d2be160b
[3b5e7871]: https://github.com/user/fractalbits/commit/3b5e7871
[fe0fe4d8]: https://github.com/user/fractalbits/commit/fe0fe4d8
[146483a0]: https://github.com/user/fractalbits/commit/146483a0
[2e820f6c]: https://github.com/user/fractalbits/commit/2e820f6c
[df03e4cb]: https://github.com/user/fractalbits/commit/df03e4cb
[4a98237d]: https://github.com/user/fractalbits/commit/4a98237d
[8316552a]: https://github.com/user/fractalbits/commit/8316552a
[985d306c]: https://github.com/user/fractalbits/commit/985d306c
[77d469d8]: https://github.com/user/fractalbits/commit/77d469d8
[d8a73c45]: https://github.com/user/fractalbits/commit/d8a73c45
[f36d05de]: https://github.com/user/fractalbits/commit/f36d05de
[e2cb5230]: https://github.com/user/fractalbits/commit/e2cb5230
[d408267e]: https://github.com/user/fractalbits/commit/d408267e
[754e470d]: https://github.com/user/fractalbits/commit/754e470d
[5c08d48e]: https://github.com/user/fractalbits/commit/5c08d48e
[3b077052]: https://github.com/user/fractalbits/commit/3b077052
[12070ee2]: https://github.com/user/fractalbits/commit/12070ee2
[d72516ef]: https://github.com/user/fractalbits/commit/d72516ef
[12a1ee40]: https://github.com/user/fractalbits/commit/12a1ee40
[178c3917]: https://github.com/user/fractalbits/commit/178c3917
[96e3d421]: https://github.com/user/fractalbits/commit/96e3d421
[7821a282]: https://github.com/user/fractalbits/commit/7821a282
[3262435f]: https://github.com/user/fractalbits/commit/3262435f
[50f5485a]: https://github.com/user/fractalbits/commit/50f5485a
[efcbad99]: https://github.com/user/fractalbits/commit/efcbad99
[178b7433]: https://github.com/user/fractalbits/commit/178b7433
[fbd62d12]: https://github.com/user/fractalbits/commit/fbd62d12
[91705a28]: https://github.com/user/fractalbits/commit/91705a28
[de21f86e]: https://github.com/user/fractalbits/commit/de21f86e
[df2be75c]: https://github.com/user/fractalbits/commit/df2be75c
[714c476c]: https://github.com/user/fractalbits/commit/714c476c
[48747521]: https://github.com/user/fractalbits/commit/48747521
[613b948b]: https://github.com/user/fractalbits/commit/613b948b
[99b75ea9]: https://github.com/user/fractalbits/commit/99b75ea9
[e446ca2a]: https://github.com/user/fractalbits/commit/e446ca2a
[cd0a8dd4]: https://github.com/user/fractalbits/commit/cd0a8dd4
[f8f5ab1f]: https://github.com/user/fractalbits/commit/f8f5ab1f
[55de5493]: https://github.com/user/fractalbits/commit/55de5493
[be6276ea]: https://github.com/user/fractalbits/commit/be6276ea
[ceac5ebd]: https://github.com/user/fractalbits/commit/ceac5ebd
[defaa50f]: https://github.com/user/fractalbits/commit/defaa50f
[fb3263c5]: https://github.com/user/fractalbits/commit/fb3263c5
[8bcc549a]: https://github.com/user/fractalbits/commit/8bcc549a
[f8bda384]: https://github.com/user/fractalbits/commit/f8bda384
[8a02ab13]: https://github.com/user/fractalbits/commit/8a02ab13
[92b1cee1]: https://github.com/user/fractalbits/commit/92b1cee1
[e31d0be9]: https://github.com/user/fractalbits/commit/e31d0be9
[b4137e5d]: https://github.com/user/fractalbits/commit/b4137e5d
[e463fcd6]: https://github.com/user/fractalbits/commit/e463fcd6
[d5077905]: https://github.com/user/fractalbits/commit/d5077905
[694dd3df]: https://github.com/user/fractalbits/commit/694dd3df
[a8103b46]: https://github.com/user/fractalbits/commit/a8103b46
[5135ecd0]: https://github.com/user/fractalbits/commit/5135ecd0
[5fe8974a]: https://github.com/user/fractalbits/commit/5fe8974a
[1a68968e]: https://github.com/user/fractalbits/commit/1a68968e
[d0586e81]: https://github.com/user/fractalbits/commit/d0586e81
[9feef39d]: https://github.com/user/fractalbits/commit/9feef39d
[753806fa]: https://github.com/user/fractalbits/commit/753806fa
[9f924308]: https://github.com/user/fractalbits/commit/9f924308
[03456b0a]: https://github.com/user/fractalbits/commit/03456b0a
[40d2dc8f]: https://github.com/user/fractalbits/commit/40d2dc8f
