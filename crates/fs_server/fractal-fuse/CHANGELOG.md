# Changelog

All notable changes to `fractal-fuse` will be documented in this file.

## [Unreleased]

### Added
- **FUSE kernel notification API** (`notify.rs`): `FuseNotifier` for
  sending cache invalidation notifications to the kernel via
  `/dev/fuse` writes. Supports `inval_entry` (invalidate dentry),
  `inval_inode` (invalidate inode attrs + page cache), and `delete`
  (invalidate dentry + notify inotify watchers).
- ABI constants and structs for FUSE notifications:
  `FUSE_NOTIFY_INVAL_INODE`, `FUSE_NOTIFY_INVAL_ENTRY`,
  `FUSE_NOTIFY_DELETE`, and corresponding `fuse_notify_*_out` structs.

### Fixed
- FUSE notify wire format: the `error` field in `fuse_out_header`
  must be the **negative** notification type (e.g., `-3` for
  `FUSE_NOTIFY_INVAL_ENTRY`). Previously used positive values which
  would be silently rejected by the kernel.

## [0.3.0] - 2025-01-30

### Added
- FUSE_PASSTHROUGH support: `backing_id` field in `ReplyOpen` for
  kernel-level passthrough I/O on fully-cached files.
- Zero-copy FUSE read path with direct-to-payload I/O, eliminating
  an extra memcpy for read operations.

### Changed
- Renamed `Result` type alias to `FsResult` to avoid shadowing
  `std::result::Result`.
- Centralized dependency versions in workspace `Cargo.toml`.

## [0.2.0] - 2025-01-09

### Added
- Rustdoc comments across all public APIs.
- Improved README with architecture diagram, trait documentation,
  and usage examples.

### Changed
- Replaced git dependencies with crates.io-compatible workarounds
  for publishing.

## [0.1.0] - 2024-12-15

### Added
- Initial release: async FUSE library using `FUSE_OVER_IO_URING`
  (Linux 6.14+) and compio runtime.
- Per-CPU io_uring queue architecture with thread affinity.
- `Filesystem` trait with async methods for all FUSE operations.
- Unprivileged mounting via `fusermount3`.
- Builder-pattern `MountOptions` for mount configuration.
- FUSE protocol v7.45 ABI definitions.
