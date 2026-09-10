# Changelog

All notable changes to `fractal-fuse` will be documented in this file.

## [Unreleased]

## [0.6.0] - 2026-07-09

### Changed
- **Breaking:** inode numbers and file handles are now distinct newtypes,
  `InodeId` and `FileHandleId`, replacing the transparent `u64` aliases.
  Every `Filesystem` trait method that took a `u64` inode or `fh` now
  takes the corresponding newtype, and the `fh` fields of `ReplyOpen`,
  `ReplyCreate`, and `SetAttr` are `FileHandleId`. The two cannot be
  swapped by accident. Implementations reach the raw value with `.get()`
  and wrap one with `InodeId::new` / `FileHandleId::new`.

## [0.5.0] - 2026-07-07

### Added
- `Session::fuse_fd()`: returns a cloned `Arc<OwnedFd>` of the kernel
  `/dev/fuse` fd backing the session. Use it to build a
  [`FuseNotifier`] (e.g. `FuseNotifier::from(session.fuse_fd())`)
  or perform raw FUSE-fd operations before calling `Session::run`.
- Filesystem callback panics are now caught and reported to the kernel
  as `EIO` instead of aborting the worker, so a single bad request no
  longer tears down the whole session. The panic message is logged.

### Changed
- **Breaking:** `Filesystem::init` no longer takes the
  `fuse_dev_fd: Arc<OwnedFd>` parameter introduced in 0.4.0. The init
  hook is now just `init(&self, req: Request) -> FsResult<ReplyInit>`.
  Filesystem implementations that need the fd should obtain it from
  `Session::fuse_fd()` after `Session::new` and thread it into the
  filesystem before calling `Session::run`. This keeps `Filesystem`
  agnostic of the transport fd and lets the notifier be constructed
  anywhere in user code, not only inside `init`.
- Upgraded to `compio` 0.12 and `io-uring` 0.7.12, dropping the
  hand-rolled ring/session workarounds now that upstream exposes the
  APIs they replaced.
- Notify struct sizes are now asserted at compile time alongside the
  existing io_uring header size guards.

### Fixed
- FUSE notify wire format: notification names are now NUL-terminated on
  the wire (the kernel reads `namelen + 1` bytes), and the notification
  code is written as a positive value in `fuse_out_header.error`,
  correcting the negative value used since 0.3.1. Names without a
  payload (e.g. `inval_inode`) are unaffected.

## [0.4.0] - 2026-05-19

### Added
- `SessionShutdownHandle`: a cloneable handle returned from
  `Session::shutdown_handle()` that lets external code trigger a
  clean shutdown of the FUSE loop and query whether shutdown has
  been requested. Re-exported from the crate root.
- `Session::with_worker_count(n)`: collapse the per-CPU worker
  topology onto `n` threads while still covering every kernel qid.
  One worker per CPU remains the default.
- `Filesystem` handlers for extended attributes (`getxattr`,
  `setxattr`, `listxattr`, `removexattr`) and file locking
  (`getlk`, `setlk` for POSIX advisory locks, `flock` for BSD
  flock-style locks). Unimplemented operations still default to
  `ENOSYS`.
- `Filesystem::init` and `Filesystem::destroy` are now actually
  driven by the session lifecycle: `init` runs after the FUSE_INIT
  handshake and before any worker dispatches a request, and `destroy`
  runs once during session teardown after all workers have stopped.
  Previously the trait methods existed but were not wired up.

### Changed
- **Breaking:** `Filesystem::init` now takes an additional
  `fuse_dev_fd: Arc<OwnedFd>` argument carrying a shared, owning
  handle to the `/dev/fuse` fd. Filesystem implementations that
  want to send kernel notifications hold on to this and turn it
  into a `FuseNotifier` via `.into()`.
- **Breaking:** `FuseNotifier::new(RawFd)` has been removed.
  Construct a notifier via `FuseNotifier::from(arc_fd)` /
  `arc_fd.into()` using the `Arc<OwnedFd>` handed to
  `Filesystem::init`. The notifier now shares ownership of the
  `/dev/fuse` fd through `Arc<OwnedFd>` instead of borrowing a
  raw fd, so the fd stays open for as long as any clone exists.
- The filesystem is mounted inside `Session::new` (previously the
  caller mounted separately and constructed the session from a raw
  fd). The `Session` value is now the single owner of the mount + fd.
- The negotiated FUSE `max_write` (driven by the filesystem's init
  reply) is now capped at 16 MiB inside the session transport to
  bound per-ring payload buffer allocation.
- Internal cancel/disconnect handling has been factored into a
  shared path so worker shutdown, external unmount, and explicit
  shutdown via `SessionShutdownHandle` all funnel through the same
  teardown logic.

### Fixed
- Sessions now shut down gracefully when the filesystem is
  externally unmounted (e.g. `fusermount3 -u` from another process)
  instead of hanging on the next ring submission.
- `Filesystem::destroy` is now actually invoked during session
  teardown -- previously it could be skipped on certain shutdown
  paths.
- Entry-task failures are now observed in completion order, so the
  first failing worker reports its error rather than being masked
  by later workers' shutdown notifications.

## [0.3.1] - 2026-04-08

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
