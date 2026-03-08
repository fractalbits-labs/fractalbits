# fractal-fuse

[![CI](https://github.com/fractalbits-labs/fractalbits-main/actions/workflows/ci.yml/badge.svg)](https://github.com/fractalbits-labs/fractalbits-main/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/fractal-fuse.svg)](https://crates.io/crates/fractal-fuse)
[![docs.rs](https://docs.rs/fractal-fuse/badge.svg)](https://docs.rs/fractal-fuse)
[![source](https://img.shields.io/badge/source-GitHub-blue)](https://github.com/fractalbits-labs/fractalbits-main/tree/main/crates/fs_server/fractal-fuse)

An async FUSE (Filesystem in Userspace) library for Linux, built on
**io_uring** and the **compio** async runtime. It uses the
`FUSE_OVER_IO_URING` kernel interface (Linux 6.14+) for high-performance
userspace filesystem I/O with zero-copy buffer registration.

## Features

- **io_uring-native FUSE transport** -- Uses `FUSE_IO_URING_CMD_REGISTER` and
  `FUSE_IO_URING_CMD_COMMIT_AND_FETCH` for zero-copy request/response cycles
- **Per-CPU queue architecture** -- Spawns one io_uring ring per CPU core with
  thread affinity for cache-friendly processing
- **Async filesystem trait** -- Implement the `Filesystem` trait with async
  methods; unimplemented operations default to `ENOSYS`
- **Unprivileged mounting** -- Uses `fusermount3` for non-root mounts
- **Configurable mount options** -- Builder-pattern `MountOptions` for
  `allow_other`, `default_permissions`, `writeback_cache`, and more
- **FUSE protocol v7.45** -- Full ABI definitions with support for
  `readdirplus`, `fallocate`, `lseek`, `copy_file_range`, and other modern
  operations

## Requirements

- **Linux 6.14+** with `FUSE_OVER_IO_URING` support enabled
- **fusermount3** installed and accessible in `$PATH`
- **Rust edition 2024** (nightly or Rust 1.85+)

## Usage

Implement the `Filesystem` trait and run a session:

```rust,no_run
use std::path::Path;
use fractal_fuse::{Filesystem, MountOptions, Session};

struct MyFs;

impl Filesystem for MyFs {
    // Implement the operations your filesystem supports.
    // All methods default to returning ENOSYS.
}

fn main() -> std::io::Result<()> {
    let opts = MountOptions::new()
        .fs_name("myfs")
        .allow_other(true)
        .default_permissions(true);

    Session::new(opts)
        .queue_depth(128)
        .run(MyFs, Path::new("/mnt/myfs"))
}
```

## Architecture

```text
Session::run()
  |
  +-- fusermount3 (mount, receive /dev/fuse fd)
  +-- FUSE_INIT handshake (blocking read/write on /dev/fuse)
  +-- Per-CPU ring threads (each with compio Runtime + thread affinity)
        |
        +-- RingEntry buffers (page-aligned, mmap'd)
        +-- FuseRegister (register buffers with kernel)
        +-- Loop: dispatch request -> FuseCommitAndFetch (respond + fetch next)
```

Each ring thread runs a compio single-threaded runtime pinned to a CPU core.
Ring entries use page-aligned `mmap` buffers for the header (288 bytes) and
payload (up to `max_write` bytes, default 1MB). The kernel fills request data
directly into these buffers, and responses are written back in-place.

## Filesystem Trait

Implement the
[`Filesystem`](https://docs.rs/fractal-fuse/latest/fractal_fuse/filesystem/trait.Filesystem.html)
trait to handle FUSE operations. All methods are async (`!Send`, matching
compio's single-threaded model) and default to returning `ENOSYS`. The trait
itself is `Send + Sync` for sharing via `Arc` across ring threads.

Supported operations follow the low-level
[FUSE API](https://libfuse.github.io/doxygen/structfuse__lowlevel__ops.html):

### Filesystem

| Operation | Description |
|-----------|-------------|
| `init` | Initialize filesystem (mount) |
| `destroy` | Clean up filesystem (unmount) |
| `statfs` | Get filesystem statistics |

### Files

| Operation | Description |
|-----------|-------------|
| `lookup` | Look up a directory entry by name |
| `forget` / `batch_forget` | Release inode reference(s) |
| `getattr` / `setattr` | Get / set file attributes |
| `access` | Check file access permissions |
| `open` / `release` | Open / close a file |
| `create` | Create and open a file |
| `read` / `write` | Read / write data |
| `flush` | Flush cached data |
| `fsync` | Synchronize file contents |
| `fallocate` | Allocate or deallocate file space |
| `lseek` | Find next data or hole |
| `copy_file_range` | Copy a range of data between files |
| `mknod` | Create a file node |
| `unlink` | Remove a file |
| `link` | Create a hard link |
| `symlink` / `readlink` | Create / read a symbolic link |
| `rename` | Rename a file or directory |

### Directories

| Operation | Description |
|-----------|-------------|
| `mkdir` / `rmdir` | Create / remove a directory |
| `opendir` / `releasedir` | Open / close a directory |
| `readdir` / `readdirplus` | Read directory entries (with optional attributes) |
| `fsyncdir` | Synchronize directory contents |

See the [trait documentation](https://docs.rs/fractal-fuse/latest/fractal_fuse/filesystem/trait.Filesystem.html)
for method signatures and semantics.

## License

Licensed under [Apache License, Version 2.0](LICENSE).
