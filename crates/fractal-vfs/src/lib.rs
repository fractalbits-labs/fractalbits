//! The fractalbits storage-cluster VFS.
//!
//! [`vfs::VfsCore`] is the inode + cache + RPC backend that wire-protocol
//! adapters (FUSE in `fs_server`, NFSv4.x via nfs-ganesha, ...) sit on top
//! of.

pub mod backend;
pub mod cache;
pub mod config;
pub mod disk_cache;
pub mod error;
pub mod inode;
pub mod prefetch;
pub mod slice_mut;
pub mod vfs;
pub mod writeback;
