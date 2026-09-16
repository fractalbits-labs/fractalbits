//! The fractalbits client-side VFS.
//!
//! [`vfs::VfsCore`] is the inode table, write buffers and writeback queue
//! that wire-protocol adapters (FUSE in `artfs-mount`, NFSv4.x via
//! nfs-ganesha, ...) sit on top of. Its storage backend is the
//! `fs_gateway` protocol, addressed by inode key and logical block; the
//! gateway owns blob identity, generations, the row map, the cluster
//! clients, the disk cache, prefetch and reclamation.

pub mod backend;
pub mod cache;
pub mod config;
pub mod error;
pub mod inode;
pub mod prefetch;
pub mod vfs;
pub mod writeback;
