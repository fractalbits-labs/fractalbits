use std::ffi::OsString;
use std::io::{self, IoSliceMut};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;
use std::process::Command;

use nix::sys::socket::{self, AddressFamily, ControlMessageOwned, MsgFlags, SockFlag, SockType};
use tracing::debug;

fn find_fusermount3() -> io::Result<std::path::PathBuf> {
    which::which("fusermount3")
        .map_err(|err| io::Error::other(format!("find fusermount3 binary failed {err:?}")))
}

/// Mount a FUSE filesystem using fusermount3 (unprivileged mount).
/// Returns the /dev/fuse file descriptor.
pub fn fusermount(mount_options: &MountOptions, mount_path: &Path) -> io::Result<OwnedFd> {
    let (sock0, sock1) = socket::socketpair(
        AddressFamily::Unix,
        SockType::SeqPacket,
        None,
        SockFlag::empty(),
    )?;

    let binary_path = find_fusermount3()?;

    let options = build_mount_options(mount_options);
    debug!("mount options {:?}", options);

    let fd0 = sock0.as_raw_fd();

    let status = Command::new(binary_path)
        .env("_FUSE_COMMFD", fd0.to_string())
        .args([OsString::from("-o"), options, mount_path.into()])
        .status()?;

    if !status.success() {
        return Err(io::Error::other("fusermount3 failed"));
    }

    let fd1 = sock1.as_raw_fd();
    let fuse_fd = receive_fuse_fd(fd1)?;

    Ok(unsafe { OwnedFd::from_raw_fd(fuse_fd) })
}

/// Unmount a FUSE filesystem.
pub fn fusermount_unmount(mount_path: &Path) -> io::Result<()> {
    let binary_path = find_fusermount3()?;

    let status = Command::new(binary_path)
        .arg("-u")
        .arg(mount_path.as_os_str())
        .status()?;

    if !status.success() {
        return Err(io::Error::other("fusermount3 -u failed"));
    }

    Ok(())
}

fn receive_fuse_fd(sock_fd: RawFd) -> io::Result<RawFd> {
    let mut buf = vec![];
    let mut cmsg_buf = nix::cmsg_space!([RawFd; 1]);
    let mut bufs = [IoSliceMut::new(&mut buf)];

    let msg = socket::recvmsg::<()>(sock_fd, &mut bufs, Some(&mut cmsg_buf), MsgFlags::empty())
        .map_err(io::Error::from)?;

    if let Some(ControlMessageOwned::ScmRights(fds)) = msg.cmsgs().map_err(io::Error::from)?.next()
    {
        if fds.is_empty() {
            return Err(io::Error::other("no fuse fd received"));
        }
        Ok(fds[0])
    } else {
        Err(io::Error::other("failed to receive fuse fd"))
    }
}

fn build_mount_options(opts: &MountOptions) -> OsString {
    let mut parts = vec![
        format!(
            "user_id={}",
            opts.uid.unwrap_or_else(|| unsafe { libc::getuid() })
        ),
        format!(
            "group_id={}",
            opts.gid.unwrap_or_else(|| unsafe { libc::getgid() })
        ),
        format!("rootmode={}", opts.rootmode.unwrap_or(40000)),
        format!("fsname={}", opts.fs_name.as_deref().unwrap_or("fuse")),
    ];

    if opts.allow_root {
        parts.push("allow_root".to_string());
    }
    if opts.allow_other {
        parts.push("allow_other".to_string());
    }
    if opts.read_only {
        parts.push("ro".to_string());
    }
    if opts.default_permissions {
        parts.push("default_permissions".to_string());
    }

    let mut options = OsString::from(parts.join(","));

    if let Some(custom) = &opts.custom_options {
        options.push(",");
        options.push(custom);
    }

    options
}

/// Mount options for a FUSE filesystem.
#[derive(Debug, Clone, Default)]
pub struct MountOptions {
    pub allow_other: bool,
    pub allow_root: bool,
    pub default_permissions: bool,
    pub read_only: bool,
    pub fs_name: Option<String>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub rootmode: Option<u32>,
    pub custom_options: Option<OsString>,
    pub dont_mask: bool,
    pub no_open_support: bool,
    pub no_open_dir_support: bool,
    pub handle_killpriv: bool,
    pub write_back: bool,
    pub force_readdir_plus: bool,
    pub passthrough: bool,
    /// Advertise FUSE_POSIX_LOCKS so the kernel routes fcntl(F_SETLK)
    /// requests to the userspace `getlk`/`setlk` handlers. Leave off
    /// unless the [`Filesystem`](crate::Filesystem) impl actually
    /// implements them; otherwise the kernel's local-only lock fallback
    /// is preferable to userspace ENOSYS.
    pub posix_locks: bool,
    /// Same gate for `FUSE_FLOCK_LOCKS` -> the `flock` handler.
    pub flock_locks: bool,
}

impl MountOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn fs_name(mut self, name: impl Into<String>) -> Self {
        self.fs_name = Some(name.into());
        self
    }

    pub fn allow_other(mut self, allow: bool) -> Self {
        self.allow_other = allow;
        self
    }

    pub fn allow_root(mut self, allow: bool) -> Self {
        self.allow_root = allow;
        self
    }

    pub fn read_only(mut self, ro: bool) -> Self {
        self.read_only = ro;
        self
    }

    pub fn default_permissions(mut self, dp: bool) -> Self {
        self.default_permissions = dp;
        self
    }

    pub fn dont_mask(mut self, dm: bool) -> Self {
        self.dont_mask = dm;
        self
    }

    pub fn no_open_support(mut self, nos: bool) -> Self {
        self.no_open_support = nos;
        self
    }

    pub fn no_open_dir_support(mut self, nos: bool) -> Self {
        self.no_open_dir_support = nos;
        self
    }

    pub fn write_back(mut self, wb: bool) -> Self {
        self.write_back = wb;
        self
    }

    pub fn force_readdir_plus(mut self, rdp: bool) -> Self {
        self.force_readdir_plus = rdp;
        self
    }

    pub fn passthrough(mut self, pt: bool) -> Self {
        self.passthrough = pt;
        self
    }

    pub fn posix_locks(mut self, pl: bool) -> Self {
        self.posix_locks = pl;
        self
    }

    pub fn flock_locks(mut self, fl: bool) -> Self {
        self.flock_locks = fl;
        self
    }

    pub fn custom_options(mut self, opts: impl Into<OsString>) -> Self {
        self.custom_options = Some(opts.into());
        self
    }
}
