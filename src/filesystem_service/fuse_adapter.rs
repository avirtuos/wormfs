//! FUSE adapter that bridges fuser::Filesystem trait with FileSystemService.
//!
//! This adapter handles the synchronous FUSE kernel API and translates it to
//! async FileSystemService calls using tokio's block_on.

#[cfg(feature = "fuser")]
use super::implementation::FileSystemServiceImpl;
#[cfg(feature = "fuser")]
use super::inode::ROOT_INODE;
#[cfg(feature = "fuser")]
use super::types::{ClientId, Error, FileType};
#[cfg(feature = "fuser")]
use super::FileSystemService;
#[cfg(feature = "fuser")]
use fuser::{
    FileAttr as FuseFileAttr, FileType as FuseFileType, Filesystem, ReplyAttr, ReplyDirectory,
    ReplyEntry, Request, TimeOrNow,
};
#[cfg(feature = "fuser")]
use std::ffi::OsStr;
#[cfg(feature = "fuser")]
use std::sync::Arc;
#[cfg(feature = "fuser")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "fuser")]
const TTL: Duration = Duration::from_secs(1);

/// FUSE adapter wrapping FileSystemServiceImpl.
///
/// Translates synchronous FUSE callbacks to async FileSystemService calls.
#[cfg(feature = "fuser")]
pub struct FuseAdapter {
    service: Arc<FileSystemServiceImpl>,
    runtime: tokio::runtime::Handle,
}

#[cfg(feature = "fuser")]
impl FuseAdapter {
    /// Create a new FuseAdapter.
    pub fn new(service: Arc<FileSystemServiceImpl>, runtime: tokio::runtime::Handle) -> Self {
        Self { service, runtime }
    }

    /// Convert our FileAttr to FUSE FileAttr.
    fn to_fuse_attr(&self, attr: &super::types::FileAttr) -> FuseFileAttr {
        FuseFileAttr {
            ino: attr.ino,
            size: attr.size,
            blocks: attr.blocks,
            atime: attr.atime,
            mtime: attr.mtime,
            ctime: attr.ctime,
            crtime: attr.crtime,
            kind: match attr.kind {
                FileType::RegularFile => FuseFileType::RegularFile,
                FileType::Directory => FuseFileType::Directory,
                FileType::Symlink => FuseFileType::Symlink,
                FileType::NamedPipe => FuseFileType::NamedPipe,
                FileType::BlockDevice => FuseFileType::BlockDevice,
                FileType::CharDevice => FuseFileType::CharDevice,
                FileType::Socket => FuseFileType::Socket,
            },
            perm: attr.perm,
            nlink: attr.nlink,
            uid: attr.uid,
            gid: attr.gid,
            rdev: attr.rdev,
            blksize: attr.blksize,
            flags: attr.flags,
        }
    }

    /// Get client ID from FUSE request.
    fn get_client_id(&self, req: &Request) -> ClientId {
        ClientId::new(req.unique())
    }
}

#[cfg(feature = "fuser")]
impl Filesystem for FuseAdapter {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        tracing::info!("FUSE filesystem initializing...");

        // Initialize root directory
        match self.runtime.block_on(self.service.initialize_root()) {
            Ok(_) => {
                tracing::info!("FUSE filesystem initialized successfully");
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to initialize root directory: {}", e);
                Err(libc::EIO)
            }
        }
    }

    fn destroy(&mut self) {
        tracing::info!("FUSE filesystem shutting down");
    }

    fn lookup(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        tracing::debug!("lookup: parent={}, name={:?}", parent, name);

        // Convert OsStr to &str
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent directory
        let parent_record = match self.runtime.block_on(async {
            use crate::metadata_store::MetadataStore;
            self.service
                .metadata_store()
                .get_file_by_inode(parent)
                .await
        }) {
            Ok(record) => record,
            Err(_) => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Check execute permission on parent directory for traversal
        let uid = req.uid();
        let gid = req.gid();
        if let Err(_) = crate::filesystem_service::permissions::check_traverse_permission(
            uid,
            gid,
            parent_record.uid,
            parent_record.gid,
            parent_record.permissions,
        ) {
            tracing::debug!(
                "lookup: permission denied for user {}:{} to traverse directory inode={}",
                uid,
                gid,
                parent
            );
            reply.error(libc::EACCES);
            return;
        }

        // List directory and find the file
        let files = match self.runtime.block_on(async {
            use crate::metadata_store::MetadataStore;
            self.service
                .metadata_store()
                .list_directory(&parent_record.path)
                .await
        }) {
            Ok(files) => files,
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };

        // Find the file by name
        for file in files {
            if let Some(file_name) = file.path.file_name() {
                if file_name == name {
                    // Found the file - get its attributes
                    match self
                        .runtime
                        .block_on(self.service.as_ref().getattr(file.inode))
                    {
                        Ok(attr) => {
                            let fuse_attr = self.to_fuse_attr(&attr);
                            reply.entry(&TTL, &fuse_attr, 0);
                            return;
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                            return;
                        }
                    }
                }
            }
        }

        // File not found
        reply.error(libc::ENOENT);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        tracing::debug!("getattr: ino={}", ino);

        match self.runtime.block_on(self.service.as_ref().getattr(ino)) {
            Ok(attr) => {
                let fuse_attr = self.to_fuse_attr(&attr);
                reply.attr(&TTL, &fuse_attr);
            }
            Err(e) => {
                tracing::warn!("getattr failed for inode {}: {}", ino, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn readdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        tracing::debug!("readdir: ino={}, offset={}", ino, offset);

        let client_id = self.get_client_id(req);

        match self
            .runtime
            .block_on(self.service.as_ref().readdir(ino, offset, client_id))
        {
            Ok(entries) => {
                for (i, entry) in entries.iter().enumerate().skip(offset as usize) {
                    let kind = match entry.kind {
                        FileType::RegularFile => FuseFileType::RegularFile,
                        FileType::Directory => FuseFileType::Directory,
                        FileType::Symlink => FuseFileType::Symlink,
                        FileType::NamedPipe => FuseFileType::NamedPipe,
                        FileType::BlockDevice => FuseFileType::BlockDevice,
                        FileType::CharDevice => FuseFileType::CharDevice,
                        FileType::Socket => FuseFileType::Socket,
                    };

                    // buffer_full means the buffer is full, should stop adding more entries
                    if reply.add(entry.ino, (i + 1) as i64, kind, &entry.name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => {
                tracing::warn!("readdir failed for inode {}: {}", ino, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn unlink(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let name_str = name.to_string_lossy();
        let client_id = self.get_client_id(req);
        let uid = req.uid();
        let gid = req.gid();

        tracing::debug!(
            "FUSE unlink: parent={}, name={}, uid={}, gid={}",
            parent,
            name_str,
            uid,
            gid
        );

        match self
            .runtime
            .block_on(self.service.unlink(parent, &name_str, uid, gid, client_id))
        {
            Ok(()) => {
                tracing::debug!("FUSE unlink success");
                reply.ok();
            }
            Err(e) => {
                let errno = e.to_errno();
                tracing::debug!("FUSE unlink error: {}, errno={}", e, errno);
                reply.error(errno);
            }
        }
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let link_name_str = link_name.to_string_lossy();
        let target_str = target.to_string_lossy();
        let client_id = self.get_client_id(req);

        tracing::debug!(
            "FUSE symlink: parent={}, link_name={}, target={}",
            parent,
            link_name_str,
            target_str
        );

        match self.runtime.block_on(self.service.symlink(
            parent,
            &link_name_str,
            &target_str,
            req.uid(),
            req.gid(),
            client_id,
        )) {
            Ok(attr) => {
                let fuse_attr = self.to_fuse_attr(&attr);
                tracing::debug!("FUSE symlink success: inode={}", attr.ino);
                reply.entry(&TTL, &fuse_attr, 0);
            }
            Err(e) => {
                let errno = e.to_errno();
                tracing::debug!("FUSE symlink error: {}, errno={}", e, errno);
                reply.error(errno);
            }
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyData) {
        tracing::debug!("FUSE readlink: ino={}", ino);

        match self.runtime.block_on(self.service.readlink(ino)) {
            Ok(target) => {
                tracing::debug!("FUSE readlink success: target={}", target);
                reply.data(target.as_bytes());
            }
            Err(e) => {
                let errno = e.to_errno();
                tracing::debug!("FUSE readlink error: {}, errno={}", e, errno);
                reply.error(errno);
            }
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_string_lossy();
        let client_id = self.get_client_id(req);
        let uid = req.uid();
        let gid = req.gid();

        tracing::debug!(
            "FUSE mkdir: parent={}, name={}, mode={:#o}, uid={}, gid={}",
            parent,
            name_str,
            mode,
            uid,
            gid
        );

        match self.runtime.block_on(
            self.service
                .mkdir(parent, &name_str, mode, uid, gid, client_id),
        ) {
            Ok(attr) => {
                let fuse_attr = self.to_fuse_attr(&attr);
                tracing::debug!("FUSE mkdir success: inode={}", attr.ino);
                reply.entry(&TTL, &fuse_attr, 0);
            }
            Err(e) => {
                let errno = e.to_errno();
                tracing::debug!("FUSE mkdir error: {}, errno={}", e, errno);
                reply.error(errno);
            }
        }
    }

    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let name_str = name.to_string_lossy();
        let client_id = self.get_client_id(req);
        let uid = req.uid();
        let gid = req.gid();

        tracing::debug!(
            "FUSE rmdir: parent={}, name={}, uid={}, gid={}",
            parent,
            name_str,
            uid,
            gid
        );

        match self
            .runtime
            .block_on(self.service.rmdir(parent, &name_str, uid, gid, client_id))
        {
            Ok(()) => {
                tracing::debug!("FUSE rmdir success");
                reply.ok();
            }
            Err(e) => {
                let errno = e.to_errno();
                tracing::debug!("FUSE rmdir error: {}, errno={}", e, errno);
                reply.error(errno);
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::debug!("FUSE release: fh={}", fh);

        match self.runtime.block_on(self.service.release(fh)) {
            Ok(()) => {
                tracing::debug!("FUSE release success");
                reply.ok();
            }
            Err(e) => {
                let errno = e.to_errno();
                tracing::debug!("FUSE release error: {}, errno={}", e, errno);
                reply.error(errno);
            }
        }
    }
}

// Export a stub when fuser feature is disabled
#[cfg(not(feature = "fuser"))]
pub struct FuseAdapter;

#[cfg(not(feature = "fuser"))]
impl FuseAdapter {
    pub fn new(_service: std::sync::Arc<super::implementation::FileSystemServiceImpl>) -> Self {
        Self
    }
}
