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

    /// Convert metadata store error to appropriate errno.
    ///
    /// This helper provides more granular error mapping than hardcoded errno values,
    /// distinguishing between "not found" vs "internal error" vs other failure types.
    fn metadata_error_to_errno(&self, error: &crate::metadata_store::Error) -> i32 {
        use crate::metadata_store::Error as MError;
        match error {
            // Not found errors
            MError::FileNotFoundByInode(_)
            | MError::FileNotFoundByPath(_)
            | MError::FileNotFoundByFileId(_)
            | MError::ParentNotFound(_) => libc::ENOENT,

            // Already exists
            MError::FileAlreadyExists(_) => libc::EEXIST,

            // Lock errors
            MError::LockConflict { .. } | MError::LockNotFound { .. } => libc::ENOLCK,

            // Invalid argument errors
            MError::ConstraintViolation(_) | MError::ConfigInvalid(_) | MError::ConfigError(_) => {
                libc::EINVAL
            }

            // Database/query errors map to EIO
            MError::QueryError(_)
            | MError::TransactionError(_)
            | MError::ConnectionError(_)
            | MError::SchemaInitFailed(_)
            | MError::SnapshotFailed(_)
            | MError::RestoreFailed(_) => libc::EIO,

            // Inode allocation errors
            MError::InodeSpaceExhausted | MError::NoAvailableInodes => libc::ENOSPC,
            MError::InodeNotReserved(_)
            | MError::InodeInUse(_)
            | MError::InodeReservationExpired(_) => libc::EINVAL,

            // Stripe/chunk not found (internal consistency error)
            MError::StripeNotFound(_) | MError::ChunkNotFound(_) => libc::EIO,

            // I/O error
            MError::Io(_) => libc::EIO,
        }
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
                tracing::debug!("lookup: invalid UTF-8 in filename: {:?}", name);
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
            Err(e) => {
                let errno = self.metadata_error_to_errno(&e);
                tracing::debug!(
                    "lookup: failed to get parent directory inode {}: {} (errno={})",
                    parent,
                    e,
                    errno
                );
                reply.error(errno);
                return;
            }
        };

        // Check execute permission on parent directory for traversal
        let uid = req.uid();
        let gid = req.gid();
        if let Err(e) = crate::filesystem_service::permissions::check_traverse_permission(
            uid,
            gid,
            parent_record.uid,
            parent_record.gid,
            parent_record.permissions,
        ) {
            tracing::debug!(
                "lookup: permission denied for user {}:{} to traverse directory inode={}: {}",
                uid,
                gid,
                parent,
                e
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
            Err(e) => {
                let errno = self.metadata_error_to_errno(&e);
                tracing::warn!(
                    "lookup: failed to list directory inode {} (path {:?}): {} (errno={})",
                    parent,
                    parent_record.path,
                    e,
                    errno
                );
                reply.error(errno);
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
                        Err(e) => {
                            let errno = e.to_errno();
                            tracing::warn!("lookup: found file '{}' (inode {}) in directory listing but getattr failed: {} (errno={})", name_str, file.inode, e, errno);
                            reply.error(errno);
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

                    // Calculate the next offset (must account for skipped entries)
                    let next_offset = offset + i as i64 + 1;

                    // buffer_full means the buffer is full, should stop adding more entries
                    if reply.add(entry.ino, next_offset, kind, &entry.name) {
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

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let name_str = name.to_string_lossy();
        let client_id = self.get_client_id(req);
        let uid = req.uid();
        let gid = req.gid();

        tracing::debug!(
            "FUSE create: parent={}, name={}, mode={:#o}, uid={}, gid={}",
            parent,
            name_str,
            mode,
            uid,
            gid
        );

        match self.runtime.block_on(
            self.service
                .create(parent, &name_str, mode, uid, gid, client_id),
        ) {
            Ok(attr) => {
                // After creating the file, open it to get a proper file handle
                // This ensures the file is tracked in open_files and can be written to
                let open_flags = _flags as u32;
                match self
                    .runtime
                    .block_on(self.service.open(attr.ino, open_flags, uid, gid, client_id))
                {
                    Ok((fh, _)) => {
                        let fuse_attr = self.to_fuse_attr(&attr);
                        tracing::debug!("FUSE create success: inode={}, fh={}", attr.ino, fh);
                        reply.created(&TTL, &fuse_attr, 0, fh, 0);
                    }
                    Err(e) => {
                        let errno = e.to_errno();
                        tracing::debug!("FUSE create+open error: {}, errno={}", e, errno);
                        reply.error(errno);
                    }
                }
            }
            Err(e) => {
                let errno = e.to_errno();
                tracing::debug!("FUSE create error: {}, errno={}", e, errno);
                reply.error(errno);
            }
        }
    }

    fn open(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let client_id = self.get_client_id(req);
        let uid = req.uid();
        let gid = req.gid();

        tracing::debug!(
            "FUSE open: ino={}, flags={:#x}, uid={}, gid={}",
            ino,
            flags,
            uid,
            gid
        );

        match self
            .runtime
            .block_on(self.service.open(ino, flags as u32, uid, gid, client_id))
        {
            Ok((fh, _attr)) => {
                tracing::debug!("FUSE open success: ino={}, fh={}", ino, fh);
                reply.opened(fh, 0);
            }
            Err(e) => {
                let errno = e.to_errno();
                tracing::debug!("FUSE open error: {}, errno={}", e, errno);
                reply.error(errno);
            }
        }
    }

    fn read(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let client_id = self.get_client_id(req);
        let uid = req.uid();
        let gid = req.gid();

        tracing::debug!(
            "FUSE read: ino={}, offset={}, size={}, uid={}, gid={}",
            ino,
            offset,
            size,
            uid,
            gid
        );

        if offset < 0 {
            tracing::debug!("FUSE read error: negative offset");
            reply.error(libc::EINVAL);
            return;
        }

        match self.runtime.block_on(self.service.read(
            ino,
            offset as u64,
            size,
            uid,
            gid,
            client_id,
        )) {
            Ok(data) => {
                tracing::debug!("FUSE read success: {} bytes", data.len());
                reply.data(&data);
            }
            Err(e) => {
                let errno = e.to_errno();
                tracing::debug!("FUSE read error: {}, errno={}", e, errno);
                reply.error(errno);
            }
        }
    }

    fn write(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let client_id = self.get_client_id(req);
        let uid = req.uid();
        let gid = req.gid();

        tracing::debug!(
            "FUSE write: ino={}, fh={}, offset={}, size={}, uid={}, gid={}",
            ino,
            fh,
            offset,
            data.len(),
            uid,
            gid
        );

        if offset < 0 {
            tracing::debug!("FUSE write error: negative offset");
            reply.error(libc::EINVAL);
            return;
        }

        match self.runtime.block_on(self.service.write(
            ino,
            fh,
            offset as u64,
            data.to_vec(),
            uid,
            gid,
            client_id,
        )) {
            Ok(written) => {
                tracing::debug!("FUSE write success: {} bytes", written);
                reply.written(written);
            }
            Err(e) => {
                let errno = e.to_errno();
                tracing::debug!("FUSE write error: {}, errno={}", e, errno);
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
