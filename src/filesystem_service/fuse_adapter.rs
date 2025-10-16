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
