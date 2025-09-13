use anyhow::Result;
use clap::Parser;
use fuser::{Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request};
use libc::{ENOENT, ENOTDIR};
use log::{debug, error, info};
use redb::{Database, TableDefinition, ReadableTable};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fs;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::io::{Seek, SeekFrom, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use fuser::{ReplyCreate, ReplyEmpty, ReplyWrite, ReplyOpen, TimeOrNow};
mod erasure;

/// A simple FUSE filesystem that stores metadata in redb and uses a backing directory for file data
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The mount point for the FUSE filesystem
    mount_point: String,

    /// The target directory to translate operations to (file data lives here)
    #[arg(short, long)]
    target: String,

    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,
}

 // redb table definitions
const T_PATH_TO_INO: TableDefinition<'static, &'static str, Vec<u8>> = TableDefinition::new("path_to_ino");
const T_INO_TO_META: TableDefinition<'static, u64, Vec<u8>> = TableDefinition::new("ino_to_meta");
const T_DIR_CHILDREN: TableDefinition<'static, &'static str, Vec<u8>> = TableDefinition::new("dir_children");
const T_META_KV: TableDefinition<'static, &'static str, Vec<u8>> = TableDefinition::new("meta_kv"); // e.g., next_inode (stored as bincode(u64))

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum MetaKind {
    RegularFile = 0,
    Directory = 1,
}

impl MetaKind {
    fn to_fuser(&self) -> fuser::FileType {
        match self {
            MetaKind::RegularFile => fuser::FileType::RegularFile,
            MetaKind::Directory => fuser::FileType::Directory,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Meta {
    // Stable inode for this path
    ino: u64,
    // Absolute path within the fuse namespace (e.g., "/a/b")
    path: String,

    size: u64,
    blocks: u64,
    atime_secs: i64,
    mtime_secs: i64,
    ctime_secs: i64,
    crtime_secs: i64,
    kind: MetaKind,
    perm: u16,
    nlink: u32,
    uid: u32,
    gid: u32,
    rdev: u32,
    flags: u32,
    blksize: u32,
}

impl Meta {
    fn to_file_attr(&self) -> fuser::FileAttr {
        fuser::FileAttr {
            ino: self.ino,
            size: self.size,
            blocks: self.blocks,
            atime: UNIX_EPOCH + Duration::from_secs(self.atime_secs.max(0) as u64),
            mtime: UNIX_EPOCH + Duration::from_secs(self.mtime_secs.max(0) as u64),
            ctime: UNIX_EPOCH + Duration::from_secs(self.ctime_secs.max(0) as u64),
            crtime: UNIX_EPOCH + Duration::from_secs(self.crtime_secs.max(0) as u64),
            kind: self.kind.to_fuser(),
            perm: self.perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            flags: self.flags,
            blksize: self.blksize,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Child {
    name: String, // file name only
    ino: u64,
    kind: MetaKind,
}

struct MetadataStore {
    db: Database,
}

impl MetadataStore {
    fn open(db_path: &Path) -> Result<Self, redb::Error> {
        // Create if missing, otherwise open
        let db = if db_path.exists() {
            Database::open(db_path)
        } else {
            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            Database::create(db_path)
        }?;
        Ok(Self { db })
    }

    fn get_next_inode_and_increment(&self) -> Result<u64, redb::Error> {
        let mut wtxn = self.db.begin_write()?;
        // Open the table and do operations in an inner scope so table borrows are dropped
        let current = {
            let mut meta = wtxn.open_table(T_META_KV)?;
            let current = match meta.get("next_inode")? {
                Some(g) => {
                    // g: AccessGuard<Vec<u8>>; get bytes via .value()
                    let data = g.value();
                    bincode::deserialize::<u64>(&data[..]).unwrap_or(2)
                }
                None => 2,
            };
            debug!("allocating new inode {}, incrementing next_inode", current);
            meta.insert("next_inode", bincode::serialize(&(current + 1)).unwrap())?;
            current
        };
        // meta (and other table handles) have been dropped; now commit
        wtxn.commit()?;
        debug!("get_next_inode_and_increment committed, returning {}", current);
        Ok(current)
    }

    fn init_root_if_needed(&self, root_meta: Meta) -> Result<(), redb::Error> {
        let mut wtxn = self.db.begin_write()?;
        {
        let mut p2i = wtxn.open_table(T_PATH_TO_INO)?;
        if p2i.get("/")?.is_none() {
            info!("initializing root metadata in DB");
            p2i.insert("/", bincode::serialize(&root_meta.ino).unwrap())?;
            let mut i2m = wtxn.open_table(T_INO_TO_META)?;
            i2m.insert(root_meta.ino, bincode::serialize(&root_meta).unwrap())?;
            debug!("init_root_if_needed: inserted root meta {:?}", root_meta);
            let mut dch = wtxn.open_table(T_DIR_CHILDREN)?;
            dch.insert("/", bincode::serialize(&Vec::<Child>::new()).unwrap())?;
            let mut meta = wtxn.open_table(T_META_KV)?;
            // Initialize next_inode if not present
            if meta.get("next_inode")?.is_none() {
                meta.insert("next_inode", bincode::serialize(&2u64).unwrap())?;
            }
            info!("root metadata initialization complete");
        }
        }
        wtxn.commit()?;
        Ok(())
    }

    fn get_inode_for_path(&self, path: &str) -> Result<Option<u64>, redb::Error> {
        let rtxn = self.db.begin_read()?;
        let p2i = rtxn.open_table(T_PATH_TO_INO)?;
        match p2i.get(path)? {
            Some(g) => {
                let data = g.value();
                let ino = bincode::deserialize::<u64>(&data[..]).ok();
                Ok(ino)
            }
            None => Ok(None),
        }
    }

    fn put_mapping(&self, path: &str, ino: u64) -> Result<(), redb::Error> {
        let mut wtxn = self.db.begin_write()?;
        {
            let mut p2i = wtxn.open_table(T_PATH_TO_INO)?;
            p2i.insert(path, bincode::serialize(&ino).unwrap())?;
        }
        wtxn.commit()?;
        Ok(())
    }

    fn get_meta_by_ino(&self, ino: u64) -> Result<Option<Meta>, redb::Error> {
        let rtxn = self.db.begin_read()?;
        let i2m = rtxn.open_table(T_INO_TO_META)?;
        Ok(match i2m.get(ino)? {
            Some(bytes) => {
                let data = bytes.value();
                let meta: Meta = bincode::deserialize(&data[..]).unwrap();
                debug!("get_meta_by_ino: ino={} -> {:?}", ino, meta);
                Some(meta)
            }
            None => None,
        })
    }

    fn put_meta(&self, meta: &Meta) -> Result<(), redb::Error> {
        let mut wtxn = self.db.begin_write()?;
        {
            let mut i2m = wtxn.open_table(T_INO_TO_META)?;
            i2m.insert(meta.ino, bincode::serialize(meta).unwrap())?;
        }
        wtxn.commit()?;
        debug!("put_meta: ino={} -> {:?}", meta.ino, meta);
        Ok(())
    }

    fn get_children(&self, dir_path: &str) -> Result<Option<Vec<Child>>, redb::Error> {
        let rtxn = self.db.begin_read()?;
        let dch = rtxn.open_table(T_DIR_CHILDREN)?;
        Ok(match dch.get(dir_path)? {
            Some(bytes) => {
                let data = bytes.value();
                Some(bincode::deserialize(&data[..]).unwrap())
            }
            None => None,
        })
    }

    fn put_children(&self, dir_path: &str, children: &Vec<Child>) -> Result<(), redb::Error> {
        let mut wtxn = self.db.begin_write()?;
        {
            let mut dch = wtxn.open_table(T_DIR_CHILDREN)?;
            dch.insert(dir_path, bincode::serialize(children).unwrap())?;
        }
        wtxn.commit()?;
        Ok(())
    }

    fn remove_mapping(&self, path: &str) -> Result<(), redb::Error> {
        let mut wtxn = self.db.begin_write()?;
        {
            let mut p2i = wtxn.open_table(T_PATH_TO_INO)?;
            p2i.remove(path)?;
        }
        wtxn.commit()?;
        Ok(())
    }

    fn remove_meta_by_ino(&self, ino: u64) -> Result<(), redb::Error> {
        let mut wtxn = self.db.begin_write()?;
        {
            let mut i2m = wtxn.open_table(T_INO_TO_META)?;
            i2m.remove(ino)?;
        }
        wtxn.commit()?;
        Ok(())
    }

    fn remove_child_from_parent(&self, parent: &str, name: &str) -> Result<(), redb::Error> {
        let mut wtxn = self.db.begin_write()?;
        {
            let mut dch = wtxn.open_table(T_DIR_CHILDREN)?;
            // Read current children into an owned Vec<u8> in an inner scope so the AccessGuard is dropped
            let data_owned_opt: Option<Vec<u8>> = {
                let guard_opt = dch.get(parent)?;
                match guard_opt {
                    Some(g) => Some(g.value().to_vec()),
                    None => None,
                }
            };
            if let Some(data_owned) = data_owned_opt {
                let mut children: Vec<Child> = bincode::deserialize(&data_owned[..]).unwrap_or_default();
                children.retain(|c| c.name != name);
                dch.insert(parent, bincode::serialize(&children).unwrap())?;
            }
        }
        wtxn.commit()?;
        Ok(())
    }
}

struct WormFS {
    target_path: PathBuf,
    store: MetadataStore,
}

impl WormFS {
    fn new(target_path: PathBuf) -> Self {
        let db_path = target_path.join(".wormfs-meta").join("meta.redb");
        info!("opening metadata database at {:?}", db_path);
        let store = MetadataStore::open(&db_path).expect("Failed to open redb database");
        info!("opened metadata database at {:?}", db_path);

        // Initialize root inode = 1
        let root_meta = Self::stat_to_meta(1, "/".to_string(), &target_path)
            .unwrap_or_else(|_| Meta {
                ino: 1,
                path: "/".to_string(),
                size: 0,
                blocks: 0,
                atime_secs: 0,
                mtime_secs: 0,
                ctime_secs: 0,
                crtime_secs: 0,
                kind: MetaKind::Directory,
                perm: 0o755,
                nlink: 1,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                rdev: 0,
                flags: 0,
                blksize: 512,
            });

        store.init_root_if_needed(root_meta).expect("Failed to init root in DB");

        Self { target_path, store }
    }

    fn system_time_secs(t: SystemTime) -> i64 {
        match t.duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_secs() as i64,
            Err(e) => -(e.duration().as_secs() as i64),
        }
    }

    fn stat_to_meta(ino: u64, rel_path: String, target_root: &Path) -> std::io::Result<Meta> {
        let target = Self::translate_target(target_root, &rel_path);
        let metadata = fs::metadata(&target)?;
        let is_dir = metadata.is_dir();

        let size = metadata.len();
        let blocks = (size + 511) / 512;

        let atime = metadata.accessed().unwrap_or(UNIX_EPOCH);
        let mtime = metadata.modified().unwrap_or(UNIX_EPOCH);
        let ctime = metadata.modified().unwrap_or(UNIX_EPOCH);
        let crtime = metadata.created().unwrap_or(UNIX_EPOCH);

        // Default perms: directories 755, files 644
        let perm = if is_dir { 0o755 } else { 0o644 };

        Ok(Meta {
            ino,
            path: rel_path,
            size,
            blocks,
            atime_secs: Self::system_time_secs(atime),
            mtime_secs: Self::system_time_secs(mtime),
            ctime_secs: Self::system_time_secs(ctime),
            crtime_secs: Self::system_time_secs(crtime),
            kind: if is_dir {
                MetaKind::Directory
            } else {
                MetaKind::RegularFile
            },
            perm,
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            flags: 0,
            blksize: 512,
        })
    }

    fn translate_target(target_root: &Path, rel_path: &str) -> PathBuf {
        let rel = rel_path.trim_start_matches('/');
        target_root.join(rel)
    }

    fn get_path_for_inode(&self, ino: u64) -> Option<String> {
        match self.store.get_meta_by_ino(ino) {
            Ok(Some(m)) => Some(m.path),
            _ => None,
        }
    }

    fn ensure_path_in_db(&self, rel_path: &str) -> Result<(u64, Meta), anyhow::Error> {
        // Only consult metadata DB; do not populate from backing filesystem.
        if let Some(ino) = self.store.get_inode_for_path(rel_path)? {
            if let Some(meta) = self.store.get_meta_by_ino(ino)? {
                return Ok((ino, meta));
            }
        }
        Err(anyhow::anyhow!("metadata not found for path {}", rel_path))
    }

    fn ensure_dir_children_in_db(&self, dir_path: &str) -> Result<Vec<Child>, anyhow::Error> {
        // Only consult the metadata DB; do not read from backing filesystem.
        if let Some(children) = self.store.get_children(dir_path)? {
            return Ok(children);
        }
        // If missing, persist and return an empty list
        let out_children: Vec<Child> = Vec::new();
        self.store.put_children(dir_path, &out_children)?;
        Ok(out_children)
    }
}

impl Filesystem for WormFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup: parent={}, name={:?}", parent, name.to_string_lossy());

        // Resolve parent path from inode via metadata
        let parent_path = if parent == 1 {
            "/".to_string()
        } else {
            match self.get_path_for_inode(parent) {
                Some(p) => p,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        let child_path = if parent_path == "/" {
            format!("/{}", name.to_string_lossy())
        } else {
            format!("{}/{}", parent_path, name.to_string_lossy())
        };

        match self.ensure_path_in_db(&child_path) {
            Ok((_ino, meta)) => {
                let attr = meta.to_file_attr();
                reply.entry(&Duration::from_secs(1), &attr, 0);
            }
            Err(_) => reply.error(ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr: ino={}", ino);
        match self.store.get_meta_by_ino(ino) {
            Ok(Some(meta)) => {
                reply.attr(&Duration::from_secs(1), &meta.to_file_attr());
            }
            _ => reply.error(ENOENT),
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        debug!("readdir: ino={}, offset={}", ino, offset);

        // Resolve directory path
        let dir_path = match if ino == 1 {
            Some("/".to_string())
        } else {
            self.get_path_for_inode(ino)
        } {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Ensure this is a directory per metadata
        let is_dir = match self.store.get_meta_by_ino(ino) {
            Ok(Some(meta)) => matches!(meta.kind, MetaKind::Directory),
            _ => false,
        };
        if !is_dir {
            reply.error(ENOTDIR);
            return;
        }

        // Ensure children populated from DB (lazily populate from disk if missing)
        let children = match self.ensure_dir_children_in_db(&dir_path) {
            Ok(c) => c,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // FUSE requires "." and ".." entries; provide them with fake offsets before listing others
        let mut idx: i64 = 0;

        if offset <= idx {
            // current directory
            let this_ino = ino;
            if reply.add(this_ino, idx + 1, fuser::FileType::Directory, ".") {
                reply.ok();
                return;
            }
        }
        idx += 1;

        if offset <= idx {
            // parent directory (for root, point to itself)
            let parent_ino = 1u64;
            if reply.add(parent_ino, idx + 1, fuser::FileType::Directory, "..") {
                reply.ok();
                return;
            }
        }
        idx += 1;

        // Actual children
        for child in children.iter() {
            if idx <= offset {
                idx += 1;
                continue;
            }
            if reply.add(child.ino, idx + 1, child.kind.to_fuser(), &child.name) {
                break;
            }
            idx += 1;
        }
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        debug!("create: parent={}, name={:?}, mode={:o}", parent, name.to_string_lossy(), mode);

        let parent_path = if parent == 1 {
            "/".to_string()
        } else {
            match self.get_path_for_inode(parent) {
                Some(p) => p,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        let rel = if parent_path == "/" {
            format!("/{}", name.to_string_lossy())
        } else {
            format!("{}/{}", parent_path, name.to_string_lossy())
        };
        let target = Self::translate_target(&self.target_path, &rel);

        match OpenOptions::new().create(true).write(true).mode(mode).open(&target) {
            Ok(mut f) => {
                // Ensure metadata present in DB; if missing, create it explicitly.
                match self.store.get_inode_for_path(&rel) {
                    Ok(Some(ino)) => {
                        match self.store.get_meta_by_ino(ino) {
                            Ok(Some(meta)) => {
                                let fh = 0;
                                reply.created(&Duration::from_secs(1), &meta.to_file_attr(), 0, fh, 0);
                            }
                            _ => {
                                reply.error(ENOENT);
                            }
                        }
                    }
                    Ok(None) => {
                        // allocate inode and create meta from backing file we just created
                        match self.store.get_next_inode_and_increment() {
                            Ok(ino) => {
                                match Self::stat_to_meta(ino, rel.clone(), &self.target_path) {
                                    Ok(meta) => {
                                        let _ = self.store.put_mapping(&rel, ino);
                                        let _ = self.store.put_meta(&meta);
                                        if let Some((parent_path, name)) = split_parent_name(&rel) {
                                            if let Ok(mut children) = self.store.get_children(parent_path).map(|opt| opt.unwrap_or_default()) {
                                                if let Some(existing) = children.iter_mut().find(|c| c.name == name) {
                                                    existing.ino = ino;
                                                    existing.kind = meta.kind;
                                                } else {
                                                    children.push(Child {
                                                        name: name.to_string(),
                                                        ino,
                                                        kind: meta.kind,
                                                    });
                                                }
                                                let _ = self.store.put_children(parent_path, &children);
                                            }
                                        }
                                        let fh = 0;
                                        reply.created(&Duration::from_secs(1), &meta.to_file_attr(), 0, fh, 0);
                                    }
                                    Err(e) => {
                                        error!("failed to stat created file {:?}: {}", target, e);
                                        reply.error(ENOENT);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("failed to allocate inode: {}", e);
                                reply.error(ENOENT);
                            }
                        }
                    }
                    Err(e) => {
                        error!("db error looking up path mapping: {}", e);
                        reply.error(ENOENT);
                    }
                }
                let _ = f.sync_all();
            }
            Err(e) => {
                error!("create failed for {:?}: {}", target, e);
                reply.error(ENOENT);
            }
        }
    }

    // Basic open implementation: validate inode -> backing path, try to open with requested access,
    // but we don't maintain persistent filehandles (we return fh = 0).
    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        debug!("open: ino={}, flags={}", ino, flags);
        let path = match self.get_path_for_inode(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let target = Self::translate_target(&self.target_path, &path);

        // Build OpenOptions according to access mode. Always try to open for read;
        // enable write when access mode requests it.
        let acc_mode = flags & libc::O_ACCMODE;
        let mut opts = OpenOptions::new();
        // For O_WRONLY we shouldn't set read, for O_RDONLY set only read, for O_RDWR set both.
        if acc_mode == libc::O_WRONLY {
            opts.write(true);
        } else if acc_mode == libc::O_RDWR {
            opts.read(true);
            opts.write(true);
        } else {
            // O_RDONLY (0) or unknown -> default to read
            opts.read(true);
        }

        match opts.open(&target) {
            Ok(_f) => {
                // We don't track per-file handles; return fh 0 and echo flags as 0 for simplicity.
                reply.opened(0, 0);
            }
            Err(e) => {
                error!("open failed for {:?}: {}", target, e);
                reply.error(ENOENT);
            }
        }
    }

    // Flush: ensure any buffered data is synced to disk.
    fn flush(&mut self, _req: &Request, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        debug!("flush: ino={}", ino);
        let path = match self.get_path_for_inode(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let target = Self::translate_target(&self.target_path, &path);
        match OpenOptions::new().write(true).open(&target) {
            Ok(mut f) => {
                if let Err(e) = f.sync_all() {
                    error!("flush sync failed for {:?}: {}", target, e);
                    reply.error(ENOENT);
                    return;
                }
                reply.ok();
            }
            Err(e) => {
                error!("flush open failed for {:?}: {}", target, e);
                reply.error(ENOENT);
            }
        }
    }

    // Release: called when a file handle is closed. Ensure data synced.
    fn release(&mut self, _req: &Request, ino: u64, _fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        debug!("release: ino={}", ino);
        let path = match self.get_path_for_inode(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let target = Self::translate_target(&self.target_path, &path);
        match OpenOptions::new().write(true).open(&target) {
            Ok(mut f) => {
                if let Err(e) = f.sync_all() {
                    error!("release sync failed for {:?}: {}", target, e);
                    reply.error(ENOENT);
                    return;
                }
                reply.ok();
            }
            Err(e) => {
                error!("release open failed for {:?}: {}", target, e);
                reply.error(ENOENT);
            }
        }
    }

    // fsync: sync data (or data+metadata) to disk. datasync == true => sync_data, otherwise sync_all.
    fn fsync(&mut self, _req: &Request, ino: u64, _fh: u64, datasync: bool, reply: ReplyEmpty) {
        debug!("fsync: ino={}, datasync={}", ino, datasync);
        let path = match self.get_path_for_inode(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let target = Self::translate_target(&self.target_path, &path);
        match OpenOptions::new().read(true).write(true).open(&target) {
            Ok(mut f) => {
                let res = if datasync {
                    // sync_data is not available on all platforms via std::fs::File;
                    // fallback to sync_all which is stronger but safe.
                    f.sync_all()
                } else {
                    f.sync_all()
                };
                if let Err(e) = res {
                    error!("fsync failed for {:?}: {}", target, e);
                    reply.error(ENOENT);
                    return;
                }
                reply.ok();
            }
            Err(e) => {
                error!("fsync open failed for {:?}: {}", target, e);
                reply.error(ENOENT);
            }
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        debug!("mkdir: parent={}, name={:?}, mode={:o}", parent, name.to_string_lossy(), mode);
        let parent_path = if parent == 1 {
            "/".to_string()
        } else {
            match self.get_path_for_inode(parent) {
                Some(p) => p,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        let rel = if parent_path == "/" {
            format!("/{}", name.to_string_lossy())
        } else {
            format!("{}/{}", parent_path, name.to_string_lossy())
        };
        let target = Self::translate_target(&self.target_path, &rel);

        match fs::create_dir(&target) {
            Ok(()) => {
                // Ensure metadata present in DB; if missing, create it explicitly.
                match self.store.get_inode_for_path(&rel) {
                    Ok(Some(ino)) => {
                        match self.store.get_meta_by_ino(ino) {
                            Ok(Some(meta)) => {
                                reply.entry(&Duration::from_secs(1), &meta.to_file_attr(), 0);
                            }
                            _ => reply.error(ENOENT),
                        }
                    }
                    Ok(None) => {
                        match self.store.get_next_inode_and_increment() {
                            Ok(ino) => {
                                match Self::stat_to_meta(ino, rel.clone(), &self.target_path) {
                                    Ok(meta) => {
                                        let _ = self.store.put_mapping(&rel, ino);
                                        let _ = self.store.put_meta(&meta);
                                        if let Some((parent_path, name)) = split_parent_name(&rel) {
                                            if let Ok(mut children) = self.store.get_children(parent_path).map(|opt| opt.unwrap_or_default()) {
                                                if let Some(existing) = children.iter_mut().find(|c| c.name == name) {
                                                    existing.ino = ino;
                                                    existing.kind = meta.kind;
                                                } else {
                                                    children.push(Child {
                                                        name: name.to_string(),
                                                        ino,
                                                        kind: meta.kind,
                                                    });
                                                }
                                                let _ = self.store.put_children(parent_path, &children);
                                            }
                                        }
                                        reply.entry(&Duration::from_secs(1), &meta.to_file_attr(), 0);
                                    }
                                    Err(e) => {
                                        error!("failed to stat created dir {:?}: {}", target, e);
                                        reply.error(ENOENT);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("failed to allocate inode: {}", e);
                                reply.error(ENOENT);
                            }
                        }
                    }
                    Err(e) => {
                        error!("db error looking up path mapping: {}", e);
                        reply.error(ENOENT);
                    }
                }
            }
            Err(e) => {
                error!("mkdir failed for {:?}: {}", target, e);
                reply.error(ENOENT);
            }
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("unlink: parent={}, name={:?}", parent, name.to_string_lossy());
        let parent_path = if parent == 1 {
            "/".to_string()
        } else {
            match self.get_path_for_inode(parent) {
                Some(p) => p,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        let rel = if parent_path == "/" {
            format!("/{}", name.to_string_lossy())
        } else {
            format!("{}/{}", parent_path, name.to_string_lossy())
        };
        let target = Self::translate_target(&self.target_path, &rel);

        match fs::remove_file(&target) {
            Ok(()) => {
                // remove mapping and meta if present
                if let Ok(Some(ino)) = self.store.get_inode_for_path(&rel) {
                    let _ = self.store.remove_mapping(&rel);
                    let _ = self.store.remove_meta_by_ino(ino);
                }
                let _ = self.store.remove_child_from_parent(&parent_path, &name.to_string_lossy());
                reply.ok();
            }
            Err(e) => {
                error!("unlink failed for {:?}: {}", target, e);
                reply.error(ENOENT);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("rmdir: parent={}, name={:?}", parent, name.to_string_lossy());
        let parent_path = if parent == 1 {
            "/".to_string()
        } else {
            match self.get_path_for_inode(parent) {
                Some(p) => p,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        let rel = if parent_path == "/" {
            format!("/{}", name.to_string_lossy())
        } else {
            format!("{}/{}", parent_path, name.to_string_lossy())
        };
        let target = Self::translate_target(&self.target_path, &rel);

        match fs::remove_dir(&target) {
            Ok(()) => {
                if let Ok(Some(ino)) = self.store.get_inode_for_path(&rel) {
                    let _ = self.store.remove_mapping(&rel);
                    let _ = self.store.remove_meta_by_ino(ino);
                }
                let _ = self.store.remove_child_from_parent(&parent_path, &name.to_string_lossy());
                reply.ok();
            }
            Err(e) => {
                error!("rmdir failed for {:?}: {}", target, e);
                reply.error(ENOENT);
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        debug!("write: ino={}, offset={}, len={}", ino, offset, data.len());
        let rel = match self.get_path_for_inode(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let target = Self::translate_target(&self.target_path, &rel);

        match OpenOptions::new().write(true).create(true).open(&target) {
            Ok(mut f) => {
                if let Err(e) = f.seek(SeekFrom::Start(offset as u64)) {
                    error!("seek failed: {}", e);
                    reply.error(ENOENT);
                    return;
                }
                match f.write(data) {
                    Ok(written) => {
                        let _ = f.sync_all();
                        // update meta size if needed
                        if let Ok(Some(mut meta)) = self.store.get_meta_by_ino(ino) {
                            if (offset as u64 + written as u64) > meta.size {
                                meta.size = offset as u64 + written as u64;
                                meta.blocks = (meta.size + 511) / 512;
                                let _ = self.store.put_meta(&meta);
                            }
                        }
                        reply.written(written as u32);
                    }
                    Err(e) => {
                        error!("write failed: {}", e);
                        reply.error(ENOENT);
                    }
                }
            }
            Err(e) => {
                error!("open for write failed: {}", e);
                reply.error(ENOENT);
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!("setattr: ino={}, size={:?}, mode={:?}", ino, size, mode);
        if let Some(sz) = size {
            if let Some(path) = self.get_path_for_inode(ino) {
                let target = Self::translate_target(&self.target_path, &path);
                match OpenOptions::new().write(true).open(&target) {
                    Ok(f) => {
                        if let Err(e) = f.set_len(sz) {
                            error!("truncate failed: {}", e);
                            reply.error(ENOENT);
                            return;
                        }
                        if let Ok(Some(mut meta)) = self.store.get_meta_by_ino(ino) {
                            meta.size = sz;
                            meta.blocks = (sz + 511) / 512;
                            let _ = self.store.put_meta(&meta);
                            reply.attr(&Duration::from_secs(1), &meta.to_file_attr());
                            return;
                        }
                    }
                    Err(e) => {
                        error!("open for truncate failed: {}", e);
                        reply.error(ENOENT);
                        return;
                    }
                }
            }
            reply.error(ENOENT);
            return;
        }

        // For other attribute changes we simply echo back current attrs if present
        match self.store.get_meta_by_ino(ino) {
            Ok(Some(mut meta)) => {
                if let Some(m) = mode {
                    meta.perm = (m & 0o7777) as u16;
                }
                if let Some(u) = uid {
                    meta.uid = u;
                }
                if let Some(g) = gid {
                    meta.gid = g;
                }
                if let Some(f) = flags {
                    meta.flags = f;
                }
                let _ = self.store.put_meta(&meta);
                reply.attr(&Duration::from_secs(1), &meta.to_file_attr());
            }
            _ => reply.error(ENOENT),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        debug!("read: ino={}, offset={}, size={}", ino, offset, size);

        let rel_path = match self.get_path_for_inode(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let target_path = WormFS::translate_target(&self.target_path, &rel_path);

        match fs::read(&target_path) {
            Ok(data) => {
                let start = offset as usize;
                let end = std::cmp::min(start + size as usize, data.len());

                if start >= data.len() {
                    reply.data(&[]);
                } else {
                    reply.data(&data[start..end]);
                }
            }
            Err(_) => {
                reply.error(ENOENT);
            }
        }
    }
}

fn split_parent_name(path: &str) -> Option<(&str, &str)> {
    if path == "/" {
        return None;
    }
    if let Some(idx) = path.rfind('/') {
        if idx == 0 {
            // parent is root
            let name = &path[1..];
            return Some(("/", name));
        } else {
            let parent = &path[..idx];
            let name = &path[idx + 1..];
            return Some((parent, name));
        }
    }
    None
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging - default to debug
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    info!(
        "wormfs starting: mount_point={}, target={}, debug={}",
        args.mount_point, args.target, args.debug
    );

    let target_path = PathBuf::from(&args.target);

    // Verify target path exists
    if !target_path.exists() {
        error!("Target path does not exist: {}", args.target);
        std::process::exit(1);
    }

    if !target_path.is_dir() {
        error!("Target path is not a directory: {}", args.target);
        std::process::exit(1);
    }

    info!("Mounting WormFS at {} -> {}", args.mount_point, args.target);

    let filesystem = WormFS::new(target_path);

    let options = vec![
        // Mount read-write (remove Read-Only option to allow writes)
        MountOption::FSName("wormfs".to_string()),
        MountOption::AllowOther, // allow non-root users to see the mount
    ];

    fuser::mount2(filesystem, &args.mount_point, &options)?;

    Ok(())
}
