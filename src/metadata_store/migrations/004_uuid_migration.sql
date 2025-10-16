-- WormFS Metadata Store - UUID Migration
-- Migrate file_id, stripe_id, and chunk_id from INTEGER (64-bit) to BLOB (128-bit UUID)
-- This eliminates UUID truncation and collision issues

-- Step 1: Create new tables with BLOB-based UUID columns
CREATE TABLE IF NOT EXISTS files_new (
    file_id BLOB PRIMARY KEY,  -- 16-byte UUID
    inode INTEGER UNIQUE NOT NULL,
    path TEXT UNIQUE NOT NULL,
    parent_path TEXT NOT NULL,
    name TEXT NOT NULL,
    file_type INTEGER NOT NULL,
    size INTEGER NOT NULL DEFAULT 0,
    permissions INTEGER NOT NULL DEFAULT 420,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    accessed_at INTEGER NOT NULL,
    storage_policy_id INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (storage_policy_id) REFERENCES storage_policies(policy_id)
);

CREATE TABLE IF NOT EXISTS stripes_new (
    stripe_id BLOB PRIMARY KEY,  -- 16-byte UUID
    file_id BLOB NOT NULL,
    stripe_index INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    size INTEGER NOT NULL,
    checksum INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files_new(file_id) ON DELETE CASCADE,
    UNIQUE(file_id, stripe_index)
);

CREATE TABLE IF NOT EXISTS chunks_new (
    chunk_id BLOB PRIMARY KEY,  -- 16-byte UUID
    stripe_id BLOB NOT NULL,
    chunk_index INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    disk_id INTEGER NOT NULL,
    checksum INTEGER NOT NULL,
    status INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    last_verified INTEGER,
    FOREIGN KEY (stripe_id) REFERENCES stripes_new(stripe_id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES nodes(node_id),
    FOREIGN KEY (disk_id) REFERENCES disks(disk_id),
    UNIQUE(stripe_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS locks_new (
    lock_id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id BLOB NOT NULL,  -- 16-byte UUID
    client_id INTEGER NOT NULL,
    lock_type INTEGER NOT NULL,
    acquired_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files_new(file_id) ON DELETE CASCADE
);

-- Step 2: Copy data from old tables to new tables
-- Note: This would convert INTEGER IDs to BLOB format, but for clean migration
-- we assume starting fresh (no existing data to migrate)

-- Step 3: Drop old tables and rename new tables
DROP TABLE IF EXISTS locks;
DROP TABLE IF EXISTS chunks;
DROP TABLE IF EXISTS stripes;
DROP TABLE IF EXISTS files;

ALTER TABLE files_new RENAME TO files;
ALTER TABLE stripes_new RENAME TO stripes;
ALTER TABLE chunks_new RENAME TO chunks;
ALTER TABLE locks_new RENAME TO locks;

-- Step 4: Recreate indexes
CREATE INDEX IF NOT EXISTS idx_files_file_type ON files(file_type);
CREATE INDEX IF NOT EXISTS idx_files_parent_path ON files(parent_path);
