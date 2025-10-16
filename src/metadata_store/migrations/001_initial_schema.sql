-- WormFS Metadata Store - Initial Schema
-- Phase 1: Single-node operation with basic file, stripe, and chunk tracking

-- Storage policies define erasure coding configuration
CREATE TABLE IF NOT EXISTS storage_policies (
    policy_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    data_shards INTEGER NOT NULL,
    parity_shards INTEGER NOT NULL,
    stripe_size INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

-- Insert default storage policy for Phase 1 (2+1 erasure coding)
INSERT OR IGNORE INTO storage_policies (policy_id, name, data_shards, parity_shards, stripe_size, created_at)
VALUES (1, 'default', 2, 1, 1048576, strftime('%s', 'now'));

-- Nodes in the cluster (single node for Phase 1)
CREATE TABLE IF NOT EXISTS nodes (
    node_id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    status INTEGER NOT NULL DEFAULT 0, -- 0=Online, 1=Offline, 2=Failed
    last_seen INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

-- Disks available for chunk storage
CREATE TABLE IF NOT EXISTS disks (
    disk_id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    total_space INTEGER NOT NULL,
    free_space INTEGER NOT NULL,
    status INTEGER NOT NULL DEFAULT 0, -- 0=Healthy, 1=Degraded, 2=Failed
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
    UNIQUE(node_id, path)
);

-- Files and directories in the filesystem
-- Note: file_id is caller-provided (UUID-based), not auto-increment
-- This design supports distributed operation and deterministic Raft log entries
CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER PRIMARY KEY,
    inode INTEGER UNIQUE NOT NULL,
    path TEXT UNIQUE NOT NULL,
    parent_path TEXT NOT NULL,
    name TEXT NOT NULL,
    file_type INTEGER NOT NULL, -- 0=regular file, 1=directory, 2=symlink
    size INTEGER NOT NULL DEFAULT 0,
    permissions INTEGER NOT NULL DEFAULT 420, -- 0o644 in octal
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    accessed_at INTEGER NOT NULL,
    storage_policy_id INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (storage_policy_id) REFERENCES storage_policies(policy_id)
);

-- Stripes are chunks of files that are erasure-coded
-- Note: stripe_id is caller-provided (UUID-based), not auto-increment
CREATE TABLE IF NOT EXISTS stripes (
    stripe_id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL,
    stripe_index INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    size INTEGER NOT NULL,
    checksum INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE,
    UNIQUE(file_id, stripe_index)
);

-- Chunks are the actual data/parity pieces stored on disks
-- Note: chunk_id is caller-provided (UUID-based), not auto-increment
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id INTEGER PRIMARY KEY,
    stripe_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    disk_id INTEGER NOT NULL,
    checksum INTEGER NOT NULL,
    status INTEGER NOT NULL DEFAULT 0, -- 0=Healthy, 1=Corrupt, 2=Missing, 3=Rebuilding
    created_at INTEGER NOT NULL,
    last_verified INTEGER,
    FOREIGN KEY (stripe_id) REFERENCES stripes(stripe_id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES nodes(node_id),
    FOREIGN KEY (disk_id) REFERENCES disks(disk_id),
    UNIQUE(stripe_id, chunk_index)
);

-- File locks for coordinating concurrent access
CREATE TABLE IF NOT EXISTS locks (
    lock_id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    client_id INTEGER NOT NULL,
    lock_type INTEGER NOT NULL, -- 0=Read, 1=Write
    acquired_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_files_file_type ON files(file_type);
CREATE INDEX IF NOT EXISTS idx_files_parent_path ON files(parent_path);
