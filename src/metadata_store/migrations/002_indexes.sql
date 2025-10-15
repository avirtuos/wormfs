-- WormFS Metadata Store - Indexes for Performance
-- Phase 1: Single-node operation indexes

-- Index on files(inode) for fast inode lookups
-- This is critical for FUSE operations which primarily work with inodes
CREATE INDEX IF NOT EXISTS idx_files_inode ON files(inode);

-- Index on files(parent_path, name) for directory listing performance
-- Speeds up list_directory() operations and file lookups by parent+name
CREATE INDEX IF NOT EXISTS idx_files_parent_name ON files(parent_path, name);

-- Index on files(path) for fast path-based lookups
-- Although path is UNIQUE (which creates an implicit index in SQLite),
-- explicitly creating it ensures consistent query planning
CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);

-- Index on stripes(file_id, stripe_index) for stripe retrieval
-- Speeds up get_file_stripes() and get_stripe_at_offset() queries
CREATE INDEX IF NOT EXISTS idx_stripes_file ON stripes(file_id, stripe_index);

-- Index on chunks(stripe_id) for chunk retrieval
-- Critical for get_stripe_chunks() performance
CREATE INDEX IF NOT EXISTS idx_chunks_stripe ON chunks(stripe_id);

-- Index on chunks(node_id, disk_id) for cluster-wide chunk queries
-- Useful for rebalancing and capacity planning (Phase 3+)
CREATE INDEX IF NOT EXISTS idx_chunks_location ON chunks(node_id, disk_id);

-- Index on chunks(status) for health monitoring queries
-- Allows fast queries for corrupt/missing chunks
CREATE INDEX IF NOT EXISTS idx_chunks_status ON chunks(status);

-- Index on locks(file_id) for lock queries
-- Speeds up get_file_locks() and lock conflict detection
CREATE INDEX IF NOT EXISTS idx_locks_file ON locks(file_id);

-- Index on locks(expires_at) for cleanup operations
-- Critical for cleanup_expired_locks() performance
CREATE INDEX IF NOT EXISTS idx_locks_expires ON locks(expires_at);
