-- WormFS Metadata Store - Inode Management
-- Inode allocation and reservation system

-- Inode pool tracks which inodes are available
-- We use a simple counter-based approach for Phase 1
CREATE TABLE IF NOT EXISTS inode_pool (
    id INTEGER PRIMARY KEY CHECK (id = 1), -- Singleton table
    next_inode INTEGER NOT NULL DEFAULT 2  -- Start from 2 (1 is reserved for root)
);

-- Initialize the inode pool
INSERT OR IGNORE INTO inode_pool (id, next_inode) VALUES (1, 2);

-- Inode reservations for distributed coordination
-- Reservations expire after 1 hour if not confirmed
CREATE TABLE IF NOT EXISTS inode_reservations (
    inode INTEGER PRIMARY KEY,
    reserved_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

-- Index for cleanup of expired reservations
CREATE INDEX IF NOT EXISTS idx_inode_reservations_expires_at ON inode_reservations(expires_at);
