-- Create root directory entry
-- This migration creates the filesystem root directory (/) if it doesn't already exist.
-- The root directory is essential for all filesystem operations, as all paths
-- are validated against their parent directory.
--
-- OWNERSHIP: The migration creates root with secure defaults (uid=0, gid=0, 0755).
-- The FileSystemService.initialize_root() method will update ownership to match
-- runtime configuration (e.g., the configured uid/gid for the mount) on first run.

-- Use a deterministic UUID for the root directory (all zeros)
-- This ensures the root directory has the same file_id across all nodes
INSERT OR IGNORE INTO files (
    file_id,
    inode,
    path,
    parent_path,
    name,
    file_type,
    size,
    permissions,
    uid,
    gid,
    created_at,
    modified_at,
    accessed_at,
    storage_policy_id,
    target
) VALUES (
    x'00000000000000000000000000000000',  -- file_id: all zeros UUID
    1,                                     -- inode: root always gets inode 1
    '/',                                   -- path
    '/',                                   -- parent_path (root is its own parent)
    '',                                    -- name (root has empty name)
    1,                                     -- file_type: 1 = directory
    0,                                     -- size: 0 for directories
    493,                                   -- permissions: 0755 in decimal (rwxr-xr-x) - standard directory permissions
    0,                                     -- uid: 0 (root user) - secure default, updated by initialize_root()
    0,                                     -- gid: 0 (root group) - secure default, updated by initialize_root()
    strftime('%s', 'now'),                 -- created_at
    strftime('%s', 'now'),                 -- modified_at
    strftime('%s', 'now'),                 -- accessed_at
    1,                                     -- storage_policy_id: default policy
    NULL                                   -- target: NULL for directories
);
