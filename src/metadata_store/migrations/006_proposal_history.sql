-- Proposal history table for tracking Raft proposals applied on this node
-- Visible in AdminUI Quorum tab for troubleshooting and monitoring

CREATE TABLE IF NOT EXISTS proposal_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Raft log metadata
    log_index INTEGER NOT NULL UNIQUE,      -- Raft log index (for idempotency)
    log_term INTEGER NOT NULL,               -- Raft term when committed
    leader_node_id INTEGER NOT NULL,         -- Node that was leader when proposed

    -- Timing
    applied_at INTEGER NOT NULL,             -- Unix timestamp when applied locally

    -- Operation summary (for table display)
    operation_type TEXT NOT NULL,            -- "AtomicTransaction", "TransactionPrepare", etc.
    tx_id TEXT,                              -- Transaction ID in hex (if applicable)
    operation_count INTEGER NOT NULL DEFAULT 1,  -- Number of sub-operations

    -- Result
    success INTEGER NOT NULL DEFAULT 1,      -- 1=success, 0=error
    error_message TEXT,                      -- Error message if failed

    -- Full operation details (for click-through view)
    operation_details TEXT NOT NULL          -- JSON-serialized full operation
);

-- Index for efficient chronological queries (newest first)
CREATE INDEX IF NOT EXISTS idx_proposal_history_applied_at
    ON proposal_history(applied_at DESC);

-- Index for log_index lookups (idempotency check and detail view)
CREATE INDEX IF NOT EXISTS idx_proposal_history_log_index
    ON proposal_history(log_index);
