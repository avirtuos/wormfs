// Raft log storage implementation using redb
//
// This module implements OpenRaft's RaftLogStorage trait for persistent log storage.

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftLogReader, StorageError,
    StorageIOError, Vote,
};
use redb::{Database, ReadableTable, TableDefinition};
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use crate::raft::types::WormFSTypeConfig;

/// Table definitions for redb log storage
const LOG_ENTRIES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("log_entries");
const STORE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("store");

/// Log store implementation using redb
#[derive(Debug, Clone)]
pub struct LogStore {
    db: Arc<Database>,
}

impl LogStore {
    /// Create a new LogStore at the specified path
    #[allow(clippy::result_large_err)]
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StorageError<u64>> {
        std::fs::create_dir_all(&path).map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        let db_path = path.as_ref().join("raft_log.redb");
        let db = Database::create(&db_path).map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        // Initialize tables
        let write_txn = db.begin_write().map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        {
            write_txn
                .open_table(LOG_ENTRIES_TABLE)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::read(&e),
                })?;

            write_txn
                .open_table(STORE_TABLE)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::read(&e),
                })?;
        }

        write_txn.commit().map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        Ok(Self { db: Arc::new(db) })
    }

    #[allow(clippy::result_large_err)]
    fn flush(
        &self,
        _subject: ErrorSubject<u64>,
        _verb: ErrorVerb,
    ) -> Result<(), StorageIOError<u64>> {
        // redb doesn't have a flush_wal method like RocksDB, compaction is automatic
        // We can just return Ok as redb handles durability internally
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn get_last_purged(&self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let read_txn = self.db.begin_read().map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        let table = read_txn
            .open_table(STORE_TABLE)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?;

        Ok(table
            .get("last_purged_log_id")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?
            .and_then(|v| serde_json::from_slice(v.value()).ok()))
    }

    #[allow(clippy::result_large_err)]
    fn set_last_purged(&self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let write_txn = self.db.begin_write().map_err(|e| StorageError::IO {
            source: StorageIOError::write(&e),
        })?;

        {
            let mut table = write_txn
                .open_table(STORE_TABLE)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write(&e),
                })?;

            let data = serde_json::to_vec(&log_id).map_err(|e| StorageError::IO {
                source: StorageIOError::write(&e),
            })?;

            table
                .insert("last_purged_log_id", data.as_slice())
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write(&e),
                })?;
        }

        write_txn.commit().map_err(|e| StorageError::IO {
            source: StorageIOError::write(&e),
        })?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)
            .map_err(|source| StorageError::IO { source })?;
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn get_vote(&self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let read_txn = self.db.begin_read().map_err(|e| StorageError::IO {
            source: StorageIOError::read_vote(&e),
        })?;

        let table = read_txn
            .open_table(STORE_TABLE)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_vote(&e),
            })?;

        Ok(table
            .get("vote")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_vote(&e),
            })?
            .and_then(|v| serde_json::from_slice(v.value()).ok()))
    }

    #[allow(clippy::result_large_err)]
    fn set_vote(&self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let write_txn = self.db.begin_write().map_err(|e| StorageError::IO {
            source: StorageIOError::write_vote(&e),
        })?;

        {
            let mut table = write_txn
                .open_table(STORE_TABLE)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write_vote(&e),
                })?;

            let data = serde_json::to_vec(vote).map_err(|e| StorageError::IO {
                source: StorageIOError::write_vote(&e),
            })?;

            table
                .insert("vote", data.as_slice())
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write_vote(&e),
                })?;
        }

        write_txn.commit().map_err(|e| StorageError::IO {
            source: StorageIOError::write_vote(&e),
        })?;

        self.flush(ErrorSubject::Vote, ErrorVerb::Write)
            .map_err(|source| StorageError::IO { source })?;
        Ok(())
    }
}

impl RaftLogReader<WormFSTypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<WormFSTypeConfig>>, StorageError<u64>> {
        let read_txn = self.db.begin_read().map_err(|e| StorageError::IO {
            source: StorageIOError::read_logs(&e),
        })?;

        let table = read_txn
            .open_table(LOG_ENTRIES_TABLE)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(&e),
            })?;

        let mut entries = Vec::new();

        for result in table.range(range).map_err(|e| StorageError::IO {
            source: StorageIOError::read_logs(&e),
        })? {
            let (_index, data) = result.map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(&e),
            })?;

            let entry: Entry<WormFSTypeConfig> =
                serde_json::from_slice(data.value()).map_err(|e| StorageError::IO {
                    source: StorageIOError::read_logs(&e),
                })?;

            entries.push(entry);
        }

        Ok(entries)
    }
}

impl RaftLogStorage<WormFSTypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<WormFSTypeConfig>, StorageError<u64>> {
        let read_txn = self.db.begin_read().map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        let table = read_txn
            .open_table(LOG_ENTRIES_TABLE)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?;

        let last = table
            .iter()
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?
            .last()
            .and_then(|res| {
                let (_, ent) = res.ok()?;
                Some(
                    serde_json::from_slice::<Entry<WormFSTypeConfig>>(ent.value())
                        .ok()?
                        .log_id,
                )
            });

        let last_purged_log_id = self.get_last_purged()?;

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.set_vote(vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.get_vote()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<WormFSTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<WormFSTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let write_txn = self.db.begin_write().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(&e),
        })?;

        {
            let mut table =
                write_txn
                    .open_table(LOG_ENTRIES_TABLE)
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::write_logs(&e),
                    })?;

            for entry in entries {
                let index = entry.log_id.index;
                let data = serde_json::to_vec(&entry).map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(&e),
                })?;

                table
                    .insert(index, data.as_slice())
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::write_logs(&e),
                    })?;
            }
        }

        write_txn.commit().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(&e),
        })?;

        // Notify that logs are flushed
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let write_txn = self.db.begin_write().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(&e),
        })?;

        {
            let mut table =
                write_txn
                    .open_table(LOG_ENTRIES_TABLE)
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::write_logs(&e),
                    })?;

            // Remove all entries after log_id
            let keys_to_remove: Vec<u64> = table
                .range(log_id.index + 1..)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::read_logs(&e),
                })?
                .map(|r| r.map(|(k, _)| k.value()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::read_logs(&e),
                })?;

            for key in keys_to_remove {
                table.remove(key).map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(&e),
                })?;
            }
        }

        write_txn.commit().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(&e),
        })?;

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.set_last_purged(log_id)?;

        let write_txn = self.db.begin_write().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(&e),
        })?;

        {
            let mut table =
                write_txn
                    .open_table(LOG_ENTRIES_TABLE)
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::write_logs(&e),
                    })?;

            // Remove all entries up to and including log_id
            let keys_to_remove: Vec<u64> = table
                .range(..=log_id.index)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::read_logs(&e),
                })?
                .map(|r| r.map(|(k, _)| k.value()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::read_logs(&e),
                })?;

            for key in keys_to_remove {
                table.remove(key).map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(&e),
                })?;
            }
        }

        write_txn.commit().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(&e),
        })?;

        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, EntryPayload};
    use tempfile::TempDir;

    use crate::raft::types::MetadataOp;

    #[allow(dead_code)]
    fn create_test_entry(index: u64, leader_id: u64, op: MetadataOp) -> Entry<WormFSTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, leader_id), index),
            payload: EntryPayload::Normal(op),
        }
    }

    #[tokio::test]
    async fn test_log_store_creation() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = LogStore::new(temp_dir.path()).unwrap();

        let log_state = store.get_log_state().await.unwrap();
        assert_eq!(log_state.last_log_id, None);
        assert_eq!(log_state.last_purged_log_id, None);
    }

    // TODO: This test requires creating LogFlushed through the Raft API
    // For now, we test the storage through integration tests with the full Raft cluster
    // #[tokio::test]
    // async fn test_append_and_read_entries() {
    //     let temp_dir = TempDir::new().unwrap();
    //     let mut store = LogStore::new(temp_dir.path()).unwrap();
    //
    //     let op = MetadataOp::DeleteFile {
    //         path: "/test/file.txt".to_string(),
    //     };
    //
    //     let entries = vec![
    //         create_test_entry(1, 1, op.clone()),
    //         create_test_entry(2, 1, op.clone()),
    //     ];
    //
    //     // LogFlushed::new is private, need to test through Raft API
    //     // let (callback, _rx) = LogFlushed::new(None, None);
    //     // store.append(entries, callback).await.unwrap();
    //
    //     let log_state = store.get_log_state().await.unwrap();
    //     assert!(log_state.last_log_id.is_some());
    //
    //     let mut reader = store.get_log_reader().await;
    //     let read_entries = reader.try_get_log_entries(1..=2).await.unwrap();
    //     assert_eq!(read_entries.len(), 2);
    // }

    #[tokio::test]
    async fn test_vote_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = LogStore::new(temp_dir.path()).unwrap();

        let vote = Vote::new(5, 2);

        store.save_vote(&vote).await.unwrap();

        let read_vote = store.read_vote().await.unwrap();
        assert!(read_vote.is_some());
    }
}
