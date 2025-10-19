//! Utility for building stripes incrementally before committing to storage.
//!
//! StripeBuilder allows buffering of stripe data with a pre-allocated StripeId,
//! enabling metadata references to be created before the stripe is actually written.
//! This is essential for the BufferedFileHandle design where metadata and data
//! changes are kept together until flush.

use super::types::{Error, FileId, StoragePolicy, StripeId};
use std::sync::Arc;
use std::time::Instant;

/// Utility for building stripes incrementally before committing to storage.
///
/// Key features:
/// - Pre-allocates StripeId on construction
/// - Buffers stripe data without computing parity
/// - Stores StoragePolicy for later use
/// - Can be "built" into actual Stripe via FileStore
///
/// # Example
///
/// ```no_run
/// use wormfs::file_store::stripe_builder::StripeBuilder;
/// use wormfs::file_store::types::{FileId, StoragePolicy, CompressionAlgorithm};
/// use std::sync::Arc;
/// use uuid::Uuid;
///
/// let file_id = FileId::new(Uuid::new_v4());
/// let policy = Arc::new(StoragePolicy::new(2, 1, 1024 * 1024, CompressionAlgorithm::None));
/// let mut builder = StripeBuilder::new(file_id, 0, 0, 4 * 1024 * 1024, policy);
///
/// // Get the pre-allocated stripe ID to use in metadata
/// let stripe_id = builder.stripe_id();
///
/// // Append data incrementally
/// let data = vec![0u8; 1024];
/// builder.append(&data).expect("Failed to append data");
///
/// // Later, flush to FileStore using the builder's data and pre-allocated ID
/// ```
#[derive(Debug, Clone)]
pub struct StripeBuilder {
    /// Pre-allocated stripe ID that will be used in FileStore
    stripe_id: StripeId,

    /// File this stripe belongs to
    file_id: FileId,

    /// Stripe index within file
    stripe_index: u32,

    /// Byte offset in file where this stripe starts
    stripe_offset: u64,

    /// Buffered data (not yet erasure-coded)
    data: Vec<u8>,

    /// Maximum capacity for this stripe
    max_size: usize,

    /// Storage policy for this stripe
    policy: Arc<StoragePolicy>,

    /// When this builder was created
    created_at: Instant,
}

impl StripeBuilder {
    /// Create a new stripe builder with pre-allocated ID.
    ///
    /// # Arguments
    ///
    /// * `file_id` - File this stripe belongs to
    /// * `stripe_index` - Stripe index within the file
    /// * `stripe_offset` - Byte offset in file where this stripe starts
    /// * `max_size` - Maximum size of buffered data (typically stripe_size)
    /// * `policy` - Storage policy for this stripe
    ///
    /// # Returns
    ///
    /// A new StripeBuilder with a pre-allocated StripeId.
    pub fn new(
        file_id: FileId,
        stripe_index: u32,
        stripe_offset: u64,
        max_size: usize,
        policy: Arc<StoragePolicy>,
    ) -> Self {
        Self {
            stripe_id: StripeId::generate(), // Pre-allocated!
            file_id,
            stripe_index,
            stripe_offset,
            data: Vec::with_capacity(max_size),
            max_size,
            policy,
            created_at: Instant::now(),
        }
    }

    /// Append data to this stripe (up to max_size).
    ///
    /// Attempts to append as much data as possible without exceeding the
    /// stripe's maximum size. Returns the number of bytes actually written.
    ///
    /// # Arguments
    ///
    /// * `data` - Data to append to the stripe buffer
    ///
    /// # Returns
    ///
    /// The number of bytes successfully appended (may be less than data.len()
    /// if capacity is reached).
    ///
    /// # Errors
    ///
    /// Currently always returns Ok, but signature allows for future validation.
    pub fn append(&mut self, data: &[u8]) -> Result<usize, Error> {
        let available = self.remaining_capacity();
        let to_write = data.len().min(available);
        self.data.extend_from_slice(&data[..to_write]);
        Ok(to_write)
    }

    /// Overwrite data at a specific offset within the stripe.
    ///
    /// This allows modifying existing data in the stripe buffer without creating
    /// a new builder. The write can extend past the current end of the buffer,
    /// in which case zeros are inserted to fill the gap.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset within the stripe where overwrite begins (0-based)
    /// * `data` - Data to write at the offset
    ///
    /// # Returns
    ///
    /// The number of bytes successfully written (may be less than data.len()
    /// if max_size would be exceeded).
    ///
    /// # Errors
    ///
    /// Currently always returns Ok, but signature allows for future validation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use wormfs::file_store::StripeBuilder;
    /// # use wormfs::file_store::types::*;
    /// # use std::sync::Arc;
    /// # use uuid::Uuid;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let file_id = FileId::new(Uuid::new_v4());
    /// # let storage_policy = Arc::new(StoragePolicy::new(2, 1, 2 * 1024 * 1024, CompressionAlgorithm::None));
    /// let mut builder = StripeBuilder::new(file_id, 0, 0, 4 * 1024 * 1024, storage_policy);
    ///
    /// // Write initial data
    /// builder.append(&vec![0xAA; 1024])?;
    ///
    /// // Overwrite 512 bytes starting at offset 256
    /// builder.overwrite(256, &vec![0xBB; 512])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn overwrite(&mut self, offset: usize, data: &[u8]) -> Result<usize, Error> {
        // Can't write past max_size
        if offset >= self.max_size {
            return Ok(0);
        }

        // Calculate how much we can actually write
        let available = self.max_size - offset;
        let to_write = data.len().min(available);

        // If offset is past current end, extend with zeros
        if offset > self.data.len() {
            self.data.resize(offset, 0);
        }

        // Calculate the end position after write
        let end_pos = offset + to_write;

        // If we're writing past the current end, extend the buffer
        if end_pos > self.data.len() {
            self.data.resize(end_pos, 0);
        }

        // Overwrite the data
        self.data[offset..end_pos].copy_from_slice(&data[..to_write]);

        Ok(to_write)
    }

    /// Get the pre-allocated stripe ID.
    ///
    /// This ID can be used in metadata references before the stripe is
    /// actually written to storage.
    pub fn stripe_id(&self) -> StripeId {
        self.stripe_id
    }

    /// Get the file ID this stripe belongs to.
    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    /// Get the stripe index within the file.
    pub fn stripe_index(&self) -> u32 {
        self.stripe_index
    }

    /// Get the byte offset in the file where this stripe starts.
    pub fn stripe_offset(&self) -> u64 {
        self.stripe_offset
    }

    /// Get current data size.
    ///
    /// Returns the number of bytes currently buffered in this stripe.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Get the buffered data as a slice.
    ///
    /// This allows reading the data without transferring ownership.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Consume the builder and return the buffered data.
    ///
    /// This transfers ownership of the data Vec, useful when writing
    /// the stripe to FileStore.
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    /// Get the storage policy for this stripe.
    pub fn policy(&self) -> Arc<StoragePolicy> {
        Arc::clone(&self.policy)
    }

    /// Get memory footprint in bytes.
    ///
    /// Returns the actual memory allocated for the data buffer, which may
    /// be larger than the current data size due to Vec capacity.
    pub fn memory_bytes(&self) -> usize {
        self.data.capacity()
    }

    /// Get the maximum size this stripe can hold.
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Get remaining capacity before stripe is full.
    ///
    /// Returns the number of additional bytes that can be appended.
    pub fn remaining_capacity(&self) -> usize {
        self.max_size.saturating_sub(self.data.len())
    }

    /// Check if the stripe buffer is full.
    pub fn is_full(&self) -> bool {
        self.data.len() >= self.max_size
    }

    /// Check if the stripe buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the age of this builder (time since creation).
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Clear all buffered data.
    ///
    /// Resets the data buffer while keeping the pre-allocated StripeId
    /// and other metadata. Useful for reusing builders.
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_store::types::CompressionAlgorithm;
    use uuid::Uuid;

    fn create_test_policy() -> Arc<StoragePolicy> {
        Arc::new(StoragePolicy::new(
            2,           // data_shards
            1,           // parity_shards
            1024 * 1024, // chunk_size (1MB)
            CompressionAlgorithm::None,
        ))
    }

    #[test]
    fn test_stripe_builder_new() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();
        let builder = StripeBuilder::new(file_id, 0, 0, 4 * 1024 * 1024, policy.clone());

        assert_eq!(builder.file_id(), file_id);
        assert_eq!(builder.stripe_index(), 0);
        assert_eq!(builder.stripe_offset(), 0);
        assert_eq!(builder.size(), 0);
        assert_eq!(builder.max_size(), 4 * 1024 * 1024);
        assert_eq!(builder.remaining_capacity(), 4 * 1024 * 1024);
        assert!(builder.is_empty());
        assert!(!builder.is_full());
    }

    #[test]
    fn test_stripe_id_pre_allocated() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();
        let builder = StripeBuilder::new(file_id, 0, 0, 4 * 1024 * 1024, policy);

        // Verify that stripe_id is allocated and consistent
        let id1 = builder.stripe_id();
        let id2 = builder.stripe_id();
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_append_data() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();
        let mut builder = StripeBuilder::new(file_id, 0, 0, 1024, policy);

        // Append some data
        let data = vec![0xAA; 512];
        let written = builder.append(&data).expect("Failed to append");

        assert_eq!(written, 512);
        assert_eq!(builder.size(), 512);
        assert_eq!(builder.remaining_capacity(), 512);
        assert!(!builder.is_empty());
        assert!(!builder.is_full());
    }

    #[test]
    fn test_append_exceeds_capacity() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();
        let mut builder = StripeBuilder::new(file_id, 0, 0, 1024, policy);

        // Try to append more data than capacity
        let data = vec![0xBB; 2048];
        let written = builder.append(&data).expect("Failed to append");

        // Should only write up to capacity
        assert_eq!(written, 1024);
        assert_eq!(builder.size(), 1024);
        assert_eq!(builder.remaining_capacity(), 0);
        assert!(builder.is_full());
    }

    #[test]
    fn test_multiple_appends() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();
        let mut builder = StripeBuilder::new(file_id, 0, 0, 1024, policy);

        // Append multiple times
        builder.append(&vec![0x11; 256]).expect("Failed to append");
        builder.append(&vec![0x22; 256]).expect("Failed to append");
        builder.append(&vec![0x33; 256]).expect("Failed to append");

        assert_eq!(builder.size(), 768);
        assert_eq!(builder.remaining_capacity(), 256);

        // Verify data integrity
        let data = builder.data();
        assert_eq!(&data[0..256], &vec![0x11; 256][..]);
        assert_eq!(&data[256..512], &vec![0x22; 256][..]);
        assert_eq!(&data[512..768], &vec![0x33; 256][..]);
    }

    #[test]
    fn test_memory_accounting() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();
        let builder = StripeBuilder::new(file_id, 0, 0, 4 * 1024 * 1024, policy);

        // Memory should be allocated based on capacity
        assert_eq!(builder.memory_bytes(), 4 * 1024 * 1024);
    }

    #[test]
    fn test_into_data() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();
        let mut builder = StripeBuilder::new(file_id, 0, 0, 1024, policy);

        builder.append(&vec![0xCC; 512]).expect("Failed to append");

        let data = builder.into_data();
        assert_eq!(data.len(), 512);
        assert_eq!(data[0], 0xCC);
    }

    #[test]
    fn test_clear() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();
        let mut builder = StripeBuilder::new(file_id, 0, 0, 1024, policy);

        builder.append(&vec![0xDD; 512]).expect("Failed to append");
        assert_eq!(builder.size(), 512);

        let original_id = builder.stripe_id();

        builder.clear();

        assert_eq!(builder.size(), 0);
        assert!(builder.is_empty());
        assert_eq!(builder.remaining_capacity(), 1024);
        // StripeId should remain the same
        assert_eq!(builder.stripe_id(), original_id);
    }

    #[test]
    fn test_age() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();
        let builder = StripeBuilder::new(file_id, 0, 0, 1024, policy);

        std::thread::sleep(std::time::Duration::from_millis(10));

        let age = builder.age();
        assert!(age >= std::time::Duration::from_millis(10));
    }

    #[test]
    fn test_getters() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();
        let builder = StripeBuilder::new(file_id, 5, 20480, 1024, policy.clone());

        assert_eq!(builder.file_id(), file_id);
        assert_eq!(builder.stripe_index(), 5);
        assert_eq!(builder.stripe_offset(), 20480);
        assert_eq!(builder.max_size(), 1024);

        let retrieved_policy = builder.policy();
        assert_eq!(retrieved_policy.data_shards, policy.data_shards);
        assert_eq!(retrieved_policy.parity_shards, policy.parity_shards);
    }

    #[test]
    fn test_unique_stripe_ids() {
        let file_id = FileId::new(Uuid::new_v4());
        let policy = create_test_policy();

        let builder1 = StripeBuilder::new(file_id, 0, 0, 1024, policy.clone());
        let builder2 = StripeBuilder::new(file_id, 1, 1024, 1024, policy);

        // Each builder should have a unique stripe ID
        assert_ne!(builder1.stripe_id(), builder2.stripe_id());
    }
}
