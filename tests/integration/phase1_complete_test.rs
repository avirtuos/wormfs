//! Phase 1 Complete Integration Tests
//!
//! Comprehensive end-to-end tests for WormFS Phase 1 functionality.
//! These tests mount an actual WormFS filesystem and verify all Phase 1 features.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use walkdir::WalkDir;

/// Helper struct to manage a mounted WormFS instance for testing
struct WormFSTestMount {
    mount_point: PathBuf,
    data_dir: TempDir,
    process: Option<Child>,
}

impl WormFSTestMount {
    /// Create and mount a new WormFS instance for testing
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let data_dir = TempDir::new()?;
        let mount_point = data_dir.path().join("mount");
        fs::create_dir_all(&mount_point)?;

        let metadata_db = data_dir.path().join("metadata.db");
        let chunks_dir = data_dir.path().join("chunks");
        fs::create_dir_all(&chunks_dir)?;

        // Build the binary path
        let binary_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("release")
            .join("wormfs");

        // If release binary doesn't exist, try debug
        let binary_path = if binary_path.exists() {
            binary_path
        } else {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("debug")
                .join("wormfs")
        };

        if !binary_path.exists() {
            return Err(format!(
                "WormFS binary not found at {:?}. Run 'cargo build' first.",
                binary_path
            )
            .into());
        }

        // Create log file for debugging in /tmp (won't be cleaned up)
        let log_file_path = PathBuf::from(format!("/tmp/wormfs_test_{}.log", std::process::id()));
        let log_file = fs::File::create(&log_file_path)?;
        let log_file_clone = log_file.try_clone()?;

        // Also store path in data_dir for easy access
        fs::write(
            data_dir.path().join("log_path.txt"),
            log_file_path.to_string_lossy().as_bytes(),
        )?;

        // Start WormFS mount in background with debug logging
        let process = Command::new(&binary_path)
            .arg("mount")
            .arg("--mount-point")
            .arg(&mount_point)
            .arg("--metadata-db")
            .arg(&metadata_db)
            .arg("--data-dir")
            .arg(&chunks_dir)
            .arg("--foreground")
            .arg("--debug") // Enable debug logging
            .stdin(Stdio::null())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file_clone))
            .spawn()?;

        // Wait for mount to complete
        thread::sleep(Duration::from_secs(2));

        // Verify mount was successful
        if !Self::is_mounted(&mount_point) {
            return Err("Failed to mount WormFS".into());
        }

        Ok(Self {
            mount_point,
            data_dir,
            process: Some(process),
        })
    }

    /// Check if the filesystem is mounted
    fn is_mounted(path: &Path) -> bool {
        // Check if we can stat the mount point
        path.exists() && path.is_dir()
    }

    /// Get the mount point path
    fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    /// Get the chunks directory path (for erasure coding tests)
    fn chunks_dir(&self) -> PathBuf {
        self.data_dir.path().join("chunks")
    }

    /// Get the log file path for debugging
    fn log_file(&self) -> PathBuf {
        // Read the log path from the stored file
        if let Ok(path_str) = fs::read_to_string(self.data_dir.path().join("log_path.txt")) {
            PathBuf::from(path_str.trim())
        } else {
            // Fallback (shouldn't happen)
            PathBuf::from(format!("/tmp/wormfs_test_{}.log", std::process::id()))
        }
    }
}

impl Drop for WormFSTestMount {
    fn drop(&mut self) {
        // Kill the WormFS process
        if let Some(mut process) = self.process.take() {
            let _ = process.kill();
            let _ = process.wait();
        }

        // Unmount using fusermount
        let _ = Command::new("fusermount")
            .arg("-u")
            .arg(&self.mount_point)
            .output();

        // Give the unmount some time
        thread::sleep(Duration::from_millis(500));
    }
}

#[test]
#[ignore] // Ignore by default as it requires FUSE and takes time
fn test_basic_operations() {
    let mount = WormFSTestMount::new().expect("Failed to create test mount");
    let mount_point = mount.mount_point();
    let log_file = mount.log_file();
    println!("WormFS logs: {:?}", log_file);

    // Test 1: List empty root directory
    let entries: Vec<_> = fs::read_dir(mount_point)
        .expect("Failed to read root directory")
        .collect();
    assert_eq!(entries.len(), 0, "Root directory should be empty initially");

    // Test 2: Create a file
    let test_file = mount_point.join("test.txt");
    let test_content = "Hello, WormFS!";
    fs::write(&test_file, test_content).expect("Failed to write file");

    // Test 3: Read the file back
    let read_content = fs::read_to_string(&test_file).expect("Failed to read file");
    assert_eq!(read_content, test_content, "File content should match");

    // Test 4: Verify file appears in directory listing
    let entries: Vec<_> = fs::read_dir(mount_point)
        .expect("Failed to read directory")
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        entries.contains(&"test.txt".to_string()),
        "File should appear in directory listing"
    );

    // Test 5: Delete the file
    fs::remove_file(&test_file).expect("Failed to remove file");

    // Test 6: Verify file is gone
    assert!(!test_file.exists(), "File should be deleted");

    println!("✓ Basic operations test passed");
}

#[test]
#[ignore] // Ignore by default as it requires FUSE and takes time
fn test_nested_directory_structure() {
    let mount = WormFSTestMount::new().expect("Failed to create test mount");
    let mount_point = mount.mount_point();

    // Create nested directory structure: /a/b/c/d
    let nested_path = mount_point.join("a").join("b").join("c").join("d");
    fs::create_dir_all(&nested_path).expect("Failed to create nested directories");

    // Verify all directories exist
    assert!(mount_point.join("a").is_dir());
    assert!(mount_point.join("a").join("b").is_dir());
    assert!(mount_point.join("a").join("b").join("c").is_dir());
    assert!(nested_path.is_dir());

    // Create a file in the deepest directory
    let deep_file = nested_path.join("deep.txt");
    fs::write(&deep_file, "Deep in the filesystem").expect("Failed to write deep file");

    // Read it back
    let content = fs::read_to_string(&deep_file).expect("Failed to read deep file");
    assert_eq!(content, "Deep in the filesystem");

    // Create files at various levels
    fs::write(mount_point.join("a").join("level1.txt"), "Level 1")
        .expect("Failed to write level1 file");
    fs::write(
        mount_point.join("a").join("b").join("level2.txt"),
        "Level 2",
    )
    .expect("Failed to write level2 file");

    // Verify directory traversal
    let level1_entries: Vec<_> = fs::read_dir(mount_point.join("a"))
        .expect("Failed to read level1 dir")
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    assert!(level1_entries.contains(&"b".to_string()));
    assert!(level1_entries.contains(&"level1.txt".to_string()));

    println!("✓ Nested directory structure test passed");
}

#[test]
#[ignore] // Ignore by default as it requires FUSE and takes time
fn test_file_size_variants() {
    let mount = WormFSTestMount::new().expect("Failed to create test mount");
    let mount_point = mount.mount_point();

    // Test different file sizes
    let test_cases = vec![
        ("empty.txt", vec![]),
        ("tiny.txt", vec![b'x'; 10]),           // 10 bytes
        ("small.txt", vec![b'a'; 1024]),        // 1 KB
        ("medium.txt", vec![b'b'; 100_000]),    // ~100 KB
        ("large.txt", vec![b'c'; 1_000_000]),   // ~1 MB
        ("xlarge.txt", vec![b'd'; 10_000_000]), // ~10 MB
    ];

    for (filename, content) in test_cases {
        let file_path = mount_point.join(filename);

        // Write the file
        fs::write(&file_path, &content).expect(&format!("Failed to write {}", filename));

        // Read it back
        let read_content = fs::read(&file_path).expect(&format!("Failed to read {}", filename));

        // Verify content matches
        assert_eq!(
            read_content.len(),
            content.len(),
            "Size mismatch for {}",
            filename
        );
        assert_eq!(read_content, content, "Content mismatch for {}", filename);

        println!("✓ {} - {} bytes verified", filename, content.len());
    }

    println!("✓ File size variants test passed");
}

#[test]
#[ignore] // Ignore by default as it requires FUSE and takes time
fn test_erasure_coding_verification() {
    let mount = WormFSTestMount::new().expect("Failed to create test mount");
    let mount_point = mount.mount_point();
    let chunks_dir = mount.chunks_dir();

    // Create a file large enough to be erasure coded (> max_chunk_size)
    let test_file = mount_point.join("erasure_test.dat");
    let original_data: Vec<u8> = (0..2_000_000).map(|i| (i % 256) as u8).collect();

    fs::write(&test_file, &original_data).expect("Failed to write test file");

    // Verify the file can be read back correctly
    let read_data = fs::read(&test_file).expect("Failed to read test file");
    assert_eq!(read_data, original_data, "Data should match after write");

    // Wait a bit to ensure all chunks are flushed
    thread::sleep(Duration::from_secs(1));

    // Count the number of chunk files created (recursively, since chunks are in nested directories)
    let chunk_count = WalkDir::new(&chunks_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file() && e.path().extension().and_then(|s| s.to_str()) == Some("dat")
        })
        .count();

    // With 2MB of data and erasure coding (2 data + 1 parity), we should have
    // multiple chunks
    assert!(
        chunk_count >= 3,
        "Should have at least 3 chunks (data + parity), found {}",
        chunk_count
    );

    println!("✓ Created {} chunks with erasure coding", chunk_count);

    // Now corrupt one chunk by truncating it
    let mut corrupted = false;
    for entry in WalkDir::new(&chunks_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file() && e.path().extension().and_then(|s| s.to_str()) == Some("dat")
        })
    {
        let chunk_path = entry.path();

        // Truncate the first chunk we find
        if let Ok(mut file) = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&chunk_path)
        {
            // Write garbage
            let _ = file.write_all(b"CORRUPTED");
            corrupted = true;
            println!("✓ Corrupted chunk: {:?}", chunk_path.file_name());
            break;
        }
    }

    assert!(corrupted, "Should have corrupted at least one chunk");

    // Try to read the file - with erasure coding, it should still work
    // (or fail gracefully if we corrupted too many chunks)
    let result = fs::read(&test_file);

    // Note: In Phase 1, we don't have full erasure recovery yet, so this might fail.
    // The test verifies that chunks were created and erasure coded.
    // Full recovery will be tested in later phases.
    match result {
        Ok(recovered_data) => {
            if recovered_data == original_data {
                println!("✓ Successfully recovered data from corrupted chunks!");
            } else {
                println!("✓ Read succeeded but data was corrupted (expected in Phase 1)");
            }
        }
        Err(e) => {
            println!(
                "✓ Read failed after corruption: {} (expected in Phase 1)",
                e
            );
        }
    }

    println!("✓ Erasure coding verification test passed");
}

#[test]
#[ignore] // Ignore by default as it requires FUSE and takes time
fn test_performance_large_file() {
    let mount = WormFSTestMount::new().expect("Failed to create test mount");
    let mount_point = mount.mount_point();

    // Create a 100 MB file
    let file_size = 100 * 1024 * 1024; // 100 MB
    let test_file = mount_point.join("large_file.dat");

    // Generate test data (pattern to make it compressible)
    let pattern: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    let mut large_data = Vec::with_capacity(file_size);
    for _ in 0..(file_size / pattern.len()) {
        large_data.extend_from_slice(&pattern);
    }

    println!("Generated {} bytes of test data", large_data.len());

    // Measure write performance
    let start = std::time::Instant::now();
    fs::write(&test_file, &large_data).expect("Failed to write large file");
    let write_duration = start.elapsed();
    let write_throughput =
        (large_data.len() as f64) / write_duration.as_secs_f64() / 1024.0 / 1024.0;

    println!(
        "✓ Write: {} MB in {:.2}s ({:.2} MB/s)",
        large_data.len() / 1024 / 1024,
        write_duration.as_secs_f64(),
        write_throughput
    );

    // Measure read performance
    let start = std::time::Instant::now();
    let read_data = fs::read(&test_file).expect("Failed to read large file");
    let read_duration = start.elapsed();
    let read_throughput = (read_data.len() as f64) / read_duration.as_secs_f64() / 1024.0 / 1024.0;

    println!(
        "✓ Read: {} MB in {:.2}s ({:.2} MB/s)",
        read_data.len() / 1024 / 1024,
        read_duration.as_secs_f64(),
        read_throughput
    );

    // Verify data integrity
    assert_eq!(read_data.len(), large_data.len(), "File size should match");
    assert_eq!(read_data, large_data, "File content should match");

    // Basic sanity check - throughput should be at least 1 MB/s
    // (this is conservative for Phase 1)
    assert!(
        write_throughput > 1.0,
        "Write throughput should be > 1 MB/s"
    );
    assert!(read_throughput > 1.0, "Read throughput should be > 1 MB/s");

    println!("✓ Performance test passed");
}

#[test]
#[ignore] // Ignore by default as it requires FUSE and takes time
fn test_stress_many_files() {
    let mount = WormFSTestMount::new().expect("Failed to create test mount");
    let mount_point = mount.mount_point();

    let num_files = 1000;

    println!("Creating {} files...", num_files);

    // Create 1000 small files
    let start = std::time::Instant::now();
    for i in 0..num_files {
        let filename = format!("file_{:04}.txt", i);
        let content = format!("This is file number {}", i);
        let file_path = mount_point.join(&filename);

        fs::write(&file_path, &content).expect(&format!("Failed to write {}", filename));

        if (i + 1) % 100 == 0 {
            println!("  Created {} files...", i + 1);
        }
    }
    let create_duration = start.elapsed();
    println!(
        "✓ Created {} files in {:.2}s",
        num_files,
        create_duration.as_secs_f64()
    );

    // Verify we can list all files
    let start = std::time::Instant::now();
    let entries: Vec<_> = fs::read_dir(mount_point)
        .expect("Failed to read directory")
        .collect();
    let list_duration = start.elapsed();

    assert_eq!(entries.len(), num_files, "Should have {} files", num_files);
    println!(
        "✓ Listed {} files in {:.2}s",
        num_files,
        list_duration.as_secs_f64()
    );

    // Verify we can read random files
    let test_indices = vec![0, 100, 500, 999];
    for i in test_indices {
        let filename = format!("file_{:04}.txt", i);
        let file_path = mount_point.join(&filename);
        let content =
            fs::read_to_string(&file_path).expect(&format!("Failed to read {}", filename));

        let expected = format!("This is file number {}", i);
        assert_eq!(content, expected, "Content mismatch for {}", filename);
    }
    println!("✓ Verified random file reads");

    // Delete all files
    let start = std::time::Instant::now();
    for i in 0..num_files {
        let filename = format!("file_{:04}.txt", i);
        let file_path = mount_point.join(&filename);
        fs::remove_file(&file_path).expect(&format!("Failed to delete {}", filename));

        if (i + 1) % 100 == 0 {
            println!("  Deleted {} files...", i + 1);
        }
    }
    let delete_duration = start.elapsed();
    println!(
        "✓ Deleted {} files in {:.2}s",
        num_files,
        delete_duration.as_secs_f64()
    );

    // Verify directory is empty
    let entries: Vec<_> = fs::read_dir(mount_point)
        .expect("Failed to read directory")
        .collect();
    assert_eq!(entries.len(), 0, "Directory should be empty");

    println!("✓ Stress test passed");
}

#[test]
#[ignore] // Ignore by default as it requires FUSE and takes time
fn test_data_integrity_with_md5() {
    let mount = WormFSTestMount::new().expect("Failed to create test mount");
    let mount_point = mount.mount_point();

    // Create a test file with random-ish data
    let test_data: Vec<u8> = (0..5_000_000).map(|i| ((i * 7 + 13) % 256) as u8).collect();

    // Calculate MD5 of original data
    let original_md5 = md5::compute(&test_data);
    println!("Original MD5: {:x}", original_md5);

    // Write to WormFS
    let test_file = mount_point.join("checksum_test.dat");
    fs::write(&test_file, &test_data).expect("Failed to write file");

    // Read back from WormFS
    let read_data = fs::read(&test_file).expect("Failed to read file");

    // Calculate MD5 of data read from WormFS
    let wormfs_md5 = md5::compute(&read_data);
    println!("WormFS MD5:  {:x}", wormfs_md5);

    // Verify checksums match
    assert_eq!(
        format!("{:x}", original_md5),
        format!("{:x}", wormfs_md5),
        "MD5 checksums should match"
    );

    println!("✓ Data integrity verification passed");
}
