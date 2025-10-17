//! POSIX permission checking for filesystem operations.
//!
//! This module implements standard POSIX permission semantics for file access control.
//! It follows the traditional UNIX permission model with owner, group, and other classes.
//!
//! # Permission Model
//!
//! Each file has:
//! - An owner user ID (uid)
//! - An owner group ID (gid)
//! - A permission mode (9 bits: rwxrwxrwx for owner, group, other)
//!
//! # Access Algorithm
//!
//! When checking permissions, the algorithm follows this precedence:
//! 1. If the requesting uid matches the file's uid, use **owner** permissions
//! 2. Else if the requesting gid matches the file's gid, use **group** permissions
//! 3. Otherwise, use **other** permissions
//!
//! **Important**: Only one permission class is ever checked. Owner takes precedence over
//! group, which takes precedence over other. This means if you own a file with mode
//! `---rwxrwx`, you cannot access it even though group and other have full permissions.
//!
//! # Examples
//!
//! ```rust,ignore
//! use wormfs::filesystem_service::permissions::{check_permission, Permission};
//!
//! // File owned by uid=1000, gid=1000, mode=0o644 (rw-r--r--)
//! let file_uid = 1000;
//! let file_gid = 1000;
//! let file_mode = 0o644;
//!
//! // Owner can read and write
//! assert!(check_permission(1000, 1000, file_uid, file_gid, file_mode, Permission::Read).is_ok());
//! assert!(check_permission(1000, 1000, file_uid, file_gid, file_mode, Permission::Write).is_ok());
//!
//! // Group can only read
//! assert!(check_permission(1001, 1000, file_uid, file_gid, file_mode, Permission::Read).is_ok());
//! assert!(check_permission(1001, 1000, file_uid, file_gid, file_mode, Permission::Write).is_err());
//!
//! // Others can only read
//! assert!(check_permission(1001, 1001, file_uid, file_gid, file_mode, Permission::Read).is_ok());
//! assert!(check_permission(1001, 1001, file_uid, file_gid, file_mode, Permission::Write).is_err());
//! ```

use crate::filesystem_service::Error;

/// Type of permission being requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    /// Read permission (r)
    Read,
    /// Write permission (w)
    Write,
    /// Execute permission (x)
    Execute,
}

impl Permission {
    /// Get the bit position for this permission within a permission class (0-2).
    ///
    /// In POSIX mode bits, each class (owner/group/other) has 3 bits: rwx
    /// - Read: bit 2 (value 4)
    /// - Write: bit 1 (value 2)
    /// - Execute: bit 0 (value 1)
    fn bit_value(&self) -> u32 {
        match self {
            Permission::Read => 4,    // 0b100
            Permission::Write => 2,   // 0b010
            Permission::Execute => 1, // 0b001
        }
    }
}

/// Permission class determines which 3-bit section of the mode to check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermissionClass {
    /// Owner permissions (bits 6-8)
    Owner,
    /// Group permissions (bits 3-5)
    Group,
    /// Other permissions (bits 0-2)
    Other,
}

impl PermissionClass {
    /// Get the bit shift for this permission class.
    ///
    /// Mode bits are arranged as: `0o_UUU_GGG_OOO` where:
    /// - UUU: owner (user) permissions, bits 6-8
    /// - GGG: group permissions, bits 3-5
    /// - OOO: other permissions, bits 0-2
    fn bit_shift(&self) -> u32 {
        match self {
            PermissionClass::Owner => 6, // bits 6-8
            PermissionClass::Group => 3, // bits 3-5
            PermissionClass::Other => 0, // bits 0-2
        }
    }
}

/// Determine which permission class to use based on POSIX precedence rules.
///
/// # Arguments
///
/// * `req_uid` - User ID of the requesting process
/// * `req_gid` - Group ID of the requesting process
/// * `file_uid` - Owner user ID of the file
/// * `file_gid` - Owner group ID of the file
///
/// # Returns
///
/// The permission class to use for access checking.
fn determine_permission_class(
    req_uid: u32,
    req_gid: u32,
    file_uid: u32,
    file_gid: u32,
) -> PermissionClass {
    if req_uid == file_uid {
        // Requesting user is the owner
        PermissionClass::Owner
    } else if req_gid == file_gid {
        // Requesting user's group matches file's group
        PermissionClass::Group
    } else {
        // All other users
        PermissionClass::Other
    }
}

/// Check if a specific permission bit is set in the mode for the given class.
///
/// # Arguments
///
/// * `mode` - File permission mode (e.g., 0o644)
/// * `class` - Which permission class to check (owner/group/other)
/// * `permission` - Which permission to check (read/write/execute)
///
/// # Returns
///
/// `true` if the permission is granted, `false` otherwise.
fn has_permission(mode: u32, class: PermissionClass, permission: Permission) -> bool {
    let class_shift = class.bit_shift();
    let permission_bit = permission.bit_value();

    // Extract the 3-bit permission for this class and check the specific bit
    (mode >> class_shift) & permission_bit != 0
}

/// Check if the requesting user has the specified permission on a file.
///
/// This function implements the standard POSIX permission checking algorithm.
///
/// # Arguments
///
/// * `req_uid` - User ID of the requesting process
/// * `req_gid` - Group ID of the requesting process
/// * `file_uid` - Owner user ID of the file
/// * `file_gid` - Owner group ID of the file
/// * `file_mode` - File permission mode (e.g., 0o644)
/// * `permission` - The permission being requested (read/write/execute)
///
/// # Returns
///
/// * `Ok(())` if permission is granted
/// * `Err(Error::PermissionDenied)` if permission is denied
///
/// # Examples
///
/// ```rust,ignore
/// // Check if user 1001 in group 1000 can read a file owned by user 1000, group 1000, mode 0o644
/// check_permission(1001, 1000, 1000, 1000, 0o644, Permission::Read)?; // OK - group can read
/// check_permission(1001, 1000, 1000, 1000, 0o644, Permission::Write)?; // Error - group cannot write
/// ```
pub fn check_permission(
    req_uid: u32,
    req_gid: u32,
    file_uid: u32,
    file_gid: u32,
    file_mode: u32,
    permission: Permission,
) -> Result<(), Error> {
    // Determine which permission class applies
    let class = determine_permission_class(req_uid, req_gid, file_uid, file_gid);

    // Check if the permission is granted for that class
    if has_permission(file_mode, class, permission) {
        Ok(())
    } else {
        Err(Error::PermissionDenied(0)) // inode will be filled in by caller
    }
}

/// Check if the requesting user can read from a file.
///
/// Convenience wrapper around `check_permission` for read access.
pub fn check_read_permission(
    req_uid: u32,
    req_gid: u32,
    file_uid: u32,
    file_gid: u32,
    file_mode: u32,
) -> Result<(), Error> {
    check_permission(
        req_uid,
        req_gid,
        file_uid,
        file_gid,
        file_mode,
        Permission::Read,
    )
}

/// Check if the requesting user can write to a file.
///
/// Convenience wrapper around `check_permission` for write access.
pub fn check_write_permission(
    req_uid: u32,
    req_gid: u32,
    file_uid: u32,
    file_gid: u32,
    file_mode: u32,
) -> Result<(), Error> {
    check_permission(
        req_uid,
        req_gid,
        file_uid,
        file_gid,
        file_mode,
        Permission::Write,
    )
}

/// Check if the requesting user can execute a file.
///
/// Convenience wrapper around `check_permission` for execute access.
pub fn check_execute_permission(
    req_uid: u32,
    req_gid: u32,
    file_uid: u32,
    file_gid: u32,
    file_mode: u32,
) -> Result<(), Error> {
    check_permission(
        req_uid,
        req_gid,
        file_uid,
        file_gid,
        file_mode,
        Permission::Execute,
    )
}

/// Check if the requesting user has permission to modify file metadata.
///
/// Only the file owner can change permissions, ownership, etc.
///
/// # Arguments
///
/// * `req_uid` - User ID of the requesting process
/// * `file_uid` - Owner user ID of the file
/// * `inode` - Inode number (for error reporting)
///
/// # Returns
///
/// * `Ok(())` if the requesting user is the owner
/// * `Err(Error::PermissionDenied)` otherwise
pub fn check_owner_permission(req_uid: u32, file_uid: u32, inode: u64) -> Result<(), Error> {
    if req_uid == file_uid {
        Ok(())
    } else {
        Err(Error::PermissionDenied(inode))
    }
}

/// Check if the requesting user can delete a file from a directory.
///
/// To delete a file, the user needs write permission on the parent directory.
///
/// # Arguments
///
/// * `req_uid` - User ID of the requesting process
/// * `req_gid` - Group ID of the requesting process
/// * `dir_uid` - Owner user ID of the parent directory
/// * `dir_gid` - Owner group ID of the parent directory
/// * `dir_mode` - Permission mode of the parent directory
///
/// # Returns
///
/// * `Ok(())` if the user can write to the directory
/// * `Err(Error::PermissionDenied)` otherwise
pub fn check_unlink_permission(
    req_uid: u32,
    req_gid: u32,
    dir_uid: u32,
    dir_gid: u32,
    dir_mode: u32,
) -> Result<(), Error> {
    check_write_permission(req_uid, req_gid, dir_uid, dir_gid, dir_mode)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_class_determination() {
        // Owner match
        assert_eq!(
            determine_permission_class(1000, 1000, 1000, 1000),
            PermissionClass::Owner
        );

        // Group match (but not owner)
        assert_eq!(
            determine_permission_class(1001, 1000, 1000, 1000),
            PermissionClass::Group
        );

        // Other (neither owner nor group match)
        assert_eq!(
            determine_permission_class(1001, 1001, 1000, 1000),
            PermissionClass::Other
        );

        // Owner takes precedence over group
        assert_eq!(
            determine_permission_class(1000, 1000, 1000, 1000),
            PermissionClass::Owner
        );
    }

    #[test]
    fn test_has_permission_checks() {
        // Mode 0o644 = rw-r--r--
        // Owner: rw- (6), Group: r-- (4), Other: r-- (4)
        let mode = 0o644;

        // Owner can read and write
        assert!(has_permission(
            mode,
            PermissionClass::Owner,
            Permission::Read
        ));
        assert!(has_permission(
            mode,
            PermissionClass::Owner,
            Permission::Write
        ));
        assert!(!has_permission(
            mode,
            PermissionClass::Owner,
            Permission::Execute
        ));

        // Group can only read
        assert!(has_permission(
            mode,
            PermissionClass::Group,
            Permission::Read
        ));
        assert!(!has_permission(
            mode,
            PermissionClass::Group,
            Permission::Write
        ));
        assert!(!has_permission(
            mode,
            PermissionClass::Group,
            Permission::Execute
        ));

        // Other can only read
        assert!(has_permission(
            mode,
            PermissionClass::Other,
            Permission::Read
        ));
        assert!(!has_permission(
            mode,
            PermissionClass::Other,
            Permission::Write
        ));
        assert!(!has_permission(
            mode,
            PermissionClass::Other,
            Permission::Execute
        ));
    }

    #[test]
    fn test_check_permission_owner() {
        // File: uid=1000, gid=1000, mode=0o600 (rw-------)
        let file_uid = 1000;
        let file_gid = 1000;
        let file_mode = 0o600;

        // Owner can read and write
        assert!(
            check_permission(1000, 1000, file_uid, file_gid, file_mode, Permission::Read).is_ok()
        );
        assert!(
            check_permission(1000, 1000, file_uid, file_gid, file_mode, Permission::Write).is_ok()
        );
        assert!(check_permission(
            1000,
            1000,
            file_uid,
            file_gid,
            file_mode,
            Permission::Execute
        )
        .is_err());
    }

    #[test]
    fn test_check_permission_group() {
        // File: uid=1000, gid=1000, mode=0o640 (rw-r-----)
        let file_uid = 1000;
        let file_gid = 1000;
        let file_mode = 0o640;

        // Group member (uid=1001, gid=1000) can only read
        assert!(
            check_permission(1001, 1000, file_uid, file_gid, file_mode, Permission::Read).is_ok()
        );
        assert!(
            check_permission(1001, 1000, file_uid, file_gid, file_mode, Permission::Write).is_err()
        );
    }

    #[test]
    fn test_check_permission_other() {
        // File: uid=1000, gid=1000, mode=0o644 (rw-r--r--)
        let file_uid = 1000;
        let file_gid = 1000;
        let file_mode = 0o644;

        // Other user (uid=1001, gid=1001) can only read
        assert!(
            check_permission(1001, 1001, file_uid, file_gid, file_mode, Permission::Read).is_ok()
        );
        assert!(
            check_permission(1001, 1001, file_uid, file_gid, file_mode, Permission::Write).is_err()
        );
    }

    #[test]
    fn test_owner_precedence_over_group() {
        // File: uid=1000, gid=1000, mode=0o077 (---rwxrwx)
        // Owner has NO permissions, but group and other have full permissions
        let file_uid = 1000;
        let file_gid = 1000;
        let file_mode = 0o077;

        // Owner CANNOT read even though group can
        // This demonstrates POSIX precedence: owner permissions checked first
        assert!(
            check_permission(1000, 1000, file_uid, file_gid, file_mode, Permission::Read).is_err()
        );
        assert!(
            check_permission(1000, 1000, file_uid, file_gid, file_mode, Permission::Write).is_err()
        );

        // But group member CAN read
        assert!(
            check_permission(1001, 1000, file_uid, file_gid, file_mode, Permission::Read).is_ok()
        );
        assert!(
            check_permission(1001, 1000, file_uid, file_gid, file_mode, Permission::Write).is_ok()
        );
    }

    #[test]
    fn test_check_read_permission_wrapper() {
        // File: uid=1000, gid=1000, mode=0o644
        assert!(check_read_permission(1000, 1000, 1000, 1000, 0o644).is_ok());
        assert!(check_read_permission(1001, 1000, 1000, 1000, 0o644).is_ok());
        assert!(check_read_permission(1001, 1001, 1000, 1000, 0o644).is_ok());
    }

    #[test]
    fn test_check_write_permission_wrapper() {
        // File: uid=1000, gid=1000, mode=0o644
        assert!(check_write_permission(1000, 1000, 1000, 1000, 0o644).is_ok());
        assert!(check_write_permission(1001, 1000, 1000, 1000, 0o644).is_err());
        assert!(check_write_permission(1001, 1001, 1000, 1000, 0o644).is_err());
    }

    #[test]
    fn test_check_owner_permission_wrapper() {
        assert!(check_owner_permission(1000, 1000, 123).is_ok());
        assert!(check_owner_permission(1001, 1000, 123).is_err());
    }

    #[test]
    fn test_check_unlink_permission() {
        // Directory: uid=1000, gid=1000, mode=0o755 (rwxr-xr-x)
        // Owner can delete files (needs write on directory)
        assert!(check_unlink_permission(1000, 1000, 1000, 1000, 0o755).is_ok());

        // Non-owner cannot delete (no write permission on directory)
        assert!(check_unlink_permission(1001, 1001, 1000, 1000, 0o755).is_err());

        // Directory: uid=1000, gid=1000, mode=0o775 (rwxrwxr-x)
        // Group member CAN delete (has write on directory)
        assert!(check_unlink_permission(1001, 1000, 1000, 1000, 0o775).is_ok());
    }
}
