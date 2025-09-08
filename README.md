# WormFS

WormFS, short for write-once-read-many file system, is intended to be user-space file system that uses erasure encoding to spread files across multiple storage devices, each running their own commodity filesystems. This allows great flexibility with respect to configuring device failure tolerance at a file or directory level. I envision this being extremely useful for media storage and deep archive use-cases.

Much of the architecture of this project is inspired by lizardfs' simplicity with a goal of offering greater control and visibility over how chunks are stored, replicated, and recovered.

This project is a work in progress. It is not even remotely usable at this point but I plan to chip away at it in my free time using my openai subscription, cline, and opencode.  


## Features

- **Path Translation**: All filesystem operations are translated to a configurable target directory
- **Read-Only Access**: Currently supports read-only operations (read, readdir, getattr, lookup)
- **Simple Implementation**: Clean, straightforward FUSE implementation for learning and extension

## Architecture

- It is important that meta-data operations are fast because client applications will be accustomed to traditional (single disk) storage which has extremely fast meta-data operations (in most cases). I think we'll use something like redb for meta-data stored on each node with peer-to-peer replication but single-leader model. libp2p has much of this logic ready to go for us.
- We'll lean in on the WORM aspect of this implementation to greatly simplify everything. Its likely that we will have a two-tier system where writes initially happen against a single node then after a few minutes of inactivity the file will get chunked up and distributed to the storage mesh. I'm not 100% on this yet, i need to think about how to handle attempts to edit an existing file. It might need to be recalled to the single writer. This way aside from the write node(s) all the other nodes are dealing with immutable data and ensuring sufficient replica counts.

## Building

```bash
cargo build --release
```

## Usage

```bash
# Create a mount point
sudo mkdir /mnt/wormfs

# Mount the filesystem (replace /path/to/target with your desired target directory)
sudo ./target/release/wormfs /mnt/wormfs --target /path/to/target

# Access the filesystem
ls /mnt/wormfs
cat /mnt/wormfs/some-file.txt

# Unmount when done
sudo umount /mnt/wormfs
```

## Command Line Options

- `mount_point`: The directory where the FUSE filesystem will be mounted
- `--target, -t`: The target directory to translate operations to
- `--debug, -d`: Enable debug logging

## Example

```bash
# Mount your home directory through WormFS
sudo ./target/release/wormfs /mnt/wormfs --target /home/user

# Now /mnt/wormfs will show the contents of /home/user
ls /mnt/wormfs  # Shows contents of /home/user
```

## Current Limitations

- Read-only filesystem
- Simplified inode handling
- Basic path translation (no advanced features yet)

## Future Enhancements

This is the first step in a larger project. Future versions will include:
- Write operations support
- Advanced path mapping
- Network filesystem capabilities
- Caching and performance optimizations
