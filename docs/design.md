# WormFS

WormFS, short for write-once-read-many file system, is intended to be user-space file system that uses erasure encoding to spread files across multiple storage devices, each running their own commodity filesystems. This allows great flexibility with respect to configuring device failure tolerance at a file or directory level. I envision this being useful for media storage and deep archive use-cases. Conventional (old school) thinking was to simply use RAID with an off the shelf NAS device like synology or qnap. Unfortunately, when thees devices or raid arrays fail you lose everything. It would be nice if failures were more configurable. For example, I may choose to have some files that I care a lot about use more space to resist more disk failures. Other files, that I care less about may be configured with lower levels of redundancy. Its also helpful to support varying disk sizes mixed in an array while still being able to use all the space.

Much of the architecture of this project is inspired by lizardfs' simplicity with a goal of offering greater control and visibility over how chunks are stored, replicated, and recovered.

This project is a work in progress. It is not even remotely usable at this point but I plan to chip away at it in my free time using my openai subscription, cline, and opencode.  

## Key Terms

- **Stripe** - refers to a continuous range of bytes in a file. Stripe size is consistent for all Stripes in a file. Only the final Stripe of a file may have a size that is smaller than the configured Stripe size.
- **Chunk Metadata** - refers to the collection of meta-data stored in sqlite that authoritatively outlines which storage node houses a given chunk, which Stripe a chunk belongs to, and which File the Stripe belongs to. 
- **File Metadata** - refers to the collection of meta-data stored in sqlite that is required to satisfy basic FUSE filesystem metadata operations without having to read all Stripes and chunks. This includes file permissions, size, inode details, and path information.
- **Backing Storage** - refers to the traditional, non-distributed, filesystem that Storage Nodes use to durably store chunks.
- **Chunk Folder** - refers to the single folder that contains all chunks for a given file on a Storage Node. This ensures it is easy to find all the chunks for a given file in backing storage of a Storage Node.
- **Erasure Coding** - mathematical technique to provide redundancy by creating additional parity chunks from data chunks. Enables reconstruction of lost data from remaining chunks.
- **Data Shards** - the original chunks of a stripe that contain actual file data.
- **Parity Shards** - the redundant chunks created through erasure coding that enable recovery of lost data shards.
- **Storage Policy** - configuration that defines redundancy level (k+m), stripe size, and other storage parameters for files or directories.
- **Node Consensus** - the process by which storage nodes agree on metadata changes and cluster state.
- **Gossip Protocol** - peer-to-peer communication method used to propagate metadata updates across all storage nodes.

## Architecture

WormFS is deployed using two types of nodes: Client Nodes and Storage Nodes. There are clients which run as a FUSE filesystem and expose posix compliant read/write/meta-data operations for users to interact with. The client is only aware of "Stripes" so it knows that when it attempts to read data to satisfy a user read request, it may need to over scan by reading one or more Stripes even if the user requested less data. Similarly, when the user attempts to write data, the Client may need to read one or more Stripes that are write targets, apply the write locally to the Stripe(s) and then send the modified Stripes to the appropriate storage node. 

Storage Nodes serve two primary roles: meta-data operations and storage operations. Any storage node can serve meta-data operations like creating folders, listing files, or identifying which storage node houses a given Chunk of a Stripe of a File. Similarly, and storage node can facilitate Stripe read and write operations by using its view of Chunk Metadata to issue chunk read and write operations to the appropriate storage nodes in order to Store new/updated Stripes or read existing Stripes on behalf of user requests. 

### Raft-Based Consensus Architecture

WormFS uses Raft consensus (via OpenRaft) to ensure strong consistency of metadata operations across the cluster:

```
┌─────────────────────────────────────────────────────────────┐
│                    Storage Node Architecture                 │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              Raft Consensus Layer                     │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │  │
│  │  │  Leader  │  │ Follower │  │    Follower      │   │  │
│  │  │ Election │  │   Log    │  │   Replication    │   │  │
│  │  └──────────┘  └──────────┘  └──────────────────┘   │  │
│  │                                                        │  │
│  │  Transaction Log (redb) ─────┐                       │  │
│  │  • Append-only                │                       │  │
│  │  • Replicated to majority     │                       │  │
│  │  • Snapshots for compaction   │                       │  │
│  └────────────────────────────────│───────────────────────┘  │
│                                   │                           │
│                                   ▼ Apply committed ops      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Metadata State Machine (SQLite)              │  │
│  │  • File metadata (paths, permissions, size)          │  │
│  │  • Chunk locations (which node has which chunks)     │  │
│  │  • Stripe mappings (stripe → chunks)                 │  │
│  │  • Lock state (active read/write locks)              │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │            Chunk Storage (Local Disks)                │  │
│  │  • Direct libp2p transfers for chunk data             │  │
│  │  • Placement coordinated via Raft                     │  │
│  │  • Erasure-coded stripes                              │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

     Metadata ops (via Raft)    │    Chunk transfers (libp2p)
                                 │
                                 ▼
           ┌─────────────────────────────────────┐
           │    Inter-Node Communication         │
           │                                     │
           │  • Raft RPCs (AppendEntries, Vote) │
           │  • Direct chunk read/write streams  │
           │  • All over libp2p with encryption  │
           └─────────────────────────────────────┘
```

**Key Design Points:**

1. **Two-Tier Storage**: Raft log (redb) for replication, SQLite for materialized state
2. **Lease-Based Reads**: Leader serves reads directly with valid lease for performance
3. **Minority Partition Support**: Stale reads available in minority partitions
4. **Separate Data Plane**: Chunk data transfers bypass Raft for efficiency
5. **Strong Consistency**: All metadata writes require majority quorum

Since all Storage Nodes participate in the Raft cluster and maintain consistent views of metadata through log replication, any node can handle client requests (though writes are coordinated through the leader). Chunk read and write operations use direct libp2p streams between storage nodes to avoid network traffic amplification - only the chunk metadata operations go through Raft consensus.

Storage nodes will use the reed-solomon-erasure rust create to implement erasure encoding of Stripes of X bytes into k data chunks and m parity chunks. The stripe size, number of data chunks, and number of parity chunks should be configurable. Administrators can configure a global default for these settings as well as a per-directory override of the default. These should appear in the main storage node config file. It is important that these settings match across all storage nodes to ensure you achieve your intended durability goals.

Clients will interact with Storage Nodes via gRPC for filesystem meta-data and Stripe read/write operations.

### Network Communication & Discovery

**Node Discovery:** Storage nodes need to discover each other and maintain cluster membership. This will be accomplished by having a "peer ips" list in the storage node config file. This list will be used to drive the libp2p swarm setup. When clients connect to the filesystem they should need to know only 1 node as they will pull the full node list and cache it in a config file locally for use in future connections as well as after re-starts. Each peer ips should also have a peer id that can be used to validate the libp2p public key of the node so that we can use libp2p's transport encryption features for security and authentication of peers. In addition to explicit peer ids, the config should also support "auto_id" where by the node will accept and store the peer id associated with a configured ip the first time it encounters the peer. This discovered id will be durably stored on the node and enforced across reboots so that if a remote peer's id changes, connections will be rejected. This balances ease of use with robust security controls.

**Communication Protocols:**
- **Client ↔ Storage Node:** gRPC for metadata operations and stripe read/write requests
- **Storage Node ↔ Storage Node:** libp2p for both gossip-based metadata propagation and direct chunk transfer
- **Administrative Interface:** WebUI with REST API for cluster management, monitoring, and configuration.

**Security Considerations:** All inter-node communication should be authenticated and encrypted to prevent unauthorized access and ensure data integrity. Inter-StorageNode communication should be protected using libp2ps built in public/private key controls. Client to Storage Node communication should be protected using TLS 1.3 w/PSK. The PSK should be user specific so that it can be used to establish identity. This likely means having a directory of user identity files on the Storage Nodes so that we can support multiple users and eventually unique permissions for those users.

### Data Consistency & Concurrency

WormFS takes a simple approach to consistency and concurrency by using basic ReadWriteLock semantics. When a client opens a file for read, it obtains a Read Lock for the file. When a client opens a file for writing, it obtains a Write Lock on the file. These locks expire after 10 seconds if the client does not make a periodic call to 'extend' their ownership of the lock while they have the file open. Concurrent read locks for a single file are allowed but write locks are exclusive of other writes or reads. Read locks can optionally be ignored (client behave as if they succeed) via Storage Node config but write locks can not be disabled. Write locks will ignore read locks if read locks are set to ignore, allowing writers to overwrite files that are being read by others.

**Metadata Consistency:** WormFS uses the Raft consensus protocol (via OpenRaft) to ensure strong consistency for all metadata operations. Raft provides automatic leader election, replicated log management, and guaranteed linearizability for metadata writes. All metadata operations (file creation, chunk location updates, locks, etc.) are proposed to the Raft leader, which replicates them to a majority of nodes before committing. Once committed, operations are applied to each node's local SQLite database, ensuring all nodes have consistent views of metadata.

**Storage Architecture:** WormFS uses a two-tier storage approach for metadata:
- **Transaction Log (redb):** Stores the Raft log containing all metadata operations. This is the source of truth for replication and recovery.
- **State Database (SQLite):** Stores the materialized metadata state after log entries are applied. This is optimized for fast queries and filesystem operations.

**Read Optimization:** For performance, WormFS supports lease-based reads where the Raft leader can serve read requests directly without going through consensus, as long as it maintains a valid lease with the majority of nodes. In network partitions, minority partitions can serve stale reads (marked as potentially stale), while only the majority partition can accept writes.

**Snapshot Management:** To prevent unbounded log growth, the system periodically creates snapshots of the SQLite state. Snapshots are triggered either by time (default: 24 hours) or by log size (default: 10MB), both configurable. New nodes joining the cluster receive a snapshot followed by log replay to catch up.

### Failure Scenarios & Recovery

**Node Failure Detection:** Raft automatically detects node failures through its heartbeat mechanism. The Raft leader sends periodic heartbeats (default: 250ms) to all followers. If a follower doesn't receive a heartbeat within the election timeout window (default: 1-2 seconds), it triggers a new leader election. Data of offline nodes is not automatically rebuilt. An administrator must run a command to manually delete offline nodes before peers will attempt to rebuild the missing chunks, placing them on other nodes. Ideally this action can be taken via the web ui but a command line should also be available.

**Data Scrubbing and Recovery:** The Raft Leader runs two background anti-entropy tasks that look for damaged files and attempts to repair them. When a new node becomes the leader, it will become responsible for running these tasks. The first task is a shallow check that confirms all chunks are accessible for each file stored in the system. This check needs to be cheap because we want to check all chunks quickly so we can detect missing chunks shortly after they are lost and before we lose more chunks for the same file, potentially limiting our ability to rebuild the missing chunk. Storage nodes will implement a "check_chunk" API that will do a cheap file stat to ensure the requested chunk is present. The second task is a deeper check which validates every Stripe stored in the system by first reading each of its chunks and confirming the integrity of each chunk by validating the checksum against the chunk contents. It then reconstructs the Stripe and validates the integrity of the Stripe by asserting the Strip checksum against the Stripe's content. If either of these operations identifies a missing or corrupt chunk, it schedules the corresponding Stripe for a rebuild. This is where a third background task comes into play which attempts to rebuild corrupt Stripes from the available Chunks and then re-writes all Stripe chunks.

**Split-Brain Prevention:** Raft's consensus protocol inherently prevents split-brain scenarios. The protocol requires a majority quorum for both leader election and log commitment. This ensures that only one network partition (the one with the majority of nodes) can elect a leader and accept writes. Minority partitions can only serve stale reads and cannot make progress until they rejoin the majority partition.

### Storage Node - Storage Design

- All meta-data should be stored in sqlite for fast access and durability across node restarts.
- wormfs storage nodes will use a traditional ext4 filesystem to store chunks as files.
- All chunks for a file stored in given wormfs storage node should exist within a single "chunk folder".
- In addition to writing chunks to the backing storage, each "Chunk Folder" should have an index file that contains basic details of the user file these chunks belong to as well as statistics about the chunks on that storage node. We'll use this file both in aiding rebuilding chunk meta-data in the event of sqlite corruption but also to power some of our operators tools that perform periodic reconciliation of chunk storage.
- Chunk Folders should be named using a 10 character alphanumeric hash of the fully qualified actual user file name and path.
- Chunk Folders should be hashed into one of a 1000 top level storage folders that are simply named 1 to 1000.
- Chunk placement can be simple for now, following two basic rules. A disk is not allowed to store more than 1 Chunk per Stripe in order to limit blast radius. Storage Nodes can have multiple disks. Chunks placement should favor disks with the most free space but not violate the prior blast radius rule that limits how many chunks of a Stripe can be put ona  single disk.

### Chunk File Layout

Every chunk file should begin with simple binary header that starts with chunk checksum (of everything in the file that follows in the chunk file) but the head should also include the chunk id, stripe id, file id, stripe start and end byte offset in the file, erasure details (e.g. chunk index, # data shards, # parity shards, stripe data checksum). The header should be variable length. The header should also include information about the chunk format version, compression algo, and erasure algo (e.g. reed-solomon or none). After the header, the chunk data follows.

We will only support reed-solomon erasure encoding and CRC32 for checksums. We will use the reed-solomon-erasure rust crate.


## Internal File Operation Semantics

- Breaks files into stripes which are broken into chunks for erasure coding.
- When a read operation takes place, the Stripe containing the bytes that were requested are rehydrated from chunks as needed so the read can take place.
- When a write operation takes place against a position in an already existing Strip of a file, the entire Stripe contained the targeted bytes is rehydrated from chunks so the write can take place.
- Stripe size should be configurable but the config should only apply to newly written Stripes any existing Stripe would have it size encoded in its meta-data and be unaffected by changes to this config after the Stripe is written.

## Configuration Management

**Storage Policies:** Users need to configure redundancy levels, stripe sizes, and other storage parameters at the file or directory level. This should support inheritance where subdirectories adopt parent policies unless explicitly overridden. Storage policies should be specified via config file on each Storage Node.

**Policy Specification Format:** Define a clear format for specifying storage policies (e.g., YAML, JSON) that can be applied via extended attributes, configuration files, or API calls.

**Dynamic Reconfiguration:** The system will not initially support changing storage policies for existing files as that potentially requires data migration and re-encoding.

**Default Policies:** Establish sensible defaults for different use cases (archival, media storage, general purpose).

## API Specifications

### Client-Storage gRPC API

**Filesystem Operations:**
- These APIs are dictated by the FUSE filesystem API spec.

**Configuration Operations:**
- `SetStoragePolicy`, `GetStoragePolicy`
- `GetClusterStatus`, `GetNodeHealth`

### Storage Node libp2p Protocols

We will use protobuf for serialization in both our libp2p and gRPC APIs.

**Metadata Gossip Protocol:**
- Event propagation, acknowledgments, and conflict resolution
- Node discovery and cluster membership

**Chunk Transfer Protocol:**
- Direct chunk read/write operations between storage nodes
- Bulk chunk migration for rebalancing

### Administrative REST API

**Cluster Management:**
- Add/remove storage nodes, cluster configuration
- Policy management, user management

**Monitoring:**
- Node status, performance metrics, health checks
- Data integrity status, reconstruction progress
