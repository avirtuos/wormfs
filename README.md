# WormFS

WormFS, short for write-once-read-many file system, is intended to be user-space file system that uses erasure encoding to spread files across multiple storage devices, each running their own commodity filesystems. This allows great flexibility with respect to configuring device failure tolerance at a file or directory level. I envision this being extremely useful for media storage and deep archive use-cases.

Much of the architecture of this project is inspired by lizardfs' simplicity with a goal of offering greater control and visibility over how chunks are stored, replicated, and recovered.

This project is a work in progress. It is not even remotely usable at this point but I plan to chip away at it in my free time using my openai subscription, cline, and opencode.  

I've never written a distributed filesystem before so I am using this project as a way to see if Claude can educate me in this domain while building an actual project. A lot of the code you will see here is written by Claude so it will very much have an evolving prototype feel while I (a) steer Claude through building my vision for WormFS (b) go back and clean up the (likely) spaghetti that will result from the combination of my inexperience with distributed filesystems and the tendency of "Vibe Coding" to layer fix on-top of fix. In an attempt to "do this right" you'll find a design spec and implementation task breakdown in the docs folder. The goal is to balance the spec driven approach with the more fluid vibe mode for ironing out bugs and refining the system.

For more information please see
- [WormFS Design](design.md)
- [WormFS Implementation Plan](implementation.md)