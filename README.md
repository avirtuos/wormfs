# WormFS

WormFS, short for write-once-read-many file system, is intended to be user-space file system that uses erasure encoding to spread files across multiple storage devices, each running their own commodity filesystems. This allows great flexibility with respect to configuring device failure tolerance at a file or directory level. I envision this being extremely useful for media storage and deep archive use-cases.

Much of the architecture of this project is inspired by lizardfs' simplicity with a goal of offering greater control and visibility over how chunks are stored, replicated, and recovered.

This project is a work in progress. It is not even remotely usable at this point but I plan to chip away at it in my free time using my openai subscription, cline, and opencode.  

For more information please see
- [WormFS Design](design.md)
- [WormFS Implementation Plan](implementation.md)