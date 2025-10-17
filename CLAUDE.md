# CLAUDE.md for Rust Project

## Project Overview

WormFS, short for write-once-read-many file system, is intended to be user-space file system that uses erasure encoding to spread files across multiple storage devices, each running their own commodity filesystems. This allows great flexibility with respect to configuring device failure tolerance at a file or directory level. 

## Rules and Guidelines

1.  **Prioritize idiomatic Rust**: Always strive for clear, concise, and idiomatic Rust code. Follow Rust's best practices, including ownership, borrowing, and error handling.
2.  **Ensure memory safety**: Rust's core strength is memory safety. Avoid `unsafe` blocks unless absolutely necessary and provide clear justifications and safety invariants when used.
3.  **Write comprehensive tests**: Every new feature or bug fix should be accompanied by appropriate unit and integration tests. Aim for high test coverage.
4.  **Optimize for performance**: Consider performance implications, especially in critical paths. Profile and benchmark when necessary to identify bottlenecks.
5.  **Maintain clear documentation**: Add doc comments (`///`) to public items (structs, enums, functions, etc.) explaining their purpose, arguments, and return values.
6.  **Use Cargo for dependency management**: Manage dependencies exclusively through `Cargo.toml`. Avoid manual manipulation of `target` directories.
7.  **Batch Cargo commands**: When performing multiple Cargo operations (e.g., `cargo build`, `cargo test`), batch them for efficiency, especially in concurrent execution contexts.
8.  **Ask for clarification**: If any task or requirement is unclear, ask for clarification before proceeding with implementation.

## Preferred Tools and Workflows

*   **Testing**: Use `cargo test` for running tests.
*   **Linting/Formatting**: Use `cargo clippy` and `cargo fmt` to maintain code quality and consistency.
*   **Dependency Management**: Use `cargo add`, `cargo update`, etc., for managing dependencies.

## Specific Instructions for Claude

*   When proposing changes, provide a clear explanation of the approach and the rationale behind it.
*   If encountering compilation errors, attempt to resolve them and explain the fix.