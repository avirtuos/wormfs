# CLAUDE.md for Rust Project

## Project Overview

WormFS, short for write-once-read-many file system, is intended to be user-space file system that uses erasure encoding to spread files across multiple storage devices, each running their own commodity filesystems. This allows great flexibility with respect to configuring device failure tolerance at a file or directory level. 

## Rules and Guidelines

1.  If you are working on a component, first read the design document in the docs/components folder and any other component required for the task. For example, StorageRaftMember's design is in docs/components/02_StorageRaftMember.md
2. Prioritize idiomatic Rust,aAlways strive for clear and concise rust code. Follow Rust's best practices, including ownership, borrowing, and error handling.
3. Make sure any relevant changes are reflected in the project REAME.md as well as the component's design file.
4. Every new feature or bug fix should be accompanied by appropriate unit and integration tests. Aim for high test coverage.
5. When validating a change or completion of a test, always run `cargo fmt`, `cargo build`, and `cargo test`.
6. At the end of each task, after validating the changes, commit them to the current git branch using a 1 sentence summary followed by a newline and a paragraph about what was accomplished in this commit and why. Do not include any authorship information or marketing snippets in commit messages.
7. Add metrics (using MetricsService) and log statements (e.g. info!, error!) at key operational and troubleshooting points in the code.
8. Maintain clear documentation, add doc comments (`///`) to public items (structs, enums, functions, etc.) explaining their purpose, arguments, and return values.
9. Ask for clarification, if any task or requirement is unclear, ask for clarification before proceeding with implementation.

## Preferred Tools and Workflows

*   **Testing**: Use `cargo test` for running tests.
*   **Linting/Formatting**: Use `cargo clippy` and `cargo fmt` to maintain code quality and consistency.
*   **Dependency Management**: Use `cargo add`, `cargo update`, etc., for managing dependencies.

## Specific Instructions for Claude

*   When proposing changes, provide a clear explanation of the approach and the rationale behind it.
*   If encountering compilation errors, attempt to resolve them and explain the fix.