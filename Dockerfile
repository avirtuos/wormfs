# Multi-stage build for WormFS
FROM rust:1.75-slim as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libfuse-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /usr/src/app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies (this will be cached)
RUN cargo build --release && rm -rf src

# Copy source code
COPY src ./src
COPY proto ./proto

# Build the application
RUN touch src/main.rs && cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl3 \
    libfuse2 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create wormfs user
RUN useradd -r -s /bin/false -m -d /var/lib/wormfs wormfs

# Create necessary directories
RUN mkdir -p /etc/wormfs /var/lib/wormfs /var/log/wormfs && \
    chown -R wormfs:wormfs /var/lib/wormfs /var/log/wormfs

# Copy the binary from builder stage
COPY --from=builder /usr/src/app/target/release/wormfs /usr/local/bin/wormfs

# Copy default configuration
COPY config/storage_node.yaml /etc/wormfs/storage_node.yaml

# Set permissions
RUN chmod +x /usr/local/bin/wormfs

# Create mount point for FUSE
RUN mkdir -p /mnt/wormfs && chown wormfs:wormfs /mnt/wormfs

# Expose ports
EXPOSE 8080 8443 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /usr/local/bin/wormfs health-check || exit 1

# Switch to wormfs user
USER wormfs

# Set working directory
WORKDIR /var/lib/wormfs

# Default command
CMD ["/usr/local/bin/wormfs", "storage-node", "--config", "/etc/wormfs/storage_node.yaml"]
