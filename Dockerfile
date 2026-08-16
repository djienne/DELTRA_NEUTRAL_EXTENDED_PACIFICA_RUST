# Multi-stage build for Extended DEX and Pacifica Funding Rate Bot
# Stage 1: Builder
FROM rust:1.91-bookworm AS builder

# Install build dependencies (following best practices)
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy Python requirements first (for order signing)
COPY requirements.txt ./
RUN pip3 install -r requirements.txt --break-system-packages

# Copy and install python_sdk-starknet (Extended DEX SDK)
COPY python_sdk-starknet ./python_sdk-starknet
RUN cd python_sdk-starknet && pip3 install -e . --break-system-packages && cd ..

# Copy Cargo files for dependency caching
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY examples ./examples

# Copy Python signing script
COPY scripts ./scripts

# Build release binaries.
# emergency_exit MUST be built and shipped alongside the bot. When it was missing from
# the image it could only be run on the host, where STATE_FILE_PATH is unset - so it read
# a different (stale) state file than the container writes, found no position, and
# printed "EMERGENCY EXIT SUCCESSFUL" while both legs were still open.
RUN cargo build --release --bin extended_connector --bin emergency_exit

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Copy Python requirements and install
COPY requirements.txt ./
RUN pip3 install -r requirements.txt --break-system-packages --no-cache-dir

# Copy and install python_sdk-starknet
COPY python_sdk-starknet ./python_sdk-starknet
RUN cd python_sdk-starknet && pip3 install -e . --break-system-packages --no-cache-dir && cd ..

# Create app user for security
RUN useradd -m -u 1000 botuser

# Create app directory
WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/target/release/extended_connector /app/extended_connector
COPY --from=builder /app/target/release/emergency_exit /app/emergency_exit

# Copy Python signing script
COPY --from=builder /app/scripts /app/scripts

# Create data directory with proper permissions
RUN mkdir -p /app/data && chown -R botuser:botuser /app

# Switch to non-root user
USER botuser

# Set environment variables
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

# Health check (checks if process is running)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f extended_connector || exit 1

# Run the bot
CMD ["/app/extended_connector"]
