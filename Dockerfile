# Multi-stage build for smaller image
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only dependency files first for better caching
COPY pyproject.toml .
COPY README.md .
COPY src/ src/

# Build wheel
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

# Final stage
FROM python:3.12-slim

WORKDIR /app

# Install runtime dependencies for confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash appuser

# Copy and install wheel
COPY --from=builder /wheels/*.whl /wheels/
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels

# Create data directory
RUN mkdir -p /app/data && chown appuser:appuser /app/data

USER appuser

# Default data volume mount point
VOLUME ["/app/data"]

# Default command - can be overridden
ENTRYPOINT ["weather-station-db"]
CMD ["--producers", "ndbc"]
