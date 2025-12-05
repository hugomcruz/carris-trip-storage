# Multi-stage build for smaller final image
FROM python:3.11-alpine AS builder

# Install build dependencies
RUN apk add --no-cache \
    gcc \
    musl-dev \
    postgresql-dev \
    linux-headers

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.11-alpine

# Install runtime dependencies only
RUN apk add --no-cache \
    libpq \
    && adduser -D -u 1000 appuser

# Set working directory
WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Copy only necessary application files
COPY --chown=appuser:appuser main.py redis_client.py postgres_client.py parquet_writer.py s3_client.py ./

# Create output directory
RUN mkdir -p /app/output/parquet && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set entrypoint
ENTRYPOINT ["python", "main.py"]

# Default command
CMD ["--help"]
