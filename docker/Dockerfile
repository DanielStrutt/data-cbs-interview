# Build stage: install dependencies
FROM python:3.11-slim AS builder

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files and install (frozen for reproducibility)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Runtime stage
FROM python:3.11-slim

WORKDIR /app

# Copy virtualenv from builder
COPY --from=builder /app/.venv /app/.venv

# Copy source and config
COPY src/ ./src/
COPY config/ ./config/
COPY docker/entrypoint.sh ./entrypoint.sh

RUN chmod +x entrypoint.sh

# Activate the venv for all subsequent commands
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

ENTRYPOINT ["./entrypoint.sh"]
CMD ["all"]
