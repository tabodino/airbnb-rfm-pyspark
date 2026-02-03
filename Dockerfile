# -----------------------------------------------------------------------------
# Stage 1: JRE Base - Official Eclipse Temurin (Secure & Maintained)
# -----------------------------------------------------------------------------
FROM eclipse-temurin:17-jre AS jre

# -----------------------------------------------------------------------------
# Stage 2: Python Base with JRE
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS base

# Prevent interactive prompts during build
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Copy JRE from official image (lightweight & secure)
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=jre ${JAVA_HOME} ${JAVA_HOME}
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install system dependencies
# curl: UV installer + health check
# wget: Dataset download
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Verify Java installation
RUN java -version

# -----------------------------------------------------------------------------
# Stage 3: Dependencies - Install Python packages with UV
# -----------------------------------------------------------------------------
FROM base AS dependencies

WORKDIR /app

# Install UV (fast Python package manager)
RUN curl -LsSf https://astral.sh/uv/0.4.18/install.sh | sh
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy dependency files (enables Docker layer caching)
COPY pyproject.toml README.md ./

# Install dependencies without cache (smaller image)
RUN uv pip install --system --no-cache-dir -e .

# -----------------------------------------------------------------------------
# Stage 4: Runtime - Final application image
# -----------------------------------------------------------------------------
FROM base AS runtime

WORKDIR /app

# Copy installed dependencies from previous stage
COPY --from=dependencies /usr/local /usr/local

# Copy application code
COPY config/ ./config/
COPY src/ ./src/
COPY pyproject.toml README.md ./
COPY .env.example ./.env

# Create necessary directories
RUN mkdir -p data logs

# Set Python path for imports
ENV PYTHONPATH=/app

# -----------------------------------------------------------------------------
# Security: Create non-root user
# -----------------------------------------------------------------------------
RUN groupadd -r appuser && useradd -r -g appuser -u 1001 appuser \
    && chown -R appuser:appuser /app

USER appuser

# -----------------------------------------------------------------------------
# Expose Streamlit port
# -----------------------------------------------------------------------------
EXPOSE 8501

# -----------------------------------------------------------------------------
# Health check for container orchestration (Docker Compose, Kubernetes)
# -----------------------------------------------------------------------------
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# -----------------------------------------------------------------------------
# Startup command
# -----------------------------------------------------------------------------
CMD ["streamlit", "run", "src/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
