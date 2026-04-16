# =============================================
# Stage 1: Builder (heavy dependencies)
# =============================================
FROM python:3.11-slim AS builder

# Fix apt lock issues and install system packages
RUN rm -f /var/cache/apt/archives/lock \
    && rm -f /var/lib/apt/lists/lock \
    && apt-get update -qq \
    && apt-get install -y --no-install-recommends \
        build-essential \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

# Use cache mount for pip + Tsinghua mirror (you are in China)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    -r requirements.txt


# =============================================
# Stage 2: Final Runtime Image (slim)
# =============================================
FROM python:3.11-slim AS final

# Install runtime system dependencies
RUN rm -f /var/cache/apt/archives/lock \
    && apt-get update -qq \
    && apt-get install -y --no-install-recommends \
        git \
        chromium \
        chromium-driver \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy Python packages from builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY . .

# Optional: Create non-root user (recommended for security)
# RUN groupadd -g 1000 appuser && useradd -u 1000 -g appuser appuser \
#     && chown -R appuser:appuser /app
# USER appuser

CMD ["bash"]