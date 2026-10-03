FROM python:3.12-slim

WORKDIR /app

# Create a non-root user for security
RUN groupadd -r taskflow && useradd -r -g taskflow taskflow

# Install dependencies first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Change ownership to non-root user
RUN chown -R taskflow:taskflow /app

# Switch to non-root user
USER taskflow

EXPOSE 8000

# Add healthcheck to monitor API status
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Default command — overridden in docker-compose for the worker
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

