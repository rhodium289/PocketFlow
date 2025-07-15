FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Redis Python package first
RUN pip install --no-cache-dir redis>=4.0.0

# Copy the entire project
COPY . .

# Install PocketFlow in development mode
RUN pip install -e .

# Create logs directory
RUN mkdir -p /app/logs

# Set Python path
ENV PYTHONPATH=/app

# Default command (can be overridden in docker-compose)
CMD ["python", "-c", "print('PocketFlow Redis container ready')"]