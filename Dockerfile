# Use Python 3.11 as base image
FROM python:3.11-slim

# Set working directory in the container
WORKDIR /app

# System dependencies first
RUN echo \
    "deb https://mirrors.ustc.edu.cn/debian bullseye main\n" \
    "deb https://mirrors.ustc.edu.cn/debian bullseye-updates main\n" \
    "deb https://mirrors.ustc.edu.cn/debian-security bullseye-security main\n" \
    > /etc/apt/sources.list \
    && apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Then Python dependencies
# Copy requirements.txt first (for better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY app/ /app/

# Create directories for data and logs
RUN mkdir -p /app/data /app/logs

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
#ENV TZ=UTC

# Command to run when container starts
CMD ["python", "src/scheduler.py"]