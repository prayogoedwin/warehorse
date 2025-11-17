FROM python:3.11-slim

# Set timezone to Asia/Jakarta
ENV TZ=Asia/Jakarta
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    unixodbc \
    unixodbc-dev \
    freetds-bin \
    freetds-dev \
    tdsodbc \
    cron \
    tzdata \
    && ln -sf /usr/share/zoneinfo/Asia/Jakarta /etc/localtime \
    && echo "Asia/Jakarta" > /etc/timezone \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configure FreeTDS for MSSQL
RUN echo "[FreeTDS]\n\
Description = FreeTDS Driver\n\
Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" > /etc/odbcinst.ini

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY sync_scheduler.py .
COPY run_scheduler.sh .

# Create logs directory
RUN mkdir -p /app/logs

# Make scripts executable
RUN chmod +x sync_scheduler.py run_scheduler.sh

# Setup cron job (runs every minute)
RUN echo "* * * * * cd /app && /app/run_scheduler.sh >> /app/logs/cron.log 2>&1" > /etc/cron.d/sync-scheduler && \
    chmod 0644 /etc/cron.d/sync-scheduler && \
    crontab /etc/cron.d/sync-scheduler

# Create cron log
RUN touch /app/logs/cron.log

# Start cron
CMD ["cron", "-f"]