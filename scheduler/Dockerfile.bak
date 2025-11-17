FROM python:3.11-alpine

# Set timezone to Asia/Jakarta
ENV TZ=Asia/Jakarta

# Install dependencies for PostgreSQL and MSSQL
RUN apk add --no-cache \
    gcc \
    musl-dev \
    postgresql-dev \
    freetds-dev \
    krb5-dev \
    openssl-dev \
    libffi-dev \
    tzdata \
    dcron \
    && cp /usr/share/zoneinfo/Asia/Jakarta /etc/localtime \
    && echo "Asia/Jakarta" > /etc/timezone

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY sync_scheduler.py .
COPY run_scheduler.sh .

# Create logs directory
RUN mkdir -p /app/logs

# Make scripts executable
RUN chmod +x sync_scheduler.py run_scheduler.sh

# Setup cron job (runs every minute)
RUN echo "* * * * * cd /app && /app/run_scheduler.sh >> /app/logs/cron.log 2>&1" > /etc/crontabs/root

# Create cron log
RUN touch /app/logs/cron.log

CMD ["crond", "-f", "-l", "2"]