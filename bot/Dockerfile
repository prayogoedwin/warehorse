FROM python:3.11-slim

# Set timezone to Asia/Jakarta
ENV TZ=Asia/Jakarta

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY . .

# Create data directory
RUN mkdir -p /app/data

CMD ["python", "bot.py"]