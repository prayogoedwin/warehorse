#!/bin/bash

echo "🚀 Quick Setup - Warehouse Sync System"
echo "========================================"
echo ""

# Make sure we're in the right directory
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ Error: docker-compose.yml not found"
    echo "   Please run this script from warehorse/ directory"
    exit 1
fi

echo "📂 Creating directories..."
mkdir -p scheduler/logs bot/data
chmod 755 scheduler/logs bot/data

echo "🔐 Setting permissions..."
chmod +x scheduler/run_scheduler.sh 2>/dev/null || true
chmod +x check_timezone.sh 2>/dev/null || true
chmod +x check_status.sh 2>/dev/null || true
chmod +x monitoring_commands.sh 2>/dev/null || true
chmod +x deploy_checklist.sh 2>/dev/null || true

echo ""
echo "📋 Environment Check..."
if [ ! -f ".env" ]; then
    echo "⚠️  Warning: .env file not found!"
    echo "   Please create .env file before starting containers"
else
    echo "✅ .env file exists"
fi

echo ""
echo "🐳 Docker Status..."
if docker ps &>/dev/null; then
    echo "✅ Docker is running"
else
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

echo ""
echo "🔨 Building containers..."
docker-compose build

echo ""
echo "🚀 Starting containers..."
docker-compose up -d

echo ""
echo "⏳ Waiting for containers to start..."
sleep 5

echo ""
echo "📊 Container Status:"
docker ps --filter "name=sync-scheduler" --filter "name=telegram-sync-bot" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "🕐 Verifying Timezone..."
echo "Scheduler timezone:"
docker exec sync-scheduler cat /etc/timezone 2>/dev/null || echo "Container not ready yet"
echo ""
echo "Bot timezone:"
docker exec telegram-sync-bot cat /etc/timezone 2>/dev/null || echo "Container not ready yet"

echo ""
echo "📝 Checking logs (last 10 lines)..."
echo ""
echo "=== Scheduler Logs ==="
docker logs --tail 10 sync-scheduler 2>/dev/null || echo "No logs yet"
echo ""
echo "=== Bot Logs ==="
docker logs --tail 10 telegram-sync-bot 2>/dev/null || echo "No logs yet"

echo ""
echo "✅ Setup Complete!"
echo ""
echo "📚 Next Steps:"
echo "   1. Check full logs: docker logs -f sync-scheduler"
echo "   2. Verify timezone: ./check_timezone.sh"
echo "   3. Monitor status: ./check_status.sh"
echo "   4. View commands: ./monitoring_commands.sh"
echo ""
echo "🧪 Test scheduler manually:"
echo "   docker exec sync-scheduler python /app/sync_scheduler.py"
echo ""
echo "⏰ Scheduler runs automatically every minute via cron"
echo "   Timezone: Asia/Jakarta (WIB/UTC+7)"
echo ""