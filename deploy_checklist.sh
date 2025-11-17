#!/bin/bash

echo "=== 🚀 Deployment Checklist ==="
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_file() {
    if [ -f "$1" ]; then
        echo -e "${GREEN}✅${NC} $1"
        return 0
    else
        echo -e "${RED}❌${NC} $1 - NOT FOUND"
        return 1
    fi
}

check_dir() {
    if [ -d "$1" ]; then
        echo -e "${GREEN}✅${NC} $1/"
        return 0
    else
        echo -e "${RED}❌${NC} $1/ - NOT FOUND"
        return 1
    fi
}

echo "📂 Checking File Structure..."
check_file "docker-compose.yml"
check_file ".env"
check_dir "scheduler"
check_file "scheduler/Dockerfile"
check_file "scheduler/sync_scheduler.py"
check_file "scheduler/requirements.txt"
check_file "scheduler/run_scheduler.sh"
check_dir "scheduler/logs"
check_dir "bot"
check_file "bot/Dockerfile"
check_file "bot/requirements.txt"
echo ""

echo "🔧 Checking Environment File..."
if [ -f ".env" ]; then
    echo -e "${YELLOW}Note: Pastikan .env sudah diisi dengan benar${NC}"
    echo "Required variables:"
    grep -E "^[A-Z_]+=.+" .env | cut -d= -f1 | sed 's/^/  - /'
fi
echo ""

echo "🐳 Checking Docker..."
if command -v docker &> /dev/null; then
    echo -e "${GREEN}✅${NC} Docker installed"
    docker --version
else
    echo -e "${RED}❌${NC} Docker NOT installed"
fi

if command -v docker-compose &> /dev/null; then
    echo -e "${GREEN}✅${NC} Docker Compose installed"
    docker-compose --version
else
    echo -e "${RED}❌${NC} Docker Compose NOT installed"
fi
echo ""

echo "📋 Deployment Steps:"
echo ""
echo "1️⃣  Setup permissions:"
echo "   chmod 755 scheduler/logs"
echo "   chmod +x scheduler/run_scheduler.sh"
echo "   chmod +x check_timezone.sh"
echo ""
echo "2️⃣  Build and start containers:"
echo "   docker-compose up -d --build"
echo ""
echo "3️⃣  Check container status:"
echo "   docker ps"
echo ""
echo "4️⃣  Verify timezone:"
echo "   ./check_timezone.sh"
echo ""
echo "5️⃣  Check logs:"
echo "   docker logs -f sync-scheduler"
echo "   docker logs -f telegram-sync-bot"
echo ""
echo "6️⃣  Monitor scheduler:"
echo "   tail -f scheduler/logs/sync_scheduler.log"
echo ""

echo "🕐 Timezone Configuration:"
echo "   All containers are configured with Asia/Jakarta (WIB/UTC+7)"
echo "   - Set in Dockerfile: ENV TZ=Asia/Jakarta"
echo "   - Set in docker-compose: TZ=Asia/Jakarta"
echo "   - System timezone: /etc/localtime -> Asia/Jakarta"
echo ""

echo "📚 Documentation:"
echo "   - README.md - Complete setup guide"
echo "   - check_timezone.sh - Verify timezone settings"
echo "   - check_status.sh - Monitor sync status"
echo "   - monitoring_commands.sh - Quick command reference"
echo ""

echo "🎯 Quick Test After Deployment:"
echo ""
echo "# Test scheduler manually"
echo "docker exec sync-scheduler python /app/sync_scheduler.py"
echo ""
echo "# Check if cron is running"
echo "docker exec sync-scheduler ps aux | grep crond"
echo ""
echo "# Verify timezone"
echo "docker exec sync-scheduler date"
echo "# Should output: ... WIB 2025"
echo ""

echo "✨ Ready to deploy!"