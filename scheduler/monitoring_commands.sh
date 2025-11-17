#!/bin/bash
# Quick monitoring commands for Docker Sync Scheduler

echo "=== 🚀 Quick Commands ==="
echo ""

cat << 'EOF'
📊 MONITORING COMMANDS:

1. Check Container Status:
   docker ps | grep sync

2. View Scheduler Logs (realtime):
   docker logs -f sync-scheduler

3. View Last 50 Scheduler Logs:
   docker exec sync-scheduler tail -50 /app/logs/sync_scheduler.log

4. View Cron Logs:
   docker exec sync-scheduler tail -50 /app/logs/cron.log

5. Check for Errors:
   docker exec sync-scheduler grep -i error /app/logs/sync_scheduler.log | tail -20

6. Manual Run Scheduler:
   docker exec sync-scheduler python /app/sync_scheduler.py

7. Check Crontab Inside Container:
   docker exec sync-scheduler crontab -l

8. Enter Container Shell:
   docker exec -it sync-scheduler sh

9. Restart Scheduler:
   docker-compose restart scheduler

10. View All Logs Together:
    docker-compose logs -f

11. Check Log Files Size:
    docker exec sync-scheduler ls -lh /app/logs/

12. View Telegram Bot Logs:
    docker logs -f telegram-sync-bot

EOF

echo ""
echo "🔧 TROUBLESHOOTING:"
echo ""

cat << 'EOF'
Problem: Scheduler not running
Fix: docker-compose restart scheduler

Problem: Cannot connect to database
Fix: Check .env file and host.docker.internal

Problem: Logs not updating
Fix: Check cron is running:
     docker exec sync-scheduler ps aux | grep crond

Problem: Manual sync test
Fix: docker exec sync-scheduler python /app/sync_scheduler.py

EOF

echo ""
echo "📁 LOG LOCATIONS (inside container):"
echo "   /app/logs/sync_scheduler.log  - Main scheduler logs"
echo "   /app/logs/cron.log            - Cron execution logs"
echo ""
echo "📁 LOG LOCATIONS (host):"
echo "   ./scheduler/logs/sync_scheduler.log"
echo "   ./scheduler/logs/cron.log"