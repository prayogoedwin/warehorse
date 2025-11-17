#!/bin/bash

echo "=== 🐳 Docker Sync Scheduler Status ==="
echo ""

echo "📊 Container Status:"
docker ps --filter "name=sync-scheduler" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

echo "📝 Last 20 Scheduler Runs:"
docker exec sync-scheduler tail -30 /app/logs/sync_scheduler.log 2>/dev/null | grep -E "(Checking schedules|Found.*schedule|finished|Completed)" || echo "❌ Cannot read logs"
echo ""

echo "❌ Recent Errors:"
docker exec sync-scheduler tail -50 /app/logs/sync_scheduler.log 2>/dev/null | grep -i "error" || echo "✅ No errors found"
echo ""

echo "⏰ Cron Logs (last 10 lines):"
docker exec sync-scheduler tail -10 /app/logs/cron.log 2>/dev/null || echo "❌ Cron log not available"
echo ""

echo "📅 Active Schedules (from DB):"
docker exec telegram-sync-bot python3 << 'PYEOF' 2>/dev/null || echo "❌ Cannot connect to DB"
import os
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT name, source_schema, table_name, schedule_date, schedule_time, 
               status, last_run, last_status 
        FROM public.schedules 
        WHERE status = 'active' 
        ORDER BY schedule_date, schedule_time
    """)
    rows = cursor.fetchall()
    
    if rows:
        print(f"{'Name':<20} {'Schema.Table':<30} {'Date':<12} {'Time':<10} {'Last Run':<20} {'Status':<10}")
        print("-" * 120)
        for r in rows:
            last_run = r['last_run'].strftime('%Y-%m-%d %H:%M') if r['last_run'] else 'Never'
            print(f"{r['name']:<20} {r['source_schema']}.{r['table_name']:<30} {r['schedule_date']:<12} {r['schedule_time']:<10} {last_run:<20} {r['last_status'] or 'N/A':<10}")
    else:
        print("No active schedules")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
PYEOF
echo ""

echo "📝 Recent Sync Logs (from DB):"
docker exec telegram-sync-bot python3 << 'PYEOF' 2>/dev/null || echo "❌ Cannot connect to DB"
import os
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT schedule_name, source_schema, source_table, records_synced, 
               status, started_at, duration_seconds 
        FROM public.sync_logs 
        ORDER BY started_at DESC 
        LIMIT 10
    """)
    rows = cursor.fetchall()
    
    if rows:
        print(f"{'Schedule':<20} {'Schema.Table':<30} {'Records':<10} {'Duration':<10} {'Status':<10} {'Started At':<20}")
        print("-" * 120)
        for r in rows:
            started = r['started_at'].strftime('%Y-%m-%d %H:%M:%S') if r['started_at'] else 'N/A'
            print(f"{r['schedule_name']:<20} {r['source_schema']}.{r['source_table']:<30} {r['records_synced']:<10} {r['duration_seconds']}s{'':<7} {r['status']:<10} {started:<20}")
    else:
        print("No sync logs yet")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
PYEOF
echo ""

echo "📂 Log Files Size:"
docker exec sync-scheduler du -sh /app/logs/ 2>/dev/null || echo "❌ Cannot check size"
echo ""

echo "💾 Disk Usage:"
docker exec sync-scheduler df -h /app/logs 2>/dev/null || echo "❌ Cannot check disk"