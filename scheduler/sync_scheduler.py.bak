#!/usr/bin/env python3
import os
import sys
import logging
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor
import pymssql

# Setup logging - Docker path
LOG_FILE = '/app/logs/sync_scheduler.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database configs
DB_CONFIG = {
    'host': os.getenv('DB_HOST', ''),
    'port': os.getenv('DB_PORT', ''),
    'database': os.getenv('DB_NAME', ''),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', '')
}

# MSSQL Config
MSSQL_CONFIG = {
    'host': os.getenv('DB_HOST_TARGET', ''),
    'port': os.getenv('DB_PORT_TARGET', ''),
    'database': os.getenv('DB_NAME_TARGET', ''),
    'user': os.getenv('DB_USER_TARGET', ''),
    'password': os.getenv('DB_PASSWORD_TARGET', '')
}

def get_pg_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_mssql_connection():
    return pymssql.connect(
        server=MSSQL_CONFIG['host'],
        port=MSSQL_CONFIG['port'],
        user=MSSQL_CONFIG['user'],
        password=MSSQL_CONFIG['password'],
        database=MSSQL_CONFIG['database']
    )

def convert_value(val):
    """Convert special types for PostgreSQL"""
    if val is None:
        return None
    elif isinstance(val, uuid.UUID):
        return str(val)
    elif isinstance(val, Decimal):
        return float(val)
    elif isinstance(val, bytes):
        return val.hex()
    else:
        return val

def sync_table(schema, table, schedule_name):
    """Sync one table from MSSQL to PostgreSQL"""
    start_time = datetime.now()
    
    try:
        logger.info(f"[{schedule_name}] Starting sync: {schema}.{table}")
        
        # 1. Get data from MSSQL
        mssql_conn = get_mssql_connection()
        mssql_cursor = mssql_conn.cursor(as_dict=True)
        
        query = f"SELECT * FROM [{schema}].[{table}]"
        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()
        
        mssql_cursor.close()
        mssql_conn.close()
        
        if not rows:
            logger.info(f"[{schedule_name}] Table is empty")
            return True, "Table is empty", 0
        
        records_count = len(rows)
        logger.info(f"[{schedule_name}] Fetched {records_count} records from MSSQL")
        
        # 2. Convert data
        columns = list(rows[0].keys())
        converted_rows = []
        for row in rows:
            converted_row = {col: convert_value(row[col]) for col in columns}
            converted_rows.append(converted_row)
        
        # 3. Truncate PostgreSQL
        pg_conn = get_pg_connection()
        pg_cursor = pg_conn.cursor()
        
        truncate_query = f"TRUNCATE TABLE {schema}.{table} CASCADE"
        pg_cursor.execute(truncate_query)
        pg_conn.commit()
        
        logger.info(f"[{schedule_name}] Truncated {schema}.{table}")
        
        # 4. Insert to PostgreSQL
        columns_str = ', '.join([f'"{col}"' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {schema}.{table} ({columns_str}) VALUES ({placeholders})"
        
        batch_size = 1000
        for i in range(0, len(converted_rows), batch_size):
            batch = converted_rows[i:i + batch_size]
            values = [tuple(row[col] for col in columns) for row in batch]
            pg_cursor.executemany(insert_query, values)
            pg_conn.commit()
            logger.info(f"[{schedule_name}] Inserted batch {i//batch_size + 1}/{(len(converted_rows)//batch_size)+1}")
        
        pg_cursor.close()
        pg_conn.close()
        
        duration = int((datetime.now() - start_time).total_seconds())
        logger.info(f"[{schedule_name}] Completed: {records_count} records in {duration}s")
        
        return True, f"Synced {records_count} records in {duration}s", records_count
        
    except Exception as e:
        logger.error(f"[{schedule_name}] Error: {e}", exc_info=True)
        return False, str(e), 0

def update_schedule_status(schedule_name, status, message):
    """Update schedule status"""
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """UPDATE public.schedules 
               SET status = %s, last_status = %s, last_message = %s, 
                   last_run = NOW(), updated_at = NOW()
               WHERE name = %s""",
            (status, 'success' if status == 'completed' else 'failed', message, schedule_name)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error updating schedule status: {e}")

def log_sync(schedule_name, schema, table, success, records, duration, error_msg=None):
    """Log sync to sync_logs table"""
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """INSERT INTO public.sync_logs 
               (schedule_name, sync_type, source_schema, source_table, 
                target_schema, target_table, records_synced, status, 
                started_at, completed_at, duration_seconds, error_message)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (schedule_name, 'single_table', schema, table, schema, table,
             records, 'success' if success else 'failed',
             datetime.now() - timedelta(seconds=duration), datetime.now(),
             duration, error_msg)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error logging sync: {e}")

def check_and_run_schedules():
    """Check schedules and run sync if needed"""
    try:
        logger.info("=" * 50)
        logger.info("Checking schedules...")
        
        # Get schedules yang waktunya sudah lewat
        conn = get_pg_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(
            """SELECT * FROM public.schedules 
               WHERE status = 'active' 
               AND sync_type = 'single_table'
               AND CONCAT(schedule_date, ' ', schedule_time)::timestamp <= NOW()
               AND (last_run IS NULL 
                    OR last_run < CONCAT(schedule_date, ' ', schedule_time)::timestamp)
               ORDER BY schedule_date, schedule_time
               LIMIT 10"""
        )
        
        schedules = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not schedules:
            logger.info("No schedules to run")
            return
        
        logger.info(f"Found {len(schedules)} schedule(s) to run")
        
        # Run each schedule
        for sched in schedules:
            name = sched['name']
            schema = sched['source_schema']
            table = sched['table_name']
            
            logger.info(f"Running schedule: {name} ({schema}.{table})")
            
            # Mark as running
            update_schedule_status(name, 'running', 'Sync in progress')
            
            # Run sync
            start = datetime.now()
            success, message, records = sync_table(schema, table, name)
            duration = int((datetime.now() - start).total_seconds())
            
            # Update status
            status = 'completed' if success else 'failed'
            update_schedule_status(name, status, message)
            
            # Log to sync_logs
            log_sync(name, schema, table, success, records, duration, 
                    None if success else message)
            
            logger.info(f"Schedule {name} finished: {message}")
        
        logger.info("All schedules processed")
        
    except Exception as e:
        logger.error(f"Error in check_and_run_schedules: {e}", exc_info=True)

if __name__ == '__main__':
    logger.info("Sync Scheduler Started")
    check_and_run_schedules()
    logger.info("Sync Scheduler Finished")