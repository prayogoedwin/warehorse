import os
import logging
import asyncio
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackContext
)
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Environment variables
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
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

N8N_API_URL = os.getenv('N8N_API_URL', '')
N8N_API_KEY = os.getenv('N8N_API_KEY', '')

# Global variables untuk info loop
info_loop_tasks = {}

class DatabaseManager:
    @staticmethod
    def get_connection():
        return psycopg2.connect(**DB_CONFIG)
    
    @staticmethod
    def get_mssql_connection():
        """Get MSSQL connection using pyodbc (lebih stabil dari pymssql)"""
        import pyodbc
        
        conn_str = (
            f"DRIVER={{FreeTDS}};"
            f"SERVER={MSSQL_CONFIG['host']};"
            f"PORT={MSSQL_CONFIG['port']};"
            f"DATABASE={MSSQL_CONFIG['database']};"
            f"UID={MSSQL_CONFIG['user']};"
            f"PWD={MSSQL_CONFIG['password']};"
            f"TDS_Version=7.4;"
        )
        return pyodbc.connect(conn_str)
    
    @staticmethod
    def execute_query(query, params=None, fetch=False):
        try:
            with DatabaseManager.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    if fetch:
                        return cur.fetchall()
                    conn.commit()
                    return True
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
    
    @staticmethod
    def manual_sync_table(schema, table):
        """
        Manual sync satu tabel dari MSSQL ke PostgreSQL
        Returns: tuple (success, message, records_count)
        """
        start_time = datetime.now()
        
        def convert_value(val):
            """Convert special types for PostgreSQL compatibility"""
            if val is None:
                return None
            elif isinstance(val, uuid.UUID):
                return str(val)
            elif isinstance(val, Decimal):
                return float(val)
            elif isinstance(val, (datetime, timedelta)):
                return val
            elif isinstance(val, bytes):
                return val.hex()
            else:
                return val
        
        try:
            # 1. Get data from MSSQL
            logger.info(f"Fetching data from MSSQL: {schema}.{table}")
            mssql_conn = DatabaseManager.get_mssql_connection()
            mssql_cursor = mssql_conn.cursor()
            
            query_mssql = f"SELECT * FROM [{schema}].[{table}]"
            mssql_cursor.execute(query_mssql)
            
            # Get column names dari cursor.description
            columns = [column[0] for column in mssql_cursor.description]
            
            # Fetch all rows
            raw_rows = mssql_cursor.fetchall()
            
            mssql_cursor.close()
            mssql_conn.close()
            
            if not raw_rows:
                return (True, "Tabel kosong, tidak ada data untuk disinkronkan", 0)
            
            records_count = len(raw_rows)
            logger.info(f"Fetched {records_count} records from MSSQL")
            
            # 2. Convert rows to dict format
            rows = []
            for raw_row in raw_rows:
                row_dict = {col: val for col, val in zip(columns, raw_row)}
                rows.append(row_dict)
            
            # 3. Convert all values
            converted_rows = []
            for row in rows:
                converted_row = {col: convert_value(row[col]) for col in columns}
                converted_rows.append(converted_row)
            
            # 4. Truncate PostgreSQL table
            logger.info(f"Truncating PostgreSQL table: {schema}.{table}")
            pg_conn = DatabaseManager.get_connection()
            pg_cursor = pg_conn.cursor()
            
            truncate_query = f"TRUNCATE TABLE {schema}.{table} CASCADE"
            pg_cursor.execute(truncate_query)
            pg_conn.commit()
            
            # 5. Insert data to PostgreSQL
            logger.info(f"Inserting {records_count} records to PostgreSQL")
            
            # Build insert query
            columns_str = ', '.join([f'"{col}"' for col in columns])
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {schema}.{table} ({columns_str}) VALUES ({placeholders})"
            
            # Insert in batches
            batch_size = 1000
            inserted_count = 0
            for i in range(0, len(converted_rows), batch_size):
                batch = converted_rows[i:i + batch_size]
                values = [tuple(row[col] for col in columns) for row in batch]
                pg_cursor.executemany(insert_query, values)
                pg_conn.commit()
                inserted_count += len(batch)
                logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
            
            pg_cursor.close()
            pg_conn.close()
            
            # 6. Log to sync_logs
            duration = int((datetime.now() - start_time).total_seconds())
            DatabaseManager.execute_query(
                """INSERT INTO public.sync_logs 
                   (schedule_name, sync_type, source_schema, source_table, target_schema, target_table, 
                    records_synced, status, started_at, completed_at, duration_seconds)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                ('manual_sync', 'manual', schema, table, schema, table, 
                 records_count, 'success', start_time, datetime.now(), duration)
            )
            
            return (True, f"Berhasil sync {records_count} records dalam {duration}s", records_count)
            
        except Exception as e:
            logger.error(f"Manual sync error: {e}", exc_info=True)
            
            # Log error
            duration = int((datetime.now() - start_time).total_seconds())
            try:
                DatabaseManager.execute_query(
                    """INSERT INTO public.sync_logs 
                       (schedule_name, sync_type, source_schema, source_table, target_schema, target_table, 
                        records_synced, status, started_at, completed_at, error_message, duration_seconds)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    ('manual_sync', 'manual', schema, table, schema, table, 
                     0, 'failed', start_time, datetime.now(), str(e), duration)
                )
            except:
                pass
            
            return (False, f"Error: {str(e)}", 0)

# Bot Commands
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /start command"""
    welcome_message = """
🤖 *Selamat datang di Bot Sinkronisasi Database*

Saya akan membantu Anda memonitor dan mengontrol sinkronisasi data dari MSSQL ke PostgreSQL.

*Perintah yang tersedia:*

📊 *Monitoring*
/info - Status sinkronisasi terkini
/info\_loop {menit} - Info berkala setiap N menit

📅 *Schedule Management*
/schedule - Lihat semua jadwal
📋 *Single Table Schedule*
/schedule single add {nama} {schema} {table} {YYYY-MM-DD} {HH:MM}
/schedule single delete {nama}

🔄 *Manual Sync*
/sync table {schema} {table} - Sync manual 1 tabel

⚙️ *Control*
/restart bot - Restart bot ini
/stop - Hentikan bot

*Contoh penggunaan:*
`/schedule single add sync_customers ref customers 2025-11-20 03:00`
`/sync table datamart orders`
`/sync table ref customers`
`/info_loop 30`
    """
    await update.message.reply_text(welcome_message, parse_mode='Markdown')

async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /info command"""
    try:
        schedules = DatabaseManager.execute_query(
            "SELECT * FROM public.schedules WHERE status = 'active' ORDER BY schedule_date, schedule_time",
            fetch=True
        )
        
        logs = DatabaseManager.execute_query(
            """SELECT * FROM public.sync_logs 
               ORDER BY started_at DESC LIMIT 5""",
            fetch=True
        )
        
        # Build response - TANPA MARKDOWN
        response = "📊 Status Sinkronisasi\n\n"
        
        response += "Jadwal Aktif:\n"
        if schedules:
            for sched in schedules:
                last_run = sched['last_run'].strftime('%Y-%m-%d %H:%M') if sched['last_run'] else 'Belum pernah'
                sync_info = ""
                if sched.get('sync_type') == 'single_table':
                    sync_info = f" ({sched.get('source_schema')}.{sched.get('table_name')})"
                response += f"• {sched['name']}{sync_info}\n"
                response += f"  📅 {sched['schedule_date']} {sched['schedule_time']}\n"
                response += f"  ⏱ Last run: {last_run}\n"
                response += f"  ✅ Status: {sched['last_status'] or 'N/A'}\n"
                if sched.get('last_message'):
                    # Escape special characters
                    msg = str(sched['last_message']).replace('_', ' ').replace('*', ' ')
                    response += f"  💬 {msg[:50]}\n"
                response += "\n"
        else:
            response += "Tidak ada jadwal aktif\n\n"
        
        response += "5 Log Terakhir:\n"
        if logs:
            for log in logs:
                status_emoji = "✅" if log['status'] == 'success' else "❌" if log['status'] == 'failed' else "⏳"
                started = log['started_at'].strftime('%Y-%m-%d %H:%M:%S') if log['started_at'] else 'N/A'
                table_info = ""
                if log.get('source_table'):
                    table_info = f" ({log.get('source_schema')}.{log.get('source_table')})"
                response += f"{status_emoji} {log['schedule_name']}{table_info} - {started}\n"
                response += f"   Records: {log['records_synced']}, Duration: {log['duration_seconds']}s\n"
        else:
            response += "Belum ada log\n"
        
        # Kirim TANPA parse_mode
        await update.message.reply_text(response)
        
    except Exception as e:
        logger.error(f"Info error: {e}")
        await update.message.reply_text(f"Error: {str(e)}")

async def schedule_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /schedule command"""
    try:
        schedules = DatabaseManager.execute_query(
            "SELECT * FROM public.schedules ORDER BY schedule_date, schedule_time",
            fetch=True
        )
        
        if not schedules:
            await update.message.reply_text("Tidak ada jadwal tersimpan")
            return
        
        response = "📅 Daftar Jadwal Sinkronisasi\n\n"
        for sched in schedules:
            status_emoji = "✅" if sched['status'] == 'active' else "⏸" if sched['status'] == 'inactive' else "🔄"
            sync_info = ""
            if sched.get('sync_type') == 'single_table':
                sync_info = f"\n   📊 {sched.get('source_schema')}.{sched.get('table_name')}"
            
            response += f"{status_emoji} {sched['name']}{sync_info}\n"
            response += f"   📆 {sched['schedule_date']} ⏰ {sched['schedule_time']}\n"
            response += f"   Cron: {sched['cron_expression']}\n"
            response += f"   Status: {sched['status']}\n\n"
        
        await update.message.reply_text(response)
        
    except Exception as e:
        logger.error(f"Schedule list error: {e}")
        await update.message.reply_text(f"Error: {str(e)}")

async def schedule_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /schedule sync add"""
    try:
        logger.info(f"Schedule add called with args: {context.args}")
        
        if len(context.args) < 5:
            await update.message.reply_text(
                "Format: /schedule sync add {nama} {YYYY-MM-DD} {HH:MM}\n"
                "Contoh: /schedule sync add weekly_backup 2025-11-20 19:00"
            )
            return
        
        name = context.args[2]
        date = context.args[3]
        time = context.args[4]
        
        datetime.strptime(date, '%Y-%m-%d')
        datetime.strptime(time, '%H:%M')
        
        dt = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M')
        cron = f"{dt.minute} {dt.hour} {dt.day} {dt.month} *"
        
        DatabaseManager.execute_query(
            """INSERT INTO public.schedules 
               (name, sync_type, schedule_date, schedule_time, cron_expression, status)
               VALUES (%s, 'full', %s, %s, %s, 'active')""",
            (name, date, time, cron)
        )
        
        await update.message.reply_text(
            f"✅ Jadwal '{name}' berhasil ditambahkan!\n"
            f"📅 {date} ⏰ {time}\n"
            f"Cron: {cron}"
        )
        
    except Exception as e:
        logger.error(f"Schedule add error: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def schedule_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /schedule sync edit"""
    try:
        logger.info(f"Schedule edit called with args: {context.args}")
        
        if len(context.args) < 5:
            await update.message.reply_text(
                "Format: /schedule sync edit {nama} {YYYY-MM-DD} {HH:MM}"
            )
            return
        
        name = context.args[2]
        date = context.args[3]
        time = context.args[4]
        
        datetime.strptime(date, '%Y-%m-%d')
        datetime.strptime(time, '%H:%M')
        
        dt = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M')
        cron = f"{dt.minute} {dt.hour} {dt.day} {dt.month} *"
        
        DatabaseManager.execute_query(
            """UPDATE public.schedules 
               SET schedule_date = %s, schedule_time = %s, 
                   cron_expression = %s, updated_at = CURRENT_TIMESTAMP
               WHERE name = %s""",
            (date, time, cron, name)
        )
        
        await update.message.reply_text(f"✅ Jadwal '{name}' berhasil diupdate!")
        
    except Exception as e:
        logger.error(f"Schedule edit error: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def schedule_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /schedule sync delete dan /schedule single delete"""
    try:
        logger.info(f"Delete called with args: {context.args}")
        
        if len(context.args) < 3:
            await update.message.reply_text("Format: /schedule sync delete {nama} atau /schedule single delete {nama}")
            return
        
        name = context.args[2]
        logger.info(f"Deleting schedule: {name}")
        
        DatabaseManager.execute_query(
            "DELETE FROM public.schedules WHERE name = %s",
            (name,)
        )
        
        await update.message.reply_text(f"✅ Jadwal '{name}' berhasil dihapus!")
        
    except Exception as e:
        logger.error(f"Delete error: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def schedule_single_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /schedule single add"""
    try:
        logger.info(f"Single add called with args: {context.args}")
        
        if len(context.args) < 7:
            await update.message.reply_text(
                "Format: /schedule single add {nama} {schema} {table} {YYYY-MM-DD} {HH:MM}\n"
                "Contoh: /schedule single add sync_customers ref customers 2025-11-20 03:00"
            )
            return
        
        name = context.args[2]
        schema = context.args[3]
        table = context.args[4]
        date = context.args[5]
        time = context.args[6]
        
        if schema not in ['datamart', 'ref', 'public']:
            await update.message.reply_text("Schema hanya boleh 'datamart', 'ref', atau 'public'")
            return
        
        datetime.strptime(date, '%Y-%m-%d')
        datetime.strptime(time, '%H:%M')
        
        dt = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M')
        cron = f"{dt.minute} {dt.hour} {dt.day} {dt.month} *"
        
        DatabaseManager.execute_query(
            """INSERT INTO public.schedules 
               (name, sync_type, source_schema, table_name, schedule_date, schedule_time, cron_expression, status)
               VALUES (%s, 'single_table', %s, %s, %s, %s, %s, 'active')""",
            (name, schema, table, date, time, cron)
        )
        
        await update.message.reply_text(
            f"✅ Single table sync '{name}' berhasil ditambahkan!\n"
            f"📊 Schema: {schema}\n"
            f"📋 Table: {table}\n"
            f"📅 {date} ⏰ {time}\n"
            f"Cron: {cron}"
        )
        
    except Exception as e:
        logger.error(f"Single add error: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def manual_sync_table(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /sync table {schema} {table}"""
    try:
        logger.info(f"Manual sync table called with args: {context.args}")
        
        if len(context.args) < 3:
            await update.message.reply_text(
                "Format: /sync table {schema} {table}\n\n"
                "Contoh:\n"
                "/sync table datamart orders\n"
                "/sync table ref customers"
            )
            return
        
        schema = context.args[1]
        table = context.args[2]
        
        if schema not in ['datamart', 'ref', 'public']:
            await update.message.reply_text("Schema hanya boleh 'datamart', 'ref', atau 'public'")
            return
        
        processing_msg = await update.message.reply_text(
            f"🔄 Memulai sinkronisasi manual...\n"
            f"📊 Schema: {schema}\n"
            f"📋 Table: {table}\n\n"
            f"Mohon tunggu..."
        )
        
        # Perform sync in thread
        result = await asyncio.to_thread(
            DatabaseManager.manual_sync_table, 
            schema, 
            table
        )
        
        success = result[0]
        message = result[1]
        records = result[2]
        
        if success:
            await processing_msg.edit_text(
                f"✅ Sinkronisasi berhasil!\n\n"
                f"📊 Schema: {schema}\n"
                f"📋 Table: {table}\n"
                f"📈 Records: {records}\n"
                f"💬 {message}"
            )
        else:
            await processing_msg.edit_text(
                f"❌ Sinkronisasi gagal!\n\n"
                f"📊 Schema: {schema}\n"
                f"📋 Table: {table}\n"
                f"💬 {message}"
            )
        
    except Exception as e:
        logger.error(f"Manual sync table handler error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def info_loop_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /info_loop"""
    try:
        if len(context.args) < 1:
            await update.message.reply_text("Format: /info_loop {menit}\nContoh: /info_loop 30")
            return
        
        minutes = int(context.args[0])
        if minutes < 1:
            await update.message.reply_text("Minimal 1 menit")
            return
        
        chat_id = update.effective_chat.id
        
        if chat_id in info_loop_tasks:
            info_loop_tasks[chat_id].cancel()
        
        async def send_periodic_info():
            while True:
                try:
                    await asyncio.sleep(minutes * 60)
                    await info(update, context)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Info loop error: {e}")
        
        task = asyncio.create_task(send_periodic_info())
        info_loop_tasks[chat_id] = task
        
        await update.message.reply_text(
            f"✅ Info loop diaktifkan!\n"
            f"Anda akan menerima update setiap {minutes} menit.\n"
            f"Gunakan /stop untuk menghentikan."
        )
        
    except Exception as e:
        logger.error(f"Info loop error: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def stop_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /stop"""
    chat_id = update.effective_chat.id
    
    if chat_id in info_loop_tasks:
        info_loop_tasks[chat_id].cancel()
        del info_loop_tasks[chat_id]
        await update.message.reply_text("⏹ Info loop dihentikan")
    else:
        await update.message.reply_text("Tidak ada info loop yang aktif")

async def restart_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler untuk /restart bot"""
    await update.message.reply_text("🔄 Bot akan restart...")
    os.execv(os.sys.executable, ['python'] + os.sys.argv)

def main():
    """Main function"""
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("info", info))
    application.add_handler(CommandHandler("restart", restart_bot))
    application.add_handler(CommandHandler("info_loop", info_loop_start))
    
    async def sync_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Router untuk sync commands"""
        try:
            logger.info(f"Sync router called with args: {context.args}")
            
            if not context.args:
                await update.message.reply_text(
                    "Format: /sync table {schema} {table}\n"
                    "Contoh: /sync table datamart orders"
                )
                return
            
            action = context.args[0].lower()
            
            if action == "table":
                await manual_sync_table(update, context)
            else:
                await update.message.reply_text("Command tidak dikenal. Gunakan: /sync table {schema} {table}")
                
        except Exception as e:
            logger.error(f"Sync router error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")
    
    application.add_handler(CommandHandler("sync", sync_router))
    
    async def schedule_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Router untuk semua schedule commands"""
        try:
            logger.info(f"Schedule router called with args: {context.args}")
            
            if not context.args:
                await schedule_list(update, context)
                return
            
            action = context.args[0].lower()
            
            if action == "sync":
                if len(context.args) < 2:
                    await schedule_list(update, context)
                    return
                    
                subaction = context.args[1].lower()
                logger.info(f"Schedule sync subaction: {subaction}")
                
                if subaction == "add":
                    await schedule_add(update, context)
                elif subaction == "edit":
                    await schedule_edit(update, context)
                elif subaction == "delete":
                    await schedule_delete(update, context)
                else:
                    await update.message.reply_text("Subcommand tidak dikenal. Gunakan: add, edit, delete")
            
            elif action == "single":
                if len(context.args) < 2:
                    await update.message.reply_text("Format: /schedule single add/delete")
                    return
                    
                subaction = context.args[1].lower()
                logger.info(f"Schedule single subaction: {subaction}")
                
                if subaction == "add":
                    await schedule_single_add(update, context)
                elif subaction == "delete":
                    await schedule_delete(update, context)
                else:
                    await update.message.reply_text("Subcommand tidak dikenal. Gunakan: add, delete")
            
            else:
                await schedule_list(update, context)
                
        except Exception as e:
            logger.error(f"Schedule router error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")
    
    application.add_handler(CommandHandler("schedule", schedule_router))
    
    async def stop_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await stop_bot(update, context)
    
    application.add_handler(CommandHandler("stop", stop_router))
    
    async def post_init(application: Application):
        await application.bot.set_my_commands([
            BotCommand("start", "Mulai bot"),
            BotCommand("info", "Status sinkronisasi"),
            BotCommand("schedule", "Kelola jadwal"),
            BotCommand("sync", "Sync manual per tabel"),
            BotCommand("stop", "Hentikan"),
        ])
    
    application.post_init = post_init
    
    logger.info("Bot started...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()