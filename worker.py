import os
import asyncio
import logging
import re
from datetime import datetime
import psycopg
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon import events

# --- LOGGING CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- DATABASE CONNECTION ENGINE ---
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://sky_otp_db_user:oYom3EdpOfLCpLSGlc2dAV8qY9zw2oot@dpg-d98lkf5aeets73f2po2g-a/sky_otp_db"
)

# Active background runtime memory parameters tracking instances
running_clients = {}

def get_db_connection():
    return psycopg.connect(DATABASE_URL)

# FIX: Added database schema initialization to worker to completely stop race conditions
def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        uid BIGINT PRIMARY KEY, 
                        balance NUMERIC(10, 2) DEFAULT 0.00, 
                        join_date TEXT,
                        screenshot_state BOOLEAN DEFAULT FALSE
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS available_accounts (
                        id SERIAL PRIMARY KEY,
                        country_id TEXT,
                        phone_number TEXT UNIQUE,
                        api_id TEXT,
                        api_hash TEXT,
                        string_session TEXT,
                        is_sold BOOLEAN DEFAULT FALSE
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS active_orders (
                        uid BIGINT,
                        account_id INTEGER,
                        phone_number TEXT,
                        country_name TEXT,
                        cost_inr NUMERIC(10, 2),
                        status TEXT DEFAULT 'WAITING',
                        timestamp TEXT
                    )
                """)
                conn.commit()
                logging.info("⚡ Background worker verified and initialized database schemas successfully.")
    except Exception as e:
        logging.error(f"Database sync fault exception in worker: {e}")

# --- OTP EXTRACTOR LOGIC ---
def extract_otp(message_text: str) -> str:
    if not message_text:
        return ""
    match = re.search(r'\b\d{4,6}\b', message_text)
    return match.group(0) if match else ""

# --- TELEGRAM INBOUND EVENT RECEPTOR ---
async def register_telegram_listeners(account_id, phone, client: TelegramClient):
    @client.on(events.NewMessage(incoming=True))
    async def incoming_sms_handler(event):
        sender = await event.get_sender()
        sender_name = getattr(sender, 'title', getattr(sender, 'username', 'Unknown System Source'))
        msg_text = event.raw_text
        
        logging.info(f"📩 Inbound payload captured on [{phone}] from {sender_name}: {msg_text}")
        
        otp_code = extract_otp(msg_text)
        if otp_code:
            logging.info(f"✨ OTP Extracted successfully: {otp_code}. Syncing payload data...")
            
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE active_orders 
                            SET status = 'COMPLETED' 
                            WHERE account_id = %s AND status = 'WAITING'
                        """, (account_id,))
                        
                        cur.execute("""
                            UPDATE available_accounts 
                            SET is_sold = TRUE 
                            WHERE id = %s
                        """, (account_id,))
                        
                        cur.execute("""
                            UPDATE users 
                            SET balance = balance - (
                                SELECT cost_inr FROM active_orders WHERE account_id = %s LIMIT 1
                            )
                            WHERE uid = (
                                SELECT uid FROM active_orders WHERE account_id = %s LIMIT 1
                            )
                        """, (account_id, account_id))
                        
                        conn.commit()
                
                logging.info(f"✅ Database transaction logs locked for {phone}. Shutting down worker runtime connection.")
                await client.disconnect()
                running_clients.pop(account_id, None)
                
            except Exception as ex:
                logging.error(f"Failed to synchronize state update operations into cluster: {ex}")

# --- PROCESSING LOOP ROUTINE ---
async def check_active_orders_pipeline():
    while True:
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT ao.account_id, ao.phone_number, aa.api_id, aa.api_hash, aa.string_session 
                        FROM active_orders ao
                        JOIN available_accounts aa ON ao.account_id = aa.id
                        WHERE ao.status = 'WAITING'
                    """)
                    pending_orders = cur.fetchall()
                    
            for order in pending_orders:
                acc_id, phone, api_id, api_hash, session_str = order
                
                if not session_str or str(session_str).strip() == "" or session_str == "None":
                    logging.warning(f"⚠️ Account ID {acc_id} ({phone}) contains an empty/corrupted session token string parameter. Skipping execution flow entirely.")
                    continue
                
                if acc_id in running_clients:
                    continue
                
                logging.info(f"⚡ Spawning independent Telethon runtime client pipeline context for: {phone}")
                
                try:
                    client = TelegramClient(
                        StringSession(str(session_str).strip()), 
                        int(api_id), 
                        str(api_hash)
                    )
                    
                    await client.connect()
                    
                    if not await client.is_user_authorized():
                        logging.error(f"❌ Session string credentials rejected by server infrastructure for phone mapping index: {phone}")
                        await client.disconnect()
                        continue
                        
                    running_clients[acc_id] = client
                    asyncio.create_task(register_telegram_listeners(acc_id, phone, client))
                    
                except Exception as client_err:
                    logging.error(f"Failed to initiate background connectivity channels for user entry {phone}: {client_err}")
                    
        except Exception as main_err:
            logging.error(f"Poller system loop met database backend connection errors: {main_err}")
            
        await asyncio.sleep(8)

# --- SYSTEM PROCESS ENTRY POINT ---
async def main():
    # FIX: Run table generation immediately at startup before looping begins
    init_db()
    logging.info("🚀 Sky Cloud Automation Worker Loop Initialized successfully. Watching deployment channels...")
    await check_active_orders_pipeline()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("👋 Background worker process stopped safely.")
