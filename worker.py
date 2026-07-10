import os
import asyncio
import logging
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

# --- OTP EXTRACTOR LOGIC ---
def extract_otp(message_text: str) -> str:
    """
    Utility script parsing module to capture numerical sequence verification thresholds.
    """
    if not message_text:
        return ""
    import re
    # Matches common 4, 5, or 6 digit codes typically transmitted by Telegram
    match = re.search(r'\b\d{4,6}\b', message_text)
    return match.group(0) if match else ""

# --- TELEGRAM INBOUND EVENT RECEPTOR ---
async def register_telegram_listeners(account_id, phone, client: TelegramClient):
    """
    Listens for live incoming messages on active tracking channels.
    """
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
                        # 1. Update active transaction pipeline state metrics
                        cur.execute("""
                            UPDATE active_orders 
                            SET status = 'COMPLETED' 
                            WHERE account_id = %s AND status = 'WAITING'
                        """, (account_id,))
                        
                        # 2. Mark specific structural phone catalog file index as sold
                        cur.execute("""
                            UPDATE available_accounts 
                            SET is_sold = TRUE 
                            WHERE id = %s
                        """, (account_id,))
                        
                        # 3. Securely deduct balance allocation requirements from customer wallet profile
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
                
                # Gracefully terminate network loop channels after successful validation receipt 
                await client.disconnect()
                running_clients.pop(account_id, None)
                
            except Exception as ex:
                logging.error(f"Failed to synchronize state update operations into cluster: {ex}")

# --- PROCESSING LOOP ROUTINE ---
async def check_active_orders_pipeline():
    """
    Scans internal tables for pending transactions, handling validation steps cleanly.
    """
    while True:
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Scan for new processing requests matching existing parameters safely
                    cur.execute("""
                        SELECT ao.account_id, ao.phone_number, aa.api_id, aa.api_hash, aa.string_session 
                        FROM active_orders ao
                        JOIN available_accounts aa ON ao.account_id = aa.id
                        WHERE ao.status = 'WAITING'
                    """)
                    pending_orders = cur.fetchall()
                    
            for order in pending_orders:
                acc_id, phone, api_id, api_hash, session_str = order
                
                # CRITICAL DEFENSIVE FIX: Skip structural entries with empty strings or NoneType instances
                if not session_str or str(session_str).strip() == "" or session_str == "None":
                    logging.warning(f"⚠️ Account ID {acc_id} ({phone}) contains an empty/corrupted session token string parameter. Skipping execution flow entirely.")
                    continue
                
                if acc_id in running_clients:
                    continue # Thread engine validation instance is already active
                
                logging.info(f"⚡ Spawning independent Telethon runtime client pipeline context for: {phone}")
                
                try:
                    # Initializing dynamic telethon storage buffers directly from clean, non-null properties
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
                    
                    # Spawn active validation listening hooks immediately
                    asyncio.create_task(register_telegram_listeners(acc_id, phone, client))
                    
                except Exception as client_err:
                    logging.error(f"Failed to initiate background connectivity channels for user entry {phone}: {client_err}")
                    
        except Exception as main_err:
            logging.error(f"Poller system loop met database backend connection errors: {main_err}")
            
        # Standard loop poll interval delay configuration
        await asyncio.sleep(8)

# --- SYSTEM PROCESS ENTRY POINT ---
async def main():
    logging.info("🚀 Sky Cloud Automation Worker Loop Initialized successfully. Watching deployment channels...")
    await check_active_orders_pipeline()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("👋 Background worker process stopped safely.")
