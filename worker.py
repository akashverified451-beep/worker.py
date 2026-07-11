import os
import re
import logging
import asyncio
import psycopg
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from aiogram import Bot

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment configuration variables Safely
BOT_TOKEN = os.getenv("BOT_TOKEN", "8761162220:AAGN9YLH9ykLKDtvewuJydI3efFkW5grAQo")

# Fixed: Read from DATABASE_URL env var, or fallback safely to the connection string directly if missing
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sky_otp_db_user:oYom3EdpOfLCpLSGlc2dAV8qY9zw2oot@dpg-d98lkf5aeets73f2po2g-a/sky_otp_db")

bot = Bot(token=BOT_TOKEN)
active_clients = {}

def get_db_connection():
    """Establishes an isolated bridge line with the Render PostgreSQL engine."""
    return psycopg.connect(DATABASE_URL)

async def handle_incoming_otp(phone_number: str, raw_text: str):
    """Processes intercepted patterns and routes parameters directly to the buyer."""
    if "Telegram code" in raw_text or "is your login code" in raw_text:
        m = re.search(r'\b\d{5,6}\b', raw_text)
        if m:
            otp = m.group(0)
            logging.info(f"Successfully caught OTP pattern string: {otp} for number: {phone_number}")
            
            # Identify which user bought this phone number and is waiting for the code
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT uid FROM active_orders WHERE phone_number = %s AND status = 'WAITING' LIMIT 1", 
                        (phone_number,)
                    )
                    row = cur.fetchone()
                    
                    if row:
                        buyer_uid = row[0]
                        # Mark transaction complete to prevent duplicates
                        cur.execute("UPDATE active_orders SET status = 'DELIVERED' WHERE phone_number = %s", (phone_number,))
                        conn.commit()
                        
                        try:
                            # Deliver straight to the active consumer chat window instantly
                            await bot.send_message(
                                chat_id=buyer_uid,
                                text=f"🔑 <b>Your Telegram Activation Code Has Arrived!</b>\n\n"
                                     f"🔢 <b>Number:</b> <code>{phone_number}</code>\n"
                                     f"⚡ <b>Code:</b> <code>{otp}</code>\n\n"
                                     f"✨ <i>Thank you for purchasing from SKY OTP BOT!</i>",
                                parse_mode="HTML"
                            )
                            logging.info(f"OTP successfully routed to user ID: {buyer_uid}")
                        except Exception as d_err:
                            logging.error(f"Failed direct routing delivery stack call execution: {d_err}")

async def start_account_client(phone_number, api_id, api_hash, session_str):
    """Starts a Telethon client session for an account to monitor updates."""
    if phone_number in active_clients:
        return
        
    try:
        client = TelegramClient(StringSession(session_str), int(api_id), api_hash)
        await client.start()
        
        @client.on(events.NewMessage(incoming=True))
        async def my_event_handler(event):
            await handle_incoming_otp(phone_number, event.raw_text)
            
        active_clients[phone_number] = client
        logging.info(f"Monitoring started for account: {phone_number}")
    except Exception as e:
        logging.error(f"Failed to start client for {phone_number}: {e}")

async def sync_and_start_scraper_pool():
    """Loops through database inventory rows and monitors active account pipelines simultaneously."""
    logging.info("Starting isolated Telethon scraper engine...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Pull all active string sessions from the database
                cur.execute("SELECT phone_number, api_id, api_hash, string_session FROM available_accounts")
                rows = cur.fetchall()
                
                for row in rows:
                    p_num, api_id, api_hash, session_str = row[0], row[1], row[2], row[3]
                    if session_str:
                        asyncio.create_task(start_account_client(p_num, api_id, api_hash, session_str))
                        
    except Exception as db_err:
        logging.error(f"Failed to fetch sessions from database: {db_err}")

async def main():
    # Continually sync and keep the pool monitoring session updates alive
    while True:
        await sync_and_start_scraper_pool()
        await asyncio.sleep(30) # Refresh account list every 30 seconds

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scraper pool engine shut down safely.")
