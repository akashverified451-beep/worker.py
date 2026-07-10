import os
import re
import logging
import asyncio
import psycopg # FIXED: Swapped from psycopg2 to psycopg
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from aiogram import Bot

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment configuration variables 
BOT_TOKEN = os.getenv("BOT_TOKEN", "8761162220:AAEsp3UI6Iv5x4y8k4tW9z33LVYFcLEnqlc")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=BOT_TOKEN)
active_clients = {}

def get_db_connection():
    """Establishes an isolated bridge line with the Render PostgreSQL engine."""
    return psycopg.connect(DATABASE_URL) # FIXED: Using new psycopg connection wrapper

async def handle_incoming_otp(phone_number: str, raw_text: str):
    """Processes intercepted patterns and routes parameters directly to the buyer."""
    # Fast regex match for standard login patterns
    if "Telegram code" in raw_text or "is your login code" in raw_text:
        m = re.search(r'\b\d{5,6}\b', raw_text)
        if m:
            otp = m.group(0)
            logging.info(f"Successfully caught OTP pattern string: {otp} for number: {phone_number}")
            
            # Identify which user bought this phone number and is waiting for the code
            try:
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
            except Exception as sql_err:
                logging.error(f"Database query operation error inside OTP handler: {sql_err}")

async def sync_and_start_scraper_pool():
    """Loops through database inventory rows and monitors active account pipelines simultaneously."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Pull all active string sessions from the database
                cur.execute("SELECT phone_number, api_id, api_hash, string_session FROM available_accounts")
                accounts = cur.fetchall()
    except Exception as db_err:
        logging.error(f"Failed to fetch sessions from database: {db_err}")
        return

    for acc in accounts:
        phone, api_id, api_hash, session_str = acc
        
        # If this number is already being monitored in memory, skip it
        if phone in active_clients:
            continue
            
        logging.info(f"Connecting to database account: {phone}...")
        try:
            # Initialize Telethon client using the stored string session
            client = TelegramClient(StringSession(session_str), int(api_id), api_hash)
            await client.start()
            
            # Set up dynamic event handler attachment pattern strings matching specific reference rows
            @client.on(events.NewMessage(incoming=True))
            async def handler(event, p=phone):
                await handle_incoming_otp(p, event.raw_text)
                
            active_clients[phone] = client
            logging.info(f"Successfully hooked monitoring event thread on pipeline target: {phone}")
            
        except Exception as conn_err:
            logging.error(f"Failed to bootstrap session record configuration row for {phone}: {conn_err}")

async def main():
    logging.info("Starting isolated Telethon scraper engine...")
    
    # Run the initial connection routine for all uploaded accounts
    await sync_and_start_scraper_pool()
    
    # Keep running continuous verification loops active to pick up additions to rows dynamically
    while True:
        await asyncio.sleep(30)  # Checks for newly added /addnumber accounts every 30 seconds
        await sync_and_start_scraper_pool()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scraper background pool offline.")
