# main.py
import os
import asyncio
import time
from dhanhq import dhanhq
from telegram import Bot
from telegram.error import TelegramError
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Commodity symbols - MCX
COMMODITIES = {
    "GOLD": {"exchange": "MCX", "security_id": "114"},
    "SILVER": {"exchange": "MCX", "security_id": "229"},
    "CRUDE OIL": {"exchange": "MCX", "security_id": "236"},
    "NATURAL GAS": {"exchange": "MCX", "security_id": "235"},
    "COPPER": {"exchange": "MCX", "security_id": "256"}
}

class DhanTelegramBot:
    def __init__(self):
        if not all([DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
            raise ValueError("Missing required environment variables!")
        
        self.dhan = dhanhq(DHAN_ACCESS_TOKEN)
        self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID
        self.last_prices = {}
        logger.info("Bot initialized successfully")
    
    async def get_ltp(self, security_id, exchange):
        """Get Latest Traded Price from DhanHQ"""
        try:
            quote = self.dhan.get_ltp_data(
                exchange_segment=exchange,
                security_id=security_id
            )
            if quote and 'data' in quote:
                ltp = quote['data'].get('LTP', 0)
                return ltp
            return None
        except Exception as e:
            logger.error(f"Error fetching LTP for {security_id}: {e}")
            return None
    
    async def format_message(self, prices):
        """Format price message for Telegram"""
        timestamp = datetime.now().strftime("%d-%m-%Y %I:%M %p")
        
        message = f"📊 *Commodity Prices*\n"
        message += f"🕒 {timestamp}\n"
        message += "━━━━━━━━━━━━━━━━\n\n"
        
        for commodity, data in prices.items():
            if data['price'] is not None:
                # Calculate change
                change = ""
                if commodity in self.last_prices and self.last_prices[commodity]:
                    diff = data['price'] - self.last_prices[commodity]
                    if diff > 0:
                        change = f"📈 +{diff:.2f}"
                    elif diff < 0:
                        change = f"📉 {diff:.2f}"
                    else:
                        change = "➖ 0.00"
                
                message += f"*{commodity}*\n"
                message += f"₹ {data['price']:.2f} {change}\n\n"
        
        message += "━━━━━━━━━━━━━━━━"
        return message
    
    async def send_telegram_message(self, message):
        """Send message to Telegram"""
        try:
            await self.telegram_bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='Markdown'
            )
            logger.info(f"Message sent at {datetime.now().strftime('%I:%M:%S %p')}")
        except TelegramError as e:
            logger.error(f"Telegram Error: {e}")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
    
    async def fetch_all_prices(self):
        """Fetch prices for all commodities"""
        prices = {}
        for name, details in COMMODITIES.items():
            ltp = await self.get_ltp(
                details['security_id'],
                details['exchange']
            )
            prices[name] = {
                'price': ltp,
                'exchange': details['exchange']
            }
            await asyncio.sleep(0.5)  # Small delay between requests
        return prices
    
    async def run(self):
        """Main bot loop"""
        logger.info("🤖 DhanHQ Commodity Bot Started!")
        logger.info(f"📤 Sending updates every 1 minute to Chat ID: {self.chat_id}")
        
        # Send startup message
        try:
            await self.telegram_bot.send_message(
                chat_id=self.chat_id,
                text="✅ Bot started successfully!\n\n📊 You will receive commodity price updates every minute."
            )
        except Exception as e:
            logger.error(f"Failed to send startup message: {e}")
        
        while True:
            try:
                # Fetch current prices
                current_prices = await self.fetch_all_prices()
                
                # Format and send message
                message = await self.format_message(current_prices)
                await self.send_telegram_message(message)
                
                # Update last prices
                for commodity, data in current_prices.items():
                    if data['price'] is not None:
                        self.last_prices[commodity] = data['price']
                
                # Wait for 1 minute
                await asyncio.sleep(60)
                
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                await asyncio.sleep(60)

# Run the bot
if __name__ == "__main__":
    try:
        bot = DhanTelegramBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise
