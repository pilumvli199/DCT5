# main.py
import os
import asyncio
import time
from dhanhq import dhanhq  # keep this — your code already uses dhanhq(...)
# NOTE: removed "from dhanhq import DhanEnv" because that import doesn't exist in installed package
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
# Keep using the dhanhq constants if available; fall back safely if not.
def _get_exchange_constant(module, name):
    # try module.NAME, then module.Exchange.NAME
    if hasattr(module, name):
        return getattr(module, name)
    exch = getattr(module, "Exchange", None)
    if exch and hasattr(exch, name):
        return getattr(exch, name)
    return name  # fallback to raw name; the package might accept string in some apis

MCX_CONST = _get_exchange_constant(dhanhq, "MCX")

COMMODITIES = {
    "GOLD": {"exchange": MCX_CONST, "security_id": "114"},
    "SILVER": {"exchange": MCX_CONST, "security_id": "229"},
    "CRUDE OIL": {"exchange": MCX_CONST, "security_id": "236"},
    "NATURAL GAS": {"exchange": MCX_CONST, "security_id": "235"},
    "COPPER": {"exchange": MCX_CONST, "security_id": "256"}
}

class DhanTelegramBot:
    def __init__(self):
        if not all([DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
            raise ValueError("Missing required environment variables!")
        
        # instantiate dhanhq client
        # many dhanhq versions accept (client_id, access_token) or a dict — you already used this pattern
        self.dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID
        self.last_prices = {}
        logger.info("Bot initialized successfully")
    
    async def get_ltp(self, security_id, exchange):
        """Get Latest Traded Price from DhanHQ"""
        try:
            # Version-agnostic attempt: try common method names / argument names
            # Primary attempt: marketfeed.get_ltp(exchange_segment=..., security_id=...)
            # If that fails, try get_ltp(exchange=..., token=..., id=...)
            response = None
            try:
                response = self.dhan.marketfeed.get_ltp(
                    exchange_segment=exchange,
                    security_id=security_id
                )
            except Exception:
                # second attempt with alternate arg names
                response = self.dhan.marketfeed.get_ltp(
                    exchange=exchange,
                    security_id=security_id
                )
            # response may be dict or object; handle common shapes
            if response is None:
                return None
            if isinstance(response, dict) and 'data' in response:
                ltp_data = response['data']
                if isinstance(ltp_data, dict):
                    return _safe_float(ltp_data.get('LTP'))
                elif isinstance(ltp_data, list) and len(ltp_data) > 0:
                    return _safe_float(ltp_data[0].get('LTP'))
            # sometimes the client returns the LTP directly or under different key
            if isinstance(response, dict) and 'LTP' in response:
                return _safe_float(response.get('LTP'))
            # lastly, if response is a number
            if isinstance(response, (int, float)):
                return float(response)
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
                if commodity in self.last_prices and self.last_prices[commodity] is not None:
                    diff = data['price'] - self.last_prices[commodity]
                    if diff > 0:
                        change = f"📈 +{diff:.2f}"
                    elif diff < 0:
                        change = f"📉 {diff:.2f}"
                    else:
                        change = "➖ 0.00"
                
                message += f"*{commodity}*\n"
                message += f"₹ {data['price']:.2f} {change}\n\n"
            else:
                message += f"*{commodity}*\n"
                message += f"_Price unavailable_\n\n"
        
        message += "━━━━━━━━━━━━━━━━"
        return message
    
    async def send_telegram_message(self, message):
        """Send message to Telegram"""
        try:
            # python-telegram-bot v20 Bot.send_message is a coroutine (awaitable)
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

def _safe_float(val):
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None

# Run the bot
if __name__ == "__main__":
    try:
        bot = DhanTelegramBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise
