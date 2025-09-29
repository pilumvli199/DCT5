# main.py
import os
import asyncio
import time
import logging
from datetime import datetime

# dhanhq client imports - we try to be tolerant to different versions
try:
    # newer versions may expose DhanContext
    from dhanhq import DhanContext, dhanhq
except Exception:
    # fallback: try to import dhanhq only
    try:
        from dhanhq import dhanhq  # type: ignore
        DhanContext = None  # type: ignore
    except Exception:
        dhanhq = None  # type: ignore
        DhanContext = None  # type: ignore

# Telegram
from telegram import Bot
from telegram.error import TelegramError

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

# Helper functions
def _safe_float(val):
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None

def _get_exchange_constant(module, name):
    """Try to get exchange constant (MCX) from dhanhq module in various shapes."""
    try:
        if module is None:
            return name
        if hasattr(module, name):
            return getattr(module, name)
        exch = getattr(module, "Exchange", None)
        if exch and hasattr(exch, name):
            return getattr(exch, name)
    except Exception:
        pass
    return name

# Commodity symbols - MCX
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
            raise ValueError("Missing required environment variables! Set DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID")

        # Instantiate dhanhq client in a robust, version-tolerant way
        self.dhan = None
        if dhanhq is None:
            logger.error("dhanhq package not available. Check requirements.txt and installed packages.")
            raise ImportError("dhanhq package not found")

        # attempt #1: prefer DhanContext -> dhanhq(DhanContext)
        if 'DhanContext' in globals() and DhanContext is not None:
            try:
                dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
                self.dhan = dhanhq(dhan_context)
                logger.info("dhanhq client created using DhanContext(dhan_client_id, access_token)")
            except Exception as e:
                logger.warning(f"DhanContext path failed: {e}")

        # attempt #2: pass a dict/config object
        if self.dhan is None:
            try:
                self.dhan = dhanhq({'client_id': DHAN_CLIENT_ID, 'access_token': DHAN_ACCESS_TOKEN})
                logger.info("dhanhq client created using dict config")
            except Exception as e:
                logger.debug(f"dict-config attempt failed: {e}")

        # attempt #3: pass only access token (some older/simple wrappers accept it)
        if self.dhan is None:
            try:
                self.dhan = dhanhq(DHAN_ACCESS_TOKEN)
                logger.info("dhanhq client created using access token only")
            except Exception as e:
                logger.debug(f"access-token attempt failed: {e}")

        # attempt #4: try the old two-arg signature (in case)
        if self.dhan is None:
            try:
                self.dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
                logger.info("dhanhq client created using (client_id, access_token)")
            except Exception as e:
                logger.debug(f"(client_id, access_token) attempt failed: {e}")

        if self.dhan is None:
            logger.error("Failed to instantiate dhanhq client with any known signature. Check dhanhq version and docs.")
            raise RuntimeError("dhanhq instantiation failed")

        # Telegram bot
        self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID
        self.last_prices = {}
        logger.info("Bot initialized successfully")

    async def get_ltp(self, security_id, exchange):
        """Get Latest Traded Price from DhanHQ (tolerant to response shape)."""
        try:
            response = None
            # try commonly used call signature(s)
            try:
                # most clients use marketfeed.get_ltp(exchange_segment=..., security_id=...)
                response = self.dhan.marketfeed.get_ltp(
                    exchange_segment=exchange,
                    security_id=security_id
                )
            except Exception:
                try:
                    # alternative argnames
                    response = self.dhan.marketfeed.get_ltp(
                        exchange=exchange,
                        security_id=security_id
                    )
                except Exception:
                    # some clients return coroutine for api calls
                    try:
                        resp_coro = self.dhan.marketfeed.get_ltp(exchange_segment=exchange, security_id=security_id)
                        if asyncio.iscoroutine(resp_coro):
                            response = await resp_coro
                    except Exception:
                        response = None

            if response is None:
                return None

            # common shapes:
            # 1) {'data': {...}}  2) {'data': [{...}]}  3) {'LTP': 12345}  4) number
            if isinstance(response, dict) and 'data' in response:
                ltp_data = response['data']
                if isinstance(ltp_data, dict):
                    return _safe_float(ltp_data.get('LTP') or ltp_data.get('last_price') or ltp_data.get('ltp'))
                elif isinstance(ltp_data, list) and len(ltp_data) > 0:
                    item = ltp_data[0]
                    if isinstance(item, dict):
                        return _safe_float(item.get('LTP') or item.get('last_price') or item.get('ltp'))
            if isinstance(response, dict) and 'LTP' in response:
                return _safe_float(response.get('LTP'))
            if isinstance(response, (int, float)):
                return float(response)
            # sometimes response is an object with attributes
            if hasattr(response, 'LTP'):
                return _safe_float(getattr(response, 'LTP'))
            if hasattr(response, 'ltp'):
                return _safe_float(getattr(response, 'ltp'))
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
                message += f"*{commodity}*\n_Price unavailable_\n\n"
        
        message += "━━━━━━━━━━━━━━━━"
        return message
    
    async def send_telegram_message(self, message):
        """Send message to Telegram"""
        try:
            # python-telegram-bot v20's Bot.send_message can be awaited
            maybe_coro = self.telegram_bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='Markdown'
            )
            if asyncio.iscoroutine(maybe_coro):
                await maybe_coro
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
            await asyncio.sleep(0.5)  # small delay between requests
        return prices
    
    async def run(self):
        """Main bot loop"""
        logger.info("🤖 DhanHQ Commodity Bot Started!")
        logger.info(f"📤 Sending updates every 1 minute to Chat ID: {self.chat_id}")
        
        # Send startup message
        try:
            maybe_coro = self.telegram_bot.send_message(
                chat_id=self.chat_id,
                text="✅ Bot started successfully!\n\n📊 You will receive commodity price updates every minute."
            )
            if asyncio.iscoroutine(maybe_coro):
                await maybe_coro
        except Exception as e:
            logger.error(f"Failed to send startup message: {e}")
        
        while True:
            try:
                current_prices = await self.fetch_all_prices()
                message = await self.format_message(current_prices)
                await self.send_telegram_message(message)
                
                # update last prices
                for commodity, data in current_prices.items():
                    if data['price'] is not None:
                        self.last_prices[commodity] = data['price']
                
                # wait for 1 minute
                await asyncio.sleep(60)
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                # short backoff on error
                await asyncio.sleep(30)

# Entrypoint
if __name__ == "__main__":
    try:
        bot = DhanTelegramBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise
