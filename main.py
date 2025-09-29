# main.py
import os
import asyncio
import logging
from datetime import datetime

# dhanhq client imports - tolerant to versions
try:
    from dhanhq import DhanContext, dhanhq
except Exception:
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

# Environment config
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Helpers
def _safe_float(val):
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None

def _get_exchange_constant(module, name):
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

# Commodity config (MCX)
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

        self.dhan = None
        if dhanhq is None:
            logger.error("dhanhq package not available. Check requirements.txt and installed packages.")
            raise ImportError("dhanhq package not found")

        # Try multiple instantiation patterns for dhanhq
        # 1) DhanContext -> dhanhq(DhanContext)
        if 'DhanContext' in globals() and DhanContext is not None:
            try:
                dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
                self.dhan = dhanhq(dhan_context)
                logger.info("dhanhq client created using DhanContext(dhan_client_id, access_token)")
            except Exception as e:
                logger.warning(f"DhanContext path failed: {e}")

        # 2) dict config
        if self.dhan is None:
            try:
                self.dhan = dhanhq({'client_id': DHAN_CLIENT_ID, 'access_token': DHAN_ACCESS_TOKEN})
                logger.info("dhanhq client created using dict config")
            except Exception as e:
                logger.debug(f"dict-config attempt failed: {e}")

        # 3) access token only
        if self.dhan is None:
            try:
                self.dhan = dhanhq(DHAN_ACCESS_TOKEN)
                logger.info("dhanhq client created using access token only")
            except Exception as e:
                logger.debug(f"access-token attempt failed: {e}")

        # 4) old two-arg signature
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
        """
        Get Latest Traded Price from DhanHQ (tolerant + debug).
        This function will log the raw response for debugging.
        """
        try:
            response = None

            # Try common synchronous call signatures first
            try:
                response = self.dhan.marketfeed.get_ltp(
                    exchange_segment=exchange,
                    security_id=security_id
                )
            except Exception as e:
                logger.debug(f"marketfeed.get_ltp(exchange_segment=..., security_id=...) failed for {security_id}: {e}")
                try:
                    response = self.dhan.marketfeed.get_ltp(
                        exchange=exchange,
                        security_id=security_id
                    )
                except Exception as e2:
                    logger.debug(f"marketfeed.get_ltp(exchange=..., security_id=...) failed for {security_id}: {e2}")
                    # maybe client exposes a direct method
                    try:
                        response = getattr(self.dhan, "get_ltp", None)
                        if callable(response):
                            response = response(exchange, security_id)
                        else:
                            response = None
                    except Exception as e3:
                        logger.debug(f"fallback get_ltp call failed: {e3}")
                        response = None

            # If coroutine was returned, await it
            if asyncio.iscoroutine(response):
                response = await response

            # DEBUG: log raw response so we can adapt parser
            logger.info(f"LTP raw response for {security_id}: {response}")

            if response is None:
                return None

            # Common shapes:
            # {'data': {...}} or {'data': [{...}]}
            if isinstance(response, dict) and 'data' in response:
                ltp_data = response['data']
                if isinstance(ltp_data, dict):
                    return _safe_float(ltp_data.get('LTP') or ltp_data.get('last_price') or ltp_data.get('ltp') or ltp_data.get('lastTradedPrice'))
                elif isinstance(ltp_data, list) and len(ltp_data) > 0:
                    item = ltp_data[0]
                    if isinstance(item, dict):
                        return _safe_float(item.get('LTP') or item.get('last_price') or item.get('ltp') or item.get('lastTradedPrice'))
            # Direct dict with LTP
            if isinstance(response, dict) and ('LTP' in response or 'ltp' in response or 'last_price' in response or 'lastTradedPrice' in response):
                return _safe_float(response.get('LTP') or response.get('ltp') or response.get('last_price') or response.get('lastTradedPrice'))
            # Numeric
            if isinstance(response, (int, float)):
                return float(response)
            # Object attributes
            if hasattr(response, 'LTP'):
                return _safe_float(getattr(response, 'LTP'))
            if hasattr(response, 'ltp'):
                return _safe_float(getattr(response, 'ltp'))
            if hasattr(response, 'last_price'):
                return _safe_float(getattr(response, 'last_price'))
            # Unknown shape
            return None
        except Exception as e:
            logger.error(f"Error fetching LTP for {security_id}: {e}")
            return None

    async def format_message(self, prices):
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
        try:
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
            await asyncio.sleep(0.5)
        return prices

    async def run(self):
        logger.info("🤖 DhanHQ Commodity Bot Started!")
        logger.info(f"📤 Sending updates every 1 minute to Chat ID: {self.chat_id}")

        # startup message
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

                for commodity, data in current_prices.items():
                    if data['price'] is not None:
                        self.last_prices[commodity] = data['price']

                await asyncio.sleep(60)
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        bot = DhanTelegramBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise
