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
        if 'DhanContext' in globals() and DhanContext is not None:
            try:
                dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
                self.dhan = dhanhq(dhan_context)
                logger.info("dhanhq client created using DhanContext(dhan_client_id, access_token)")
            except Exception as e:
                logger.warning(f"DhanContext path failed: {e}")

        if self.dhan is None:
            try:
                self.dhan = dhanhq({'client_id': DHAN_CLIENT_ID, 'access_token': DHAN_ACCESS_TOKEN})
                logger.info("dhanhq client created using dict config")
            except Exception as e:
                logger.debug(f"dict-config attempt failed: {e}")

        if self.dhan is None:
            try:
                self.dhan = dhanhq(DHAN_ACCESS_TOKEN)
                logger.info("dhanhq client created using access token only")
            except Exception as e:
                logger.debug(f"access-token attempt failed: {e}")

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

    async def _try_call(self, label, func, *args, **kwargs):
        """Helper that tries calling func, awaits if coroutine, logs result and returns it or None."""
        try:
            result = func(*args, **kwargs) if callable(func) else None
        except Exception as e:
            logger.debug(f"LTP attempt {label} raised exception during call: {e}")
            result = None

        # If function returned a callable (rare), call it
        if callable(result) and not asyncio.iscoroutine(result):
            try:
                result = result()
            except Exception as e:
                logger.debug(f"LTP attempt {label} inner call failed: {e}")
                result = None

        if asyncio.iscoroutine(result):
            try:
                result = await result
            except Exception as e:
                logger.debug(f"LTP attempt {label} awaiting failed: {e}")
                result = None

        # log raw
        logger.info(f"LTP attempt {label} raw response: {result}")
        return result

    async def get_ltp(self, security_id, exchange):
        """
        Very aggressive LTP fetcher:
        Tries a sequence of likely methods/signatures and logs every attempt.
        """
        try:
            attempts = []
            idx = 0

            # 1) marketfeed.get_ltp(exchange_segment=..., security_id=...)
            try:
                mf = getattr(self.dhan, "marketfeed", None)
                if mf:
                    attempts.append(("marketfeed.get_ltp(exchange_segment,security_id)",
                                     lambda: mf.get_ltp(exchange_segment=exchange, security_id=security_id)))
                    attempts.append(("marketfeed.get_ltp(exchange,security_id)",
                                     lambda: mf.get_ltp(exchange=exchange, security_id=security_id)))
                    attempts.append(("marketfeed.get_quote(exchange_segment,security_id)",
                                     lambda: mf.get_quote(exchange_segment=exchange, security_id=security_id)))
            except Exception:
                pass

            # 2) top-level helpers
            for name in ("get_ltp", "getQuote", "get_quote", "ltp", "get_instruments_ltp", "getInstrumentLTP"):
                if hasattr(self.dhan, name):
                    fn = getattr(self.dhan, name)
                    attempts.append((f"top.{name}(exchange,security_id)", lambda fn=fn: fn(exchange, security_id)))

            # 3) some libs expose dhanhq.market.get_ltp
            try:
                market = getattr(self.dhan, "market", None)
                if market:
                    attempts.append(("market.get_ltp(exchange,security_id)", lambda: market.get_ltp(exchange, security_id)))
                    attempts.append(("market.get_quote(exchange,security_id)", lambda: market.get_quote(exchange, security_id)))
            except Exception:
                pass

            # 4) try dhanhq.marketfeed.get_snapshot or similar
            try:
                if mf:
                    attempts.append(("marketfeed.get_snapshot(exchange,security_id)", lambda: mf.get_snapshot(exchange, security_id)))
            except Exception:
                pass

            # 5) last-resort: try call to .marketfeed (callable) if it's callable
            if callable(getattr(self.dhan, "marketfeed", None)):
                attempts.append(("dhan.marketfeed()", lambda: self.dhan.marketfeed()))

            # iterate attempts and return first parsable numeric LTP
            for label, func in attempts:
                idx += 1
                resp = await self._try_call(f"{idx} {label}", func)
                # parse common shapes
                if resp is None:
                    continue
                # common dict shapes with 'data'
                if isinstance(resp, dict) and 'data' in resp:
                    d = resp['data']
                    if isinstance(d, dict):
                        val = _safe_float(d.get('LTP') or d.get('ltp') or d.get('last_price') or d.get('lastTradedPrice') or d.get('lastTraded'))
                        if val is not None:
                            return val
                    elif isinstance(d, list) and d:
                        item = d[0]
                        if isinstance(item, dict):
                            val = _safe_float(item.get('LTP') or item.get('ltp') or item.get('last_price') or item.get('lastTradedPrice'))
                            if val is not None:
                                return val
                if isinstance(resp, dict):
                    val = _safe_float(resp.get('LTP') or resp.get('ltp') or resp.get('last_price') or resp.get('lastTradedPrice') or resp.get('lastTraded'))
                    if val is not None:
                        return val
                if isinstance(resp, (int, float)):
                    return float(resp)
                # attribute objects
                for attr in ("LTP", "ltp", "last_price", "lastTradedPrice", "lastTraded"):
                    if hasattr(resp, attr):
                        val = _safe_float(getattr(resp, attr))
                        if val is not None:
                            return val
                # if resp contains nested dicts, do a shallow search for numeric-like values
                if isinstance(resp, dict):
                    for k, v in resp.items():
                        if isinstance(v, (int, float)):
                            return float(v)
                        if isinstance(v, str) and v.replace('.', '', 1).isdigit():
                            return _safe_float(v)
                # otherwise continue to next attempt

            # if none of attempts returned
            logger.warning(f"No parsable LTP found for security_id={security_id}")
            return None

        except Exception as e:
            logger.error(f"Error in get_ltp for {security_id}: {e}")
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
            ltp = await self.get_ltp(details['security_id'], details['exchange'])
            prices[name] = {'price': ltp, 'exchange': details['exchange']}
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
