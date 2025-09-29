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

# httpx for REST fallback
import httpx

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

# Use documented MCX segment name for REST (Annexure says: MCX_COMM)
MCX_REST_SEGMENT = "MCX_COMM"
MCX_CONST = _get_exchange_constant(dhanhq, "MCX")  # for client lib usage (if present)

COMMODITIES = {
    "GOLD": {"exchange": MCX_CONST, "security_id": "114"},
    "SILVER": {"exchange": MCX_CONST, "security_id": "229"},
    "CRUDE OIL": {"exchange": MCX_CONST, "security_id": "236"},
    "NATURAL GAS": {"exchange": MCX_CONST, "security_id": "235"},
    "COPPER": {"exchange": MCX_CONST, "security_id": "256"}
}

# REST base
DHAN_REST_BASE = "https://api.dhan.co/v2"

class DhanTelegramBot:
    def __init__(self):
        if not all([DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
            raise ValueError("Missing required environment variables! Set DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID")

        self.dhan = None
        if dhanhq is None:
            logger.warning("dhanhq package not available or import failed. Will use REST fallback.")

        # Try multiple instantiation patterns for dhanhq (if package present)
        if dhanhq is not None:
            if 'DhanContext' in globals() and DhanContext is not None:
                try:
                    dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
                    self.dhan = dhanhq(dhan_context)
                    logger.info("dhanhq client created using DhanContext(dhan_client_id, access_token)")
                except Exception as e:
                    logger.debug(f"DhanContext path failed: {e}")

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
                logger.info("dhanhq is present but unable to instantiate with known patterns; REST fallback will be used.")

        # Telegram bot
        self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID
        self.last_prices = {}
        logger.info("Bot initialized successfully")

    # -------------------------
    # REST fallback: fetch LTP from Dhan API directly
    # -------------------------
    async def fetch_ltp_via_rest(self, security_id):
        """
        Call POST /marketfeed/ltp with JSON body { "MCX_COMM": [security_id] }
        Headers required: access-token, client-id
        """
        url = f"{DHAN_REST_BASE}/marketfeed/ltp"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": DHAN_ACCESS_TOKEN,
            "client-id": str(DHAN_CLIENT_ID)
        }
        payload = {MCX_REST_SEGMENT: [int(security_id)]}  # docs expect integer security IDs in list

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, headers=headers, json=payload)
                logger.info(f"REST LTP HTTP {resp.status_code} for security_id={security_id}")
                if resp.status_code != 200:
                    logger.warning(f"REST LTP non-200 response: {resp.status_code} body: {resp.text}")
                    return None
                j = resp.json()
                logger.debug(f"REST LTP raw json for {security_id}: {j}")
                # docs show response: {"data": {"MCX_COMM": {"<id>": {"last_price": 123.45}}}, "status":"success"}
                data = j.get("data") or {}
                seg = data.get(MCX_REST_SEGMENT) or {}
                sec_obj = seg.get(str(security_id)) or seg.get(int(security_id)) or {}
                if isinstance(sec_obj, dict):
                    lp = sec_obj.get("last_price") or sec_obj.get("last_price")
                    if lp is None:
                        # sometimes they nest as {"114": {"last_price": 123}} or {"MCX_COMM": {"114": {"last_price":123}}}
                        # Already tried above; fallback: shallow scan
                        for v in sec_obj.values():
                            if isinstance(v, (int, float)):
                                return float(v)
                    return _safe_float(lp)
                # fallback shallow search
                if isinstance(data, dict):
                    for segk, segv in data.items():
                        if isinstance(segv, dict) and str(security_id) in segv:
                            lp = segv[str(security_id)].get("last_price")
                            if lp is not None:
                                return _safe_float(lp)
                return None
        except Exception as e:
            logger.error(f"REST LTP fetch error for {security_id}: {e}")
            return None

    # -------------------------
    # attempt using dhanhq client (if available) - tolerant to shapes
    # -------------------------
    async def get_ltp_from_client(self, security_id, exchange):
        """
        Attempts multiple common client calls to fetch LTP (synchronous or coroutine).
        Returns float LTP or None.
        """
        if self.dhan is None:
            return None

        # try a few likely attributes/methods
        attempts = []

        try:
            mf = getattr(self.dhan, "marketfeed", None)
            if mf:
                attempts.append(lambda: mf.get_ltp(exchange_segment=exchange, security_id=security_id))
                attempts.append(lambda: mf.get_ltp(exchange=exchange, security_id=security_id))
                attempts.append(lambda: mf.get_quote(exchange_segment=exchange, security_id=security_id))
                attempts.append(lambda: mf.get_snapshot(exchange, security_id))
        except Exception:
            pass

        # top-level helpers
        for name in ("get_ltp", "getQuote", "get_quote", "ltp", "get_instruments_ltp", "getInstrumentLTP"):
            fn = getattr(self.dhan, name, None)
            if callable(fn):
                attempts.append(lambda fn=fn: fn(exchange, security_id))

        # market.* variations
        market = getattr(self.dhan, "market", None)
        if market:
            attempts.append(lambda: market.get_ltp(exchange, security_id))
            attempts.append(lambda: market.get_quote(exchange, security_id))

        for attempt_fn in attempts:
            try:
                res = attempt_fn()
            except Exception as e:
                logger.debug(f"Client attempt raised: {e}")
                res = None

            # await if coroutine
            if asyncio.iscoroutine(res):
                try:
                    res = await res
                except Exception as e:
                    logger.debug(f"Awaiting client attempt failed: {e}")
                    res = None

            logger.debug(f"Client attempt raw response for {security_id}: {res}")

            # parse common shapes:
            if res is None:
                continue
            if isinstance(res, dict) and 'data' in res:
                d = res['data']
                # try nested shapes
                if isinstance(d, dict):
                    # If data contains segment -> id -> {'last_price': ..}
                    for segk, segv in d.items():
                        if isinstance(segv, dict) and str(security_id) in segv:
                            lp = segv[str(security_id)].get("last_price")
                            if lp is not None:
                                return _safe_float(lp)
                    # if 'LTP' directly inside
                    for k, v in d.items():
                        if isinstance(v, (int, float)):
                            return float(v)
                # If data itself is a dict with 'last_price'
                lp = d.get("last_price") if isinstance(d, dict) else None
                if lp is not None:
                    return _safe_float(lp)
            if isinstance(res, dict):
                lp = res.get("last_price") or res.get("LTP") or res.get("ltp")
                if lp is not None:
                    return _safe_float(lp)
            if isinstance(res, (int, float)):
                return float(res)
            # object attributes
            for attr in ("last_price", "LTP", "ltp"):
                if hasattr(res, attr):
                    return _safe_float(getattr(res, attr))
        return None

    # unified get_ltp: try client then REST fallback
    async def get_ltp(self, security_id, exchange):
        # try client first
        try:
            lp = await self.get_ltp_from_client(security_id, exchange)
            if lp is not None:
                logger.info(f"LTP from client for {security_id}: {lp}")
                return lp
        except Exception as e:
            logger.debug(f"Client get_ltp raised: {e}")

        # REST fallback
        lp = await self.fetch_ltp_via_rest(security_id)
        if lp is not None:
            logger.info(f"LTP from REST for {security_id}: {lp}")
            return lp

        logger.warning(f"No LTP found for security_id={security_id}")
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
