# main.py
import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional

# dhanhq imports (best-effort)
try:
    from dhanhq import DhanContext, dhanhq
except Exception:
    try:
        from dhanhq import dhanhq  # type: ignore
        DhanContext = None  # type: ignore
    except Exception:
        dhanhq = None  # type: ignore
        DhanContext = None  # type: ignore

import httpx
from telegram import Bot
from telegram.error import TelegramError

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Environment
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    logger.error("Missing env vars. Please set DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID")
    raise SystemExit("Missing env vars")

# Dhan REST base
DHAN_REST_BASE = "https://api.dhan.co/v2"
MCX_REST_SEGMENT = "MCX_COMM"  # documented segment used by Dhan REST

# Commodities mapping (security IDs you provided). If these are wrong, enable instruments validation below.
COMMODITIES = {
    "GOLD": {"security_id": 114},
    "SILVER": {"security_id": 229},
    "CRUDE OIL": {"security_id": 236},
    "NATURAL GAS": {"security_id": 235},
    "COPPER": {"security_id": 256}
}

# Toggle: validate instruments at startup (calls instruments endpoint to confirm IDs exist)
VALIDATE_INSTRUMENTS_AT_START = True

# HTTP client timeout & backoff settings
HTTP_TIMEOUT = 10.0
MAX_BACKOFF_RETRIES = 4
BASE_BACKOFF_SECONDS = 1.0

# Helpers
def _safe_float(val) -> Optional[float]:
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None

def format_message(prices: Dict[str, Optional[float]], last_prices: Dict[str, float]) -> str:
    timestamp = datetime.now().strftime("%d-%m-%Y %I:%M %p")
    message = f"📊 *Commodity Prices*\n"
    message += f"🕒 {timestamp}\n"
    message += "━━━━━━━━━━━━━━━━\n\n"
    for name, price in prices.items():
        if price is None:
            message += f"*{name}*\n_Price unavailable_\n\n"
        else:
            change = ""
            if name in last_prices and last_prices[name] is not None:
                diff = price - last_prices[name]
                if diff > 0:
                    change = f"📈 +{diff:.2f}"
                elif diff < 0:
                    change = f"📉 {diff:.2f}"
                else:
                    change = "➖ 0.00"
            message += f"*{name}*\n₹ {price:.2f} {change}\n\n"
    message += "━━━━━━━━━━━━━━━━"
    return message

class DhanTelegramBot:
    def __init__(self):
        self.dhan = None
        # Try instantiate library client if present (best-effort). Not necessary for REST fallback.
        if dhanhq is not None:
            try:
                if 'DhanContext' in globals() and DhanContext is not None:
                    ctx = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
                    self.dhan = dhanhq(ctx)
                    logger.info("dhanhq client created via DhanContext")
                else:
                    # try common patterns
                    try:
                        self.dhan = dhanhq({'client_id': DHAN_CLIENT_ID, 'access_token': DHAN_ACCESS_TOKEN})
                        logger.info("dhanhq client created via dict")
                    except Exception:
                        try:
                            self.dhan = dhanhq(DHAN_ACCESS_TOKEN)
                            logger.info("dhanhq client created via token-only")
                        except Exception:
                            logger.info("dhanhq present but couldn't instantiate with known signatures")
            except Exception as e:
                logger.warning(f"dhanhq instantiate warning: {e}")

        # HTTPX client shared
        self.http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
        self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID
        self.last_prices: Dict[str, Optional[float]] = {}

        # local commodities structure (copy of COMMODITIES) so we can replace IDs if validation runs
        self.commodities = {k: v.copy() for k, v in COMMODITIES.items()}

    async def close(self):
        await self.http_client.aclose()

    # Optional: validate instruments by calling instruments endpoint once at startup.
    async def validate_instruments(self):
        """
        Optional helper that tries to confirm the provided security_ids exist.
        This will try a documented endpoint. If not available or fails, silently continue.
        """
        try:
            # many dhanhq deployments have an instruments endpoint; this is best-effort
            url = f"{DHAN_REST_BASE}/marketfeed/instruments"
            headers = {
                "accept": "application/json",
                "access-token": DHAN_ACCESS_TOKEN,
                "client-id": str(DHAN_CLIENT_ID)
            }
            # We supply the segment to narrow results - may not be supported by all accounts.
            payload = {"segment": MCX_REST_SEGMENT}
            resp = await self.http_client.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                logger.debug(f"Instruments validation non-200: {resp.status_code} {resp.text}")
                return
            j = resp.json()
            data = j.get("data") or {}
            # Data may be list or dict - do a cautious scan
            found_ids = set()
            if isinstance(data, list):
                for item in data:
                    sid = item.get("id") or item.get("security_id") or item.get("instrument_token")
                    name = item.get("symbol") or item.get("tradingsymbol") or item.get("name")
                    if sid is not None and name:
                        found_ids.add(int(sid))
            elif isinstance(data, dict):
                # sometimes API returns id->object mapping
                for k, v in data.items():
                    try:
                        found_ids.add(int(k))
                    except Exception:
                        pass
            # If we found nothing, just return
            if not found_ids:
                logger.debug("Instruments validation: no instrument ids found in response")
                return
            # Compare with our list
            for cname, meta in self.commodities.items():
                sid = meta.get("security_id")
                if int(sid) not in found_ids:
                    logger.warning(f"Validated instruments: configured id {sid} for {cname} not found in instruments response")
                else:
                    logger.debug(f"Instrument validated: {cname} id {sid}")
        except Exception as e:
            logger.debug(f"Instruments validation failed: {e}")

    async def fetch_batch_ltp_rest(self, security_ids: List[int]) -> Dict[int, Optional[float]]:
        """
        Single batch POST to /marketfeed/ltp with payload { "MCX_COMM": [ids...] }.
        Respects rate-limit by exponential backoff on HTTP 429.
        Returns mapping security_id -> last_price (None if unavailable).
        """
        url = f"{DHAN_REST_BASE}/marketfeed/ltp"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": DHAN_ACCESS_TOKEN,
            "client-id": str(DHAN_CLIENT_ID)
        }
        payload = {MCX_REST_SEGMENT: security_ids}
        attempt = 0
        while attempt <= MAX_BACKOFF_RETRIES:
            try:
                resp = await self.http_client.post(url, headers=headers, json=payload)
            except Exception as e:
                logger.error(f"HTTP error when calling REST LTP: {e}")
                return {sid: None for sid in security_ids}
            logger.info(f"REST LTP HTTP {resp.status_code} for batch ids={security_ids}")
            if resp.status_code == 200:
                try:
                    j = resp.json()
                except Exception:
                    logger.warning("REST LTP: invalid JSON response")
                    return {sid: None for sid in security_ids}
                data = j.get("data") or {}
                results: Dict[int, Optional[float]] = {}
                seg = data.get(MCX_REST_SEGMENT) or {}
                # seg expected to be dict of id->object
                for sid in security_ids:
                    sec_obj = seg.get(str(sid)) or seg.get(int(sid)) or {}
                    if isinstance(sec_obj, dict):
                        lp = sec_obj.get("last_price") or sec_obj.get("lastTradedPrice") or sec_obj.get("last_price")
                        # if last_price is 0.0 treat as unavailable (common artifact)
                        if lp is None:
                            results[sid] = None
                        else:
                            f = _safe_float(lp)
                            if f is None or f == 0.0:
                                results[sid] = None
                            else:
                                results[sid] = f
                    else:
                        # fallback shallow scan
                        results[sid] = None
                logger.debug(f"REST batch parsed results: {results}")
                return results
            elif resp.status_code == 429:
                # rate limited -> backoff
                attempt += 1
                wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(f"REST LTP rate limited (429). Backoff attempt {attempt}. Waiting {wait}s")
                await asyncio.sleep(wait)
                continue
            else:
                logger.warning(f"REST LTP non-200 response: {resp.status_code} body: {resp.text}")
                # don't retry on other 4xx/5xx
                return {sid: None for sid in security_ids}
        # exhausted retries
        logger.error("REST LTP: exhausted retries due to rate limiting")
        return {sid: None for sid in security_ids}

    async def get_all_prices(self) -> Dict[str, Optional[float]]:
        """
        Fetch all commodity prices in one batch call and return mapping name->price (or None).
        """
        # prepare list of ids
        ids = []
        name_for_id = {}
        for name, meta in self.commodities.items():
            sid = int(meta["security_id"])
            ids.append(sid)
            name_for_id[sid] = name

        batch_result = await self.fetch_batch_ltp_rest(ids)

        prices: Dict[str, Optional[float]] = {}
        for sid in ids:
            name = name_for_id[sid]
            prices[name] = batch_result.get(sid)
        return prices

    async def send_telegram(self, message: str):
        try:
            maybe = self.telegram_bot.send_message(chat_id=self.chat_id, text=message, parse_mode="Markdown")
            if asyncio.iscoroutine(maybe):
                await maybe
            logger.info(f"Message sent at {datetime.now().strftime('%I:%M:%S %p')}")
        except TelegramError as e:
            logger.error(f"Telegram error: {e}")
        except Exception as e:
            logger.error(f"Error sending telegram: {e}")

    async def run(self):
        logger.info("Bot starting...")

        # optional instruments validation
        if VALIDATE_INSTRUMENTS_AT_START:
            await self.validate_instruments()

        # initial startup message
        try:
            maybe = self.telegram_bot.send_message(chat_id=self.chat_id, text="✅ Bot started successfully!\n\n📊 You will receive commodity price updates every minute.")
            if asyncio.iscoroutine(maybe):
                await maybe
        except Exception as e:
            logger.warning(f"Startup Telegram message failed: {e}")

        # main loop
        while True:
            try:
                prices = await self.get_all_prices()
                message = format_message(prices, self.last_prices)
                await self.send_telegram(message)
                # update last prices only for non-None values
                for name, val in prices.items():
                    if val is not None:
                        self.last_prices[name] = val
                await asyncio.sleep(60)
            except KeyboardInterrupt:
                logger.info("Stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected loop error: {e}")
                await asyncio.sleep(10)

# Entrypoint
if __name__ == "__main__":
    bot = DhanTelegramBot()
    try:
        asyncio.run(bot.run())
    finally:
        # best-effort cleanup
        try:
            asyncio.run(bot.close())
        except Exception:
            pass
