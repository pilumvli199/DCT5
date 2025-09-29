# main.py
"""
DhanHQ WebSocket -> Telegram bot with:
- instruments refresh (REST best-effort)
- front-month auto-pick per commodity
- automatic re-subscribe (reconnect) when mapping changes
- fallback: persist last-good prices to disk
"""
import os
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Any

import websockets
import httpx
from telegram import Bot
from telegram.error import TelegramError

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("dhan-telegram-ws")

# ---------- Environment ----------
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    logger.error("Missing env vars. Set DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID")
    raise SystemExit("Missing required environment variables")

# ---------- Config ----------
# WebSocket base and query-template (per Dhan docs / earlier working handshake)
WS_BASE = "wss://api-feed.dhan.co"
WS_QUERY_TEMPLATE = "?version=2&token={token}&clientId={clientId}&authType=2"

# REST base for instrument discovery / fallback
DHAN_REST_BASE = "https://api.dhan.co/v2"

# segment used for MCX commodity instruments
MCX_SEGMENT = "MCX_COMM"

# Commodities (underlying names) - we will auto-pick front-month contract for each
COMMODITY_UNDERLYINGS = {
    "GOLD": "GOLD",
    "SILVER": "SILVER",
    "CRUDE OIL": "CRUDEOIL",      # sometimes symbol naming differs; we'll match case-insensitive substring
    "NATURAL GAS": "NATURALGAS",
    "COPPER": "COPPER"
}

# If REST instruments endpoint not available on your account, code falls back to the current/default security IDs below.
# These are initial defaults — they'll be replaced by instrument discovery if possible.
INITIAL_SECURITY_IDS = {
    "GOLD": 114,
    "SILVER": 229,
    "CRUDE OIL": 236,
    "NATURAL GAS": 235,
    "COPPER": 256
}

# Backoff & intervals
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 60.0
SEND_INTERVAL = int(os.getenv("SEND_INTERVAL", "60"))          # Telegram summary interval (seconds)
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "3600"))  # Instrument discovery interval (seconds)
CACHE_FILE = os.getenv("LAST_PRICE_CACHE", "last_prices.json") # path to persist last-good prices

# Rate limiting tolerance for REST calls
HTTP_TIMEOUT = 10.0

# ---------- Helpers ----------
def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def _now_str() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%d-%m-%Y %I:%M %p %Z")

def save_cache(path: str, data: Dict[str, Any]):
    try:
        with open(path, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.warning(f"Failed to save cache {path}: {e}")

def load_cache(path: str) -> Dict[str, Any]:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load cache {path}: {e}")
        return {}

def format_telegram_message(latest: Dict[int, Optional[float]], prev_sent: Dict[int, Optional[float]], mapping: Dict[str, int]) -> str:
    timestamp = _now_str()
    msg = f"📊 *Commodity Prices*\n"
    msg += f"🕒 {timestamp}\n"
    msg += "━━━━━━━━━━━━━━━━\n\n"
    # iterate in stable order
    for cname in COMMODITY_UNDERLYINGS.keys():
        sid = mapping.get(cname)
        price = latest.get(sid) if sid is not None else None
        if price is None:
            msg += f"*{cname}*\n_Price unavailable_\n\n"
        else:
            change = ""
            prev = prev_sent.get(sid)
            if prev is not None:
                diff = price - prev
                if diff > 0:
                    change = f"📈 +{diff:.2f}"
                elif diff < 0:
                    change = f"📉 {diff:.2f}"
                else:
                    change = "➖ 0.00"
            msg += f"*{cname}*\n₹ {price:.2f} {change}\n\n"
    msg += "━━━━━━━━━━━━━━━━"
    return msg

# ---------- Instrument discovery & pick front-month ----------
async def fetch_instruments_rest(client: httpx.AsyncClient, segment: str) -> List[Dict[str, Any]]:
    """
    Best-effort REST call to fetch instruments for a given segment.
    Endpoint may be unavailable for some accounts (404) — function handles gracefully.
    """
    url = f"{DHAN_REST_BASE}/marketfeed/instruments"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id": str(DHAN_CLIENT_ID)
    }
    payload = {"segment": segment}
    try:
        resp = await client.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logger.debug(f"fetch_instruments_rest HTTP error: {e}")
        return []
    if resp.status_code != 200:
        logger.info(f"fetch_instruments_rest: non-200 {resp.status_code} {resp.text[:200]}")
        return []
    try:
        j = resp.json()
    except Exception:
        logger.debug("fetch_instruments_rest: invalid json")
        return []
    data = j.get("data") or []
    # data may be dict or list; normalize to list of objects
    if isinstance(data, dict):
        # dict mapping id -> obj
        items = []
        for k, v in data.items():
            try:
                if isinstance(v, dict):
                    v = v.copy()
                    v["id"] = int(k)
                    items.append(v)
            except Exception:
                continue
        return items
    elif isinstance(data, list):
        return data
    else:
        return []

def parse_instrument_expiry(inst: Dict[str, Any]) -> Optional[datetime]:
    # Try common fields: 'expiry', 'expiry_date', 'expiry_dt' - various formats
    for f in ("expiry", "expiry_date", "expiry_dt", "exp_date"):
        v = inst.get(f)
        if not v:
            continue
        # try iso parse
        try:
            # allow date-only or datetime
            if isinstance(v, (int, float)):
                # some APIs pass epoch ms
                return datetime.fromtimestamp(int(v)/1000, tz=timezone.utc)
            txt = str(v)
            # try common patterns
            try:
                return datetime.fromisoformat(txt)
            except Exception:
                # fallback: try yyyy-mm-dd
                from datetime import datetime as dt
                try:
                    return dt.strptime(txt, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except Exception:
                    pass
        except Exception:
            continue
    return None

def matches_underlying(inst: Dict[str, Any], underlying: str) -> bool:
    """
    Determine if instrument object corresponds to underlying symbol.
    We check fields like 'symbol', 'tradingsymbol', 'name'.
    Use case-insensitive substring match as heuristic.
    """
    keys = ["symbol", "tradingsymbol", "name", "instrument_name"]
    u = underlying.lower()
    for k in keys:
        v = inst.get(k)
        if v and u in str(v).lower().replace(" ", ""):
            return True
    # fallback check attributes
    txt = " ".join(str(inst.get(k, "")).lower() for k in keys)
    if u in txt.replace(" ", ""):
        return True
    return False

def pick_front_month(instruments: List[Dict[str, Any]], underlying: str) -> Optional[Dict[str, Any]]:
    """
    Pick nearest future (front-month) instrument for an underlying symbol.
    Returns the instrument dict or None.
    """
    candidates = []
    for inst in instruments:
        try:
            if matches_underlying(inst, underlying):
                exp = parse_instrument_expiry(inst)
                # only include future expiries
                if exp is not None and exp > datetime.now(timezone.utc):
                    candidates.append((exp, inst))
                else:
                    # sometimes expiry not provided; include with None expiry as fallback later
                    candidates.append((None, inst))
        except Exception:
            continue
    # prefer those with expiries and nearest expiry
    with_expiry = [c for c in candidates if c[0] is not None]
    if with_expiry:
        with_expiry.sort(key=lambda x: x[0])
        return with_expiry[0][1]
    # otherwise fallback to first candidate (if any)
    if candidates:
        return candidates[0][1]
    return None

# ---------- Main bot class ----------
class DhanWebsocketTelegramBot:
    def __init__(self):
        # initial mapping from commodity name -> security_id (int)
        self.mapping: Dict[str, int] = INITIAL_SECURITY_IDS.copy()
        # reverse mapping used for quick lookups
        self.current_security_ids: List[int] = [int(v) for v in self.mapping.values()]

        # latest prices keyed by security_id -> float|None
        self.latest_prices: Dict[int, Optional[float]] = {}
        # prev sent snapshot for change calculation
        self.prev_sent_prices: Dict[int, Optional[float]] = {}

        # load persisted last-good prices (fallback)
        cache = load_cache(CACHE_FILE)
        persisted = cache.get("last_prices", {})
        # persisted keyed by str(sid) -> price
        for cname, sid in self.mapping.items():
            sid_int = int(sid)
            p = persisted.get(str(sid_int))
            self.latest_prices[sid_int] = _safe_float(p) if p is not None else None
            self.prev_sent_prices[sid_int] = _safe_float(p) if p is not None else None

        # ws state
        self.ws_base = WS_BASE
        self.token = DHAN_ACCESS_TOKEN
        self.client_id = str(DHAN_CLIENT_ID)
        self.segment = MCX_SEGMENT

        self._ws = None  # active websocket handle (context-managed)
        self._ws_lock = asyncio.Lock()  # protect ws handle
        self._reconnect = asyncio.Event()
        self._stop = False

        # httpx client for REST instrument discovery
        self.http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)

        # telegram
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID

        logger.info(f"Initial mapping: {self.mapping}")

    def _build_ws_url(self) -> str:
        qs = WS_QUERY_TEMPLATE.format(token=self.token, clientId=self.client_id)
        return f"{self.ws_base}{qs}"

    async def close(self):
        # clean up
        try:
            await self.http_client.aclose()
        except Exception:
            pass
        # persist cache
        try:
            # transform latest_prices to str keys
            to_save = {"last_prices": {str(k): v for k, v in self.latest_prices.items() if v is not None}}
            save_cache(CACHE_FILE, to_save)
            logger.info(f"Saved cache {CACHE_FILE}")
        except Exception as e:
            logger.debug(f"Error saving cache on close: {e}")

    async def send_telegram(self, text: str):
        try:
            maybe = self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="Markdown")
            if asyncio.iscoroutine(maybe):
                await maybe
        except Exception as e:
            logger.warning(f"send_telegram error: {e}")

    async def fetch_and_update_instruments(self) -> bool:
        """
        Fetch instruments via REST and update mapping.
        Returns True if mapping changed (so caller should resubscribe).
        """
        changed = False
        try:
            instruments = await fetch_instruments_rest(self.http_client, self.segment)
            if not instruments:
                logger.info("Instrument discovery returned empty (endpoint might be unavailable for this account).")
                return False
            new_mapping: Dict[str, int] = {}
            for cname, underlying in COMMODITY_UNDERLYINGS.items():
                inst = pick_front_month(instruments, underlying)
                if inst:
                    # find id in common fields
                    sid = None
                    for pk in ("id", "security_id", "instrument_token", "instrumentId"):
                        if pk in inst and inst.get(pk) is not None:
                            try:
                                sid = int(inst.get(pk))
                                break
                            except Exception:
                                continue
                    # some APIs use 'token' or 'contract_id'
                    if sid is None:
                        for pk in ("token", "contract_id"):
                            if pk in inst and inst.get(pk) is not None:
                                try:
                                    sid = int(inst.get(pk))
                                    break
                                except Exception:
                                    continue
                    if sid is not None:
                        new_mapping[cname] = sid
                    else:
                        logger.debug(f"No id found on instrument object for {cname}: {inst}")
                        # fallback to existing mapping if available
                        new_mapping[cname] = int(self.mapping.get(cname, INITIAL_SECURITY_IDS.get(cname)))
                else:
                    # not found: keep existing mapping
                    new_mapping[cname] = int(self.mapping.get(cname, INITIAL_SECURITY_IDS.get(cname)))
            # compare
            if new_mapping != self.mapping:
                logger.info(f"Instrument mapping changed: {self.mapping} -> {new_mapping}")
                self.mapping = new_mapping
                # update current security id list
                self.current_security_ids = [int(v) for v in self.mapping.values()]
                # ensure latest_prices has keys for new ids
                for sid in self.current_security_ids:
                    if sid not in self.latest_prices:
                        self.latest_prices[sid] = None
                changed = True
            else:
                logger.debug("Instrument mapping unchanged.")
        except Exception as e:
            logger.warning(f"Error during instrument discovery: {e}")
        return changed

    async def resubscribe_ws(self):
        """
        Signal that ws reconnect/resubscribe should happen.
        We trigger a reconnect by setting event and closing websocket if open.
        """
        logger.info("Triggering WS resubscribe/reconnect")
        self._reconnect.set()
        # close current ws if exists
        async with self._ws_lock:
            if self._ws is not None:
                try:
                    await self._ws.close()
                except Exception:
                    pass

    async def _ws_worker(self):
        """
        WebSocket connect + read loop. Respects self.mapping for subscription ids.
        If self._reconnect is set, triggers reconnection to subscribe new ids.
        """
        backoff = INITIAL_BACKOFF
        while not self._stop:
            ws_url = self._build_ws_url()
            masked = f"{self.ws_base}?version=2&token=<hidden>&clientId={self.client_id}&authType=2"
            logger.info(f"Connecting to WS {masked} (subscribe {self.current_security_ids})")
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("WebSocket connected")
                    async with self._ws_lock:
                        self._ws = ws
                        # ensure reconnect flag cleared
                        if self._reconnect.is_set():
                            self._reconnect.clear()
                    # subscribe
                    subscribe_payload = {
                        "msgtype": "subscribe",
                        "exchange_segment": self.segment,
                        "security_ids": self.current_security_ids
                    }
                    try:
                        await ws.send(json.dumps(subscribe_payload))
                        logger.info(f"Subscribed: {subscribe_payload}")
                    except Exception as e:
                        logger.warning(f"Failed to send subscribe payload: {e}")

                    # read loop
                    async for raw in ws:
                        if raw is None:
                            continue
                        # log raw for debugging but keep it concise
                        logger.debug(f"WS RAW: {raw if len(str(raw)) < 1000 else str(raw)[:1000] + '...'}")
                        # parse JSON
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            continue
                        # extract sid and price (per earlier patterns)
                        sid = None
                        for k in ("security_id", "id", "instrument", "sec_id"):
                            if k in obj:
                                try:
                                    sid = int(obj[k])
                                    break
                                except Exception:
                                    pass
                        if sid is None and isinstance(obj.get("data"), dict):
                            for kk, vv in obj.get("data", {}).items():
                                try:
                                    ss = int(kk)
                                    sid = ss
                                    obj = vv if isinstance(vv, dict) else obj
                                    break
                                except Exception:
                                    continue
                        price = None
                        for price_key in ("last_price", "lastTradedPrice", "lastPrice", "ltp", "LTP", "lastTraded"):
                            if price_key in obj:
                                price = _safe_float(obj.get(price_key))
                                break
                        if price is None and "tick" in obj and isinstance(obj["tick"], dict):
                            tick = obj["tick"]
                            for price_key in ("last_price", "lastTradedPrice", "lastPrice", "ltp", "LTP"):
                                if price_key in tick:
                                    price = _safe_float(tick.get(price_key))
                                    break
                            if sid is None:
                                for k in ("security_id", "id"):
                                    if k in tick:
                                        try:
                                            sid = int(tick[k])
                                        except Exception:
                                            pass
                        # fallback nested mapping
                        if sid is None and isinstance(obj, dict):
                            for segk, segv in obj.items():
                                if isinstance(segv, dict):
                                    for idk, idv in segv.items():
                                        try:
                                            ss = int(idk)
                                            for price_key in ("last_price", "lastTradedPrice", "lastPrice", "ltp", "LTP"):
                                                if isinstance(idv, dict) and price_key in idv:
                                                    price = _safe_float(idv.get(price_key))
                                                    sid = ss
                                                    break
                                            if sid is not None:
                                                break
                                        except Exception:
                                            continue
                                if sid is not None:
                                    break
                        # update latest_prices with caution
                        if sid is not None and price is not None:
                            # treat 0.0 as artifact/unavailable
                            if price == 0.0:
                                logger.debug(f"Received 0.0 price for sid={sid}; storing as None")
                                self.latest_prices[sid] = None
                            else:
                                self.latest_prices[sid] = price
                                logger.debug(f"Updated price sid={sid} -> {price}")
                                # persist last-good price to cache file as we go (lightweight)
                                try:
                                    to_save = {"last_prices": {str(k): v for k, v in self.latest_prices.items() if v is not None}}
                                    save_cache(CACHE_FILE, to_save)
                                except Exception:
                                    pass
                        # check if somebody requested resubscribe (mapping changed)
                        if self._reconnect.is_set():
                            logger.info("Detected resubscribe request -> break to reconnect")
                            break
                    # end async for -> connection closed / break
                    logger.info("WebSocket connection closed (read loop ended)")
                    async with self._ws_lock:
                        self._ws = None
                    # reset backoff on normal close before reconnecting
                    backoff = INITIAL_BACKOFF
            except asyncio.CancelledError:
                logger.info("WS worker cancelled")
                break
            except Exception as e:
                logger.warning(f"WebSocket connection error: {e}")
                # exponential backoff then reconnect
                logger.info(f"Reconnecting after {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

    async def periodic_refresh_worker(self):
        """
        Periodically refresh instruments and trigger resubscribe if mapping changed.
        """
        # initial immediate attempt to discover & update mapping
        try:
            changed = await self.fetch_and_update_instruments()
            if changed:
                await self.resubscribe_ws()
        except Exception as e:
            logger.debug(f"Initial instruments refresh error: {e}")

        while not self._stop:
            try:
                await asyncio.sleep(REFRESH_INTERVAL)
                logger.info("Running periodic instruments refresh")
                changed = await self.fetch_and_update_instruments()
                if changed:
                    # mapping changed -> resubscribe
                    await self.resubscribe_ws()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error in periodic_refresh_worker: {e}")
                continue

    async def periodic_sender_worker(self):
        """
        Send periodic Telegram summary using current mapping and latest_prices.
        """
        # initial startup message
        try:
            await self.send_telegram("✅ Bot started successfully!\n\n📊 You will receive commodity price updates every minute.")
        except Exception as e:
            logger.debug(f"Startup telegram message failed: {e}")

        while not self._stop:
            try:
                # build and send summary
                msg = format_telegram_message(self.latest_prices, self.prev_sent_prices, self.mapping)
                await self.send_telegram(msg)
                # update prev_sent_prices
                for sid, val in list(self.latest_prices.items()):
                    if val is not None:
                        self.prev_sent_prices[sid] = val
                await asyncio.sleep(SEND_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"periodic_sender_worker error: {e}")
                await asyncio.sleep(5)

    async def run(self):
        """
        Kick off workers: ws worker, refresh worker, sender worker.
        """
        logger.info("Starting main run loop")
        ws_task = asyncio.create_task(self._ws_worker())
        refresh_task = asyncio.create_task(self.periodic_refresh_worker())
        sender_task = asyncio.create_task(self.periodic_sender_worker())
        try:
            await asyncio.gather(ws_task, refresh_task, sender_task)
        finally:
            self._stop = True
            for t in (ws_task, refresh_task, sender_task):
                try:
                    t.cancel()
                except Exception:
                    pass
            await self.close()

# ---------- Entrypoint ----------
if __name__ == "__main__":
    bot = DhanWebsocketTelegramBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user — exiting")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        logger.info("Bot stopped")
