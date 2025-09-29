# main.py
"""
DhanHQ commodity LTP -> Telegram bot
Features:
- CSV-based instrument discovery (api-scrip-master-detailed.csv)
- front-month auto-pick per commodity
- v2 WebSocket (query-param) subscribe (JSON)
- automatic re-subscribe when mapping changes
- persist last-good prices to disk (fallback)
- send Telegram summary every SEND_INTERVAL seconds (default 60)
"""

import os
import asyncio
import json
import logging
import csv
from io import StringIO
from datetime import datetime, timezone
from typing import Dict, Optional, List, Any, Tuple

import websockets
import httpx
from telegram import Bot
from telegram.error import TelegramError

# ---------- Config & Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("dhan-commodity-bot")

# Environment variables (must be set)
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    logger.error("Missing required env vars. Set DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID")
    raise SystemExit("Missing required environment variables")

# Defaults - adjust if needed
WS_BASE = "wss://api-feed.dhan.co"
WS_QUERY_TEMPLATE = "?version=2&token={token}&clientId={clientId}&authType=2"
CSV_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

# Underlying names to match in CSV/tradingsymbol (heuristic)
COMMODITY_UNDERLYINGS = {
    "GOLD": "GOLD",
    "SILVER": "SILVER",
    "CRUDE OIL": "CRUDEOIL",     # heuristics for matching in CSV
    "NATURAL GAS": "NATURALGAS",
    "COPPER": "COPPER"
}

# Fallback initial IDs (will be replaced by discovery if possible)
INITIAL_SECURITY_IDS = {
    "GOLD": 114,
    "SILVER": 229,
    "CRUDE OIL": 236,
    "NATURAL GAS": 235,
    "COPPER": 256
}

SEND_INTERVAL = int(os.getenv("SEND_INTERVAL", "60"))       # seconds between Telegram summaries
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "3600"))  # CSV refresh interval (seconds)
CACHE_FILE = os.getenv("LAST_PRICE_CACHE", "last_prices.json")
HTTP_TIMEOUT = 15.0

# ---------- Utilities ----------
def _now_str() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%d-%m-%Y %I:%M %p %Z")

def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def load_cache(path: str) -> Dict[str, Any]:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.debug(f"Failed to load cache {path}: {e}")
        return {}

def save_cache(path: str, data: Dict[str, Any]) -> None:
    try:
        with open(path, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.debug(f"Failed to save cache {path}: {e}")

# ---------- CSV instrument discovery ----------
async def fetch_instruments_from_csv(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Download and parse Dhan scrip-master CSV (detailed) and return rows as dicts."""
    try:
        resp = await client.get(CSV_MASTER_URL, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logger.warning(f"CSV fetch failed: {e}")
        return []
    if resp.status_code != 200:
        logger.warning(f"CSV fetch non-200: {resp.status_code}")
        return []
    text = resp.text
    if not text:
        return []
    f = StringIO(text)
    try:
        reader = csv.DictReader(f)
    except Exception as e:
        logger.warning(f"CSV parse failed: {e}")
        return []
    items = []
    for row in reader:
        # normalize keys/values (strip)
        norm = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        items.append(norm)
    logger.info(f"Fetched {len(items)} instruments from CSV")
    return items

def parse_instrument_expiry(inst: Dict[str, Any]) -> Optional[datetime]:
    """Try common expiry fields and return datetime (UTC) if parseable."""
    for f in ("expiry", "expiry_date", "expiry_dt", "exp_date", "contract_expiry"):
        v = inst.get(f)
        if not v:
            continue
        try:
            txt = str(v).strip()
            # try ISO first
            try:
                return datetime.fromisoformat(txt)
            except Exception:
                pass
            # try YYYY-MM-DD
            try:
                from datetime import datetime as dt
                return dt.strptime(txt, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except Exception:
                pass
        except Exception:
            continue
    return None

def matches_underlying(inst: Dict[str, Any], underlying: str) -> bool:
    """Heuristic match by tradingsymbol / symbol / name."""
    keys = ["symbol", "tradingsymbol", "name", "instrument_name", "instrument"]
    u = underlying.lower().replace(" ", "")
    for k in keys:
        v = inst.get(k)
        if v and u in str(v).lower().replace(" ", ""):
            return True
    # fallback: join some fields
    txt = " ".join(str(inst.get(k, "")).lower() for k in keys)
    if u in txt.replace(" ", ""):
        return True
    return False

def pick_front_month(instruments: List[Dict[str, Any]], underlying: str) -> Optional[Dict[str, Any]]:
    """Pick nearest-future instrument for underlying from CSV rows."""
    candidates: List[Tuple[Optional[datetime], Dict[str, Any]]] = []
    for inst in instruments:
        try:
            if matches_underlying(inst, underlying):
                exp = parse_instrument_expiry(inst)
                if exp is not None and exp > datetime.now(timezone.utc):
                    candidates.append((exp, inst))
                else:
                    candidates.append((None, inst))
        except Exception:
            continue
    with_expiry = [c for c in candidates if c[0] is not None]
    if with_expiry:
        with_expiry.sort(key=lambda x: x[0])
        return with_expiry[0][1]
    if candidates:
        return candidates[0][1]
    return None

# ---------- Small helper to map exchange int -> segment string ----------
def exchange_segment_map(code: int) -> str:
    mapping = {
        0: "IDX_I", 1: "NSE_EQ", 2: "NSE_FNO", 3: "NSE_CURRENCY",
        4: "BSE_EQ", 5: "MCX_COMM", 7: "BSE_CURRENCY", 8: "BSE_FNO"
    }
    return mapping.get(code, str(code))

# ---------- Main Bot ----------
class DhanCommodityBot:
    def __init__(self):
        # mapping: commodity name -> security_id (int)
        self.mapping: Dict[str, int] = INITIAL_SECURITY_IDS.copy()
        self.current_security_ids: List[int] = [int(v) for v in self.mapping.values()]

        # latest prices keyed by security_id
        self.latest_prices: Dict[int, Optional[float]] = {}
        self.prev_sent_prices: Dict[int, Optional[float]] = {}

        # load persisted last-good prices
        cache = load_cache(CACHE_FILE)
        persisted = cache.get("last_prices", {})
        for sid in set(self.current_security_ids):
            self.latest_prices[int(sid)] = _safe_float(persisted.get(str(sid)))
            self.prev_sent_prices[int(sid)] = _safe_float(persisted.get(str(sid)))

        # http client for CSV discovery
        self.http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)

        # websocket & control
        self.ws_base = WS_BASE
        self.token = DHAN_ACCESS_TOKEN
        self.client_id = str(DHAN_CLIENT_ID)
        self.ws_url = f"{self.ws_base}{WS_QUERY_TEMPLATE.format(token=self.token, clientId=self.client_id)}"
        self.ws = None
        self._ws_lock = asyncio.Lock()
        self._reconnect = asyncio.Event()
        self._stop = False

        # telegram
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID

        # used to forward first few raw WS messages to debug (set 0 to disable)
        self._forward_raw_max = 0
        self._forward_raw_count = 0

        logger.info(f"Initial mapping: {self.mapping}")

    # ---------- instrument discovery ----------
    async def refresh_mapping_from_csv(self) -> bool:
        """
        Download CSV and compute front-month mapping for each commodity.
        Returns True if mapping changed (so caller should resubscribe).
        """
        try:
            instruments = await fetch_instruments_from_csv(self.http_client)
            if not instruments:
                logger.info("CSV discovery empty — will keep existing mapping")
                return False
            new_mapping: Dict[str, int] = {}
            for cname, underlying in COMMODITY_UNDERLYINGS.items():
                inst = pick_front_month(instruments, underlying)
                if inst:
                    # try common id fields
                    sid = None
                    for k in ("id", "security_id", "instrument_token", "token", "contract_id", "instrumentId"):
                        if k in inst and inst.get(k):
                            try:
                                sid = int(inst.get(k))
                                break
                            except Exception:
                                continue
                    # sometimes CSV has 'securityId' (case-sensitive variations)
                    if sid is None:
                        for k in inst.keys():
                            if k.lower().endswith("id") and inst.get(k):
                                try:
                                    sid = int(inst.get(k))
                                    break
                                except Exception:
                                    continue
                    if sid is not None:
                        new_mapping[cname] = sid
                    else:
                        logger.debug(f"No numeric id found for {cname}, keeping old mapping")
                        new_mapping[cname] = int(self.mapping.get(cname, INITIAL_SECURITY_IDS.get(cname)))
                else:
                    new_mapping[cname] = int(self.mapping.get(cname, INITIAL_SECURITY_IDS.get(cname)))
            if new_mapping != self.mapping:
                logger.info(f"Instrument mapping changed: {self.mapping} -> {new_mapping}")
                self.mapping = new_mapping
                self.current_security_ids = [int(v) for v in self.mapping.values()]
                # ensure latest_prices keys exist
                for sid in self.current_security_ids:
                    if sid not in self.latest_prices:
                        self.latest_prices[sid] = None
                return True
        except Exception as e:
            logger.warning(f"Error refreshing mapping from CSV: {e}")
        return False

    # ---------- websocket connect & read ----------
    async def _send_subscribe_batches(self, ws):
        """
        Send JSON subscribe messages in batches of up to 100 instruments.
        """
        batch_size = 100
        instruments: List[Tuple[int, int]] = [(5, int(sid)) for sid in self.current_security_ids]  # use MCX exchange code 5
        for i in range(0, len(instruments), batch_size):
            batch = instruments[i:i+batch_size]
            msg = {
                "RequestCode": 15,
                "InstrumentCount": len(batch),
                "InstrumentList": [
                    {"ExchangeSegment": exchange_segment_map(ex), "SecurityId": int(sec)}
                    for ex, sec in batch
                ]
            }
            try:
                await ws.send(json.dumps(msg))
                logger.info(f"Sent subscribe (count={len(batch)})")
            except Exception as e:
                logger.warning(f"Failed to send subscribe: {e}")

    async def _ws_worker(self):
        backoff = 1.0
        while not self._stop:
            try:
                masked = f"{self.ws_base}?version=2&token=<hidden>&clientId={self.client_id}&authType=2"
                logger.info(f"Connecting to WS {masked} (subscribe {self.current_security_ids})")
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("WebSocket connected")
                    async with self._ws_lock:
                        self.ws = ws
                        if self._reconnect.is_set():
                            self._reconnect.clear()
                    # subscribe
                    await self._send_subscribe_batches(ws)
                    # read loop
                    async for raw in ws:
                        if raw is None:
                            continue
                        # optionally forward a few raw messages to Telegram for debug
                        if self._forward_raw_count < self._forward_raw_max:
                            try:
                                raw_text = str(raw)
                                if len(raw_text) > 3900:
                                    raw_text = raw_text[:3900] + "...(truncated)"
                                asyncio.create_task(self._send_telegram(f"WS RAW: {raw_text}"))
                                self._forward_raw_count += 1
                            except Exception:
                                pass
                        # try parse JSON
                        parsed = None
                        try:
                            obj = json.loads(raw)
                            parsed = self._extract_price_from_obj(obj)
                        except Exception:
                            # not JSON — ignore in v2 flow
                            continue
                        if parsed:
                            sid = parsed.get("security_id")
                            ltp = parsed.get("ltp")
                            if sid is not None and ltp is not None:
                                # treat 0.0 as unavailable
                                if ltp == 0.0:
                                    self.latest_prices[int(sid)] = None
                                else:
                                    self.latest_prices[int(sid)] = float(ltp)
                                    # persist last-good immediately (lightweight)
                                    try:
                                        to_save = {"last_prices": {str(k): v for k, v in self.latest_prices.items() if v is not None}}
                                        save_cache(CACHE_FILE, to_save)
                                    except Exception:
                                        pass
                        # if mapping update requested -> break to reconnect and subscribe new ids
                        if self._reconnect.is_set():
                            logger.info("Resubscribe requested -> breaking read loop to reconnect")
                            break
                    logger.info("WebSocket read loop ended")
                    async with self._ws_lock:
                        self.ws = None
                    backoff = 1.0
            except asyncio.CancelledError:
                logger.info("WS worker cancelled")
                break
            except Exception as e:
                logger.warning(f"WebSocket connection error: {e}")
                logger.info(f"Reconnecting after {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue

    def _extract_price_from_obj(self, obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Heuristic extraction of security_id and ltp from JSON payload shapes.
        """
        # direct shape: {"security_id":114, "LTP":1234.0}
        if isinstance(obj, dict):
            for pk in ("security_id", "id"):
                if pk in obj:
                    sid = obj.get(pk)
                    for price_key in ("LTP", "ltp", "last_price", "lastPrice", "lastTradedPrice"):
                        if price_key in obj:
                            try:
                                return {"security_id": int(sid), "ltp": float(obj.get(price_key))}
                            except Exception:
                                return None
        # data mapping shape: {"data": {"114": {...}}}
        if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], dict):
            for k, v in obj["data"].items():
                try:
                    sid = int(k)
                except Exception:
                    continue
                if isinstance(v, dict):
                    for price_key in ("LTP", "ltp", "last_price", "lastPrice"):
                        if price_key in v:
                            try:
                                return {"security_id": sid, "ltp": float(v.get(price_key))}
                            except Exception:
                                continue
        # 'tick' nested
        if isinstance(obj, dict) and "tick" in obj and isinstance(obj["tick"], dict):
            tick = obj["tick"]
            sid = tick.get("security_id") or tick.get("id")
            for price_key in ("LTP", "ltp", "last_price", "lastPrice"):
                if price_key in tick:
                    try:
                        return {"security_id": int(sid), "ltp": float(tick.get(price_key))}
                    except Exception:
                        return None
        return None

    async def trigger_resubscribe(self):
        """Ask ws worker to reconnect and subscribe new ids."""
        self._reconnect.set()
        async with self._ws_lock:
            if self.ws is not None:
                try:
                    await self.ws.close()
                except Exception:
                    pass

    # ---------- periodic tasks ----------
    async def periodic_refresh(self):
        """Refresh instrument mapping periodically (CSV) and resubscribe on change."""
        # initial immediate refresh
        try:
            changed = await self.refresh_mapping_from_csv()
            if changed:
                await self.trigger_resubscribe()
        except Exception as e:
            logger.debug(f"Initial mapping refresh error: {e}")
        while not self._stop:
            try:
                await asyncio.sleep(REFRESH_INTERVAL)
                logger.info("Running periodic CSV refresh")
                changed = await self.refresh_mapping_from_csv()
                if changed:
                    await self.trigger_resubscribe()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"periodic_refresh error: {e}")
                continue

    async def _send_telegram(self, text: str):
        """Send Telegram message (safely handling sync/coro differences)."""
        try:
            maybe = self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="Markdown")
            if asyncio.iscoroutine(maybe):
                await maybe
        except TelegramError as e:
            logger.warning(f"Telegram API error: {e}")
        except Exception as e:
            logger.warning(f"Failed to send Telegram message: {e}")

    async def periodic_sender(self):
        """Send Telegram summary every SEND_INTERVAL seconds (and immediate startup message)."""
        try:
            await self._send_telegram("✅ Bot started successfully!\n\n📊 You will receive commodity price updates every minute.")
        except Exception:
            pass

        # send immediate summary if we already have some prices
        await asyncio.sleep(2)

        while not self._stop:
            try:
                msg = self.format_message()
                await self._send_telegram(msg)
                # update prev_sent
                for sid, val in list(self.latest_prices.items()):
                    if val is not None:
                        self.prev_sent_prices[sid] = val
                await asyncio.sleep(SEND_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"periodic_sender error: {e}")
                await asyncio.sleep(5)

    def format_message(self) -> str:
        timestamp = _now_str()
        message = f"📊 *Commodity Prices*\n"
        message += f"🕒 {timestamp}\n"
        message += "━━━━━━━━━━━━━━━━\n\n"
        for cname, sid in self.mapping.items():
            sid_int = int(sid)
            price = self.latest_prices.get(sid_int)
            if price is None:
                message += f"*{cname}*\n_Price unavailable_\n\n"
            else:
                change = ""
                prev = self.prev_sent_prices.get(sid_int)
                if prev is not None:
                    diff = price - prev
                    if diff > 0:
                        change = f"📈 +{diff:.2f}"
                    elif diff < 0:
                        change = f"📉 {diff:.2f}"
                    else:
                        change = "➖ 0.00"
                message += f"*{cname}*\n₹ {price:.2f} {change}\n\n"
        message += "━━━━━━━━━━━━━━━━"
        return message

    # ---------- run / shutdown ----------
    async def run(self):
        logger.info("Starting bot run loop")
        ws_task = asyncio.create_task(self._ws_worker())
        refresh_task = asyncio.create_task(self.periodic_refresh())
        sender_task = asyncio.create_task(self.periodic_sender())
        try:
            await asyncio.gather(ws_task, refresh_task, sender_task)
        finally:
            self._stop = True
            for t in (ws_task, refresh_task, sender_task):
                try:
                    t.cancel()
                except Exception:
                    pass
            # close http client and save cache
            try:
                await self.http_client.aclose()
            except Exception:
                pass
            try:
                # save last-good prices
                to_save = {"last_prices": {str(k): v for k, v in self.latest_prices.items() if v is not None}}
                save_cache(CACHE_FILE, to_save)
            except Exception:
                pass
            logger.info("Bot stopped and cleaned up")

# ---------- Entrypoint ----------
if __name__ == "__main__":
    bot = DhanCommodityBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user — exiting")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
    finally:
        logger.info("Exiting")
