# main.py
"""
DhanHQ commodity LTP -> Telegram bot (v2, query-param auth)
- Build safe wss URL from env (URL-encodes token)
- CSV-based instrument discovery (public scrip-master)
- Subscribe via JSON to WS, parse common JSON shapes, update LTPs
- Send Telegram summary every SEND_INTERVAL seconds
- Persist last-good prices to disk (last_prices.json)
"""

import os
import asyncio
import json
import logging
import csv
from io import StringIO
from datetime import datetime, timezone
from typing import Dict, Optional, List, Any, Tuple
from urllib.parse import quote_plus

import websockets
import httpx
from telegram import Bot
from telegram.error import TelegramError
from dotenv import load_dotenv

# load .env (if present)
load_dotenv()

# ---------- Config & Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("dhan-commodity-bot")

# ---------- Env vars (required) ----------
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    logger.error("Missing required env vars. Set DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID")
    raise SystemExit("Missing required environment variables")

# ---------- Tunables ----------
WS_BASE = "wss://api-feed.dhan.co"
WS_QUERY_TEMPLATE = "?version=2&token={token}&clientId={clientId}&authType=2"
CSV_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

COMMODITY_UNDERLYINGS = {
    "GOLD": "GOLD",
    "SILVER": "SILVER",
    "CRUDE OIL": "CRUDEOIL",
    "NATURAL GAS": "NATURALGAS",
    "COPPER": "COPPER"
}

INITIAL_SECURITY_IDS = {
    "GOLD": 114,
    "SILVER": 229,
    "CRUDE OIL": 236,
    "NATURAL GAS": 235,
    "COPPER": 256
}

SEND_INTERVAL = int(os.getenv("SEND_INTERVAL", "60"))
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "3600"))
CACHE_FILE = os.getenv("LAST_PRICE_CACHE", "last_prices.json")
HTTP_TIMEOUT = 15.0

# ---------- Helpers ----------
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

# ---------- CSV discovery ----------
async def fetch_instruments_from_csv(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
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
        norm = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        items.append(norm)
    logger.info(f"Fetched {len(items)} instruments from CSV")
    return items

def parse_instrument_expiry(inst: Dict[str, Any]) -> Optional[datetime]:
    for f in ("expiry", "expiry_date", "expiry_dt", "exp_date", "contract_expiry"):
        v = inst.get(f)
        if not v:
            continue
        try:
            txt = str(v).strip()
            try:
                return datetime.fromisoformat(txt)
            except Exception:
                pass
            try:
                from datetime import datetime as dt
                return dt.strptime(txt, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except Exception:
                pass
        except Exception:
            continue
    return None

def matches_underlying(inst: Dict[str, Any], underlying: str) -> bool:
    keys = ["symbol", "tradingsymbol", "name", "instrument_name", "instrument"]
    u = underlying.lower().replace(" ", "")
    for k in keys:
        v = inst.get(k)
        if v and u in str(v).lower().replace(" ", ""):
            return True
    txt = " ".join(str(inst.get(k, "")).lower() for k in keys)
    if u in txt.replace(" ", ""):
        return True
    return False

def pick_front_month(instruments: List[Dict[str, Any]], underlying: str) -> Optional[Dict[str, Any]]:
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

def exchange_segment_map(code: int) -> str:
    mapping = {
        0: "IDX_I", 1: "NSE_EQ", 2: "NSE_FNO", 3: "NSE_CURRENCY",
        4: "BSE_EQ", 5: "MCX_COMM", 7: "BSE_CURRENCY", 8: "BSE_FNO"
    }
    return mapping.get(code, str(code))

# ---------- Bot ----------
class DhanCommodityBot:
    def __init__(self):
        self.mapping: Dict[str, int] = INITIAL_SECURITY_IDS.copy()
        self.current_security_ids: List[int] = [int(v) for v in self.mapping.values()]
        self.latest_prices: Dict[int, Optional[float]] = {}
        self.prev_sent_prices: Dict[int, Optional[float]] = {}
        cache = load_cache(CACHE_FILE)
        persisted = cache.get("last_prices", {})
        for sid in set(self.current_security_ids):
            self.latest_prices[int(sid)] = _safe_float(persisted.get(str(sid)))
            self.prev_sent_prices[int(sid)] = _safe_float(persisted.get(str(sid)))
        self.http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
        # build safe encoded ws url
        token_q = quote_plus(DHAN_ACCESS_TOKEN.strip())
        client_q = quote_plus(str(DHAN_CLIENT_ID).strip())
        self.ws_base = WS_BASE
        self.ws_url = f"{self.ws_base}{WS_QUERY_TEMPLATE.format(token=token_q, clientId=client_q)}"
        # masked for logs
        self._masked_ws = f"{self.ws_base}{WS_QUERY_TEMPLATE.format(token='<hidden>', clientId=client_q)}"
        self.ws = None
        self._ws_lock = asyncio.Lock()
        self._reconnect = asyncio.Event()
        self._stop = False
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID
        # debug forward raw messages? 0 = disabled
        self._forward_raw_max = 0
        self._forward_raw_count = 0
        logger.info(f"Initial mapping: {self.mapping}")

    async def refresh_mapping_from_csv(self) -> bool:
        try:
            instruments = await fetch_instruments_from_csv(self.http_client)
            if not instruments:
                logger.info("CSV discovery empty — keeping existing mapping")
                return False
            new_mapping: Dict[str, int] = {}
            for cname, underlying in COMMODITY_UNDERLYINGS.items():
                inst = pick_front_month(instruments, underlying)
                if inst:
                    sid = None
                    for k in ("id", "security_id", "instrument_token", "token", "contract_id", "instrumentId"):
                        if k in inst and inst.get(k):
                            try:
                                sid = int(inst.get(k))
                                break
                            except Exception:
                                continue
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
                        new_mapping[cname] = int(self.mapping.get(cname, INITIAL_SECURITY_IDS.get(cname)))
                else:
                    new_mapping[cname] = int(self.mapping.get(cname, INITIAL_SECURITY_IDS.get(cname)))
            if new_mapping != self.mapping:
                logger.info(f"Instrument mapping changed: {self.mapping} -> {new_mapping}")
                self.mapping = new_mapping
                self.current_security_ids = [int(v) for v in self.mapping.values()]
                for sid in self.current_security_ids:
                    if sid not in self.latest_prices:
                        self.latest_prices[sid] = None
                return True
        except Exception as e:
            logger.warning(f"Error refreshing mapping: {e}")
        return False

    async def _send_subscribe_batches(self, ws):
        batch_size = 100
        instruments: List[Tuple[int, int]] = [(5, int(sid)) for sid in self.current_security_ids]
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

    def _extract_price_from_obj(self, obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
        if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], dict):
            for k, v in obj["data"].items():
                try:
                    sid = int(k)
                except Exception:
                    continue
                if isinstance(v, dict):
                    for price_key in ("LTP","ltp","last_price","lastPrice"):
                        if price_key in v:
                            try:
                                return {"security_id": sid, "ltp": float(v.get(price_key))}
                            except Exception:
                                continue
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

    async def _ws_worker(self):
        backoff = 1.0
        while not self._stop:
            try:
                logger.info(f"Connecting to WS {self._masked_ws}")
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("WebSocket connected")
                    async with self._ws_lock:
                        self.ws = ws
                        if self._reconnect.is_set():
                            self._reconnect.clear()
                    await self._send_subscribe_batches(ws)
                    async for raw in ws:
                        if raw is None:
                            continue
                        if self._forward_raw_count < self._forward_raw_max:
                            try:
                                raw_text = str(raw)
                                if len(raw_text) > 3900:
                                    raw_text = raw_text[:3900] + "...(truncated)"
                                asyncio.create_task(self._send_telegram(f"WS RAW: {raw_text}"))
                                self._forward_raw_count += 1
                            except Exception:
                                pass
                        parsed = None
                        try:
                            obj = json.loads(raw)
                            parsed = self._extract_price_from_obj(obj)
                        except Exception:
                            continue
                        if parsed:
                            sid = parsed.get("security_id")
                            ltp = parsed.get("ltp")
                            if sid is not None and ltp is not None:
                                if ltp == 0.0:
                                    self.latest_prices[int(sid)] = None
                                else:
                                    self.latest_prices[int(sid)] = float(ltp)
                                    try:
                                        to_save = {"last_prices": {str(k): v for k, v in self.latest_prices.items() if v is not None}}
                                        save_cache(CACHE_FILE, to_save)
                                    except Exception:
                                        pass
                        if self._reconnect.is_set():
                            logger.info("Resubscribe requested -> reconnecting")
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

    async def trigger_resubscribe(self):
        self._reconnect.set()
        async with self._ws_lock:
            if self.ws is not None:
                try:
                    await self.ws.close()
                except Exception:
                    pass

    async def periodic_refresh(self):
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
        try:
            maybe = self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="Markdown")
            if asyncio.iscoroutine(maybe):
                await maybe
        except TelegramError as e:
            logger.warning(f"Telegram API error: {e}")
        except Exception as e:
            logger.warning(f"Failed to send Telegram message: {e}")

    async def periodic_sender(self):
        try:
            await self._send_telegram("✅ Bot started successfully!\n\n📊 You will receive commodity price updates every minute.")
        except Exception:
            pass
        await asyncio.sleep(2)
        while not self._stop:
            try:
                msg = self.format_message()
                await self._send_telegram(msg)
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
            try:
                await self.http_client.aclose()
            except Exception:
                pass
            try:
                to_save = {"last_prices": {str(k): v for k, v in self.latest_prices.items() if v is not None}}
                save_cache(CACHE_FILE, to_save)
            except Exception:
                pass
            logger.info("Bot stopped and cleaned up")

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
