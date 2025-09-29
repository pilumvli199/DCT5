# main.py
import os
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Optional, List

import websockets  # pip install websockets
from telegram import Bot
from telegram.error import TelegramError

# ---------- logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("dhan-telegram-ws")

# ---------- env ----------
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    logger.error("Missing env vars. Set DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID")
    raise SystemExit("Missing required environment variables")

# ---------- WebSocket endpoint & config ----------
WS_URL = "wss://api-feed.dhan.co"  # DhanHQ websocket endpoint (as used earlier)
MCX_SEGMENT = "MCX_COMM"  # exchange segment to subscribe for MCX commodities

# Commodities mapping with security IDs (as you provided)
COMMODITIES = {
    "GOLD": 114,
    "SILVER": 229,
    "CRUDE OIL": 236,
    "NATURAL GAS": 235,
    "COPPER": 256,
}

# subscribe list for websocket (integers)
SECURITY_IDS: List[int] = [int(v) for v in COMMODITIES.values()]

# Backoff config for reconnects
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 60.0

# Telegram send interval (seconds)
SEND_INTERVAL = 60

# ---------- Helper functions ----------
def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def format_telegram_message(latest: Dict[int, Optional[float]], prev_sent: Dict[int, Optional[float]]) -> str:
    timestamp = datetime.now().strftime("%d-%m-%Y %I:%M %p")
    msg = f"📊 *Commodity Prices*\n"
    msg += f"🕒 {timestamp}\n"
    msg += "━━━━━━━━━━━━━━━━\n\n"
    # iterate in same order as COMMODITIES dict
    for name, sid in COMMODITIES.items():
        price = latest.get(sid)
        if price is None:
            msg += f"*{name}*\n_Price unavailable_\n\n"
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
            msg += f"*{name}*\n₹ {price:.2f} {change}\n\n"
    msg += "━━━━━━━━━━━━━━━━"
    return msg

# ---------- Main bot class ----------
class DhanWebsocketTelegramBot:
    def __init__(self):
        self.ws_url = WS_URL
        self.client_id = str(DHAN_CLIENT_ID)
        self.access_token = str(DHAN_ACCESS_TOKEN)
        self.segment = MCX_SEGMENT
        self.security_ids = SECURITY_IDS

        # storage for latest prices {security_id: price}
        self.latest_prices: Dict[int, Optional[float]] = {sid: None for sid in self.security_ids}
        # previously sent prices for change calculation
        self.prev_sent_prices: Dict[int, Optional[float]] = {}

        # telegram
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID

        # control
        self._stop = False
        self._ws_task: Optional[asyncio.Task] = None
        self._sender_task: Optional[asyncio.Task] = None

    async def start(self):
        logger.info("Starting Dhan WebSocket -> Telegram bot")
        # run websocket listener and sender concurrently
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._sender_task = asyncio.create_task(self._periodic_sender())

        # wait for tasks (they run until cancelled)
        await asyncio.gather(self._ws_task, self._sender_task)

    async def stop(self):
        logger.info("Stopping bot")
        self._stop = True
        if self._ws_task:
            self._ws_task.cancel()
        if self._sender_task:
            self._sender_task.cancel()

    async def _periodic_sender(self):
        # Wait a bit for initial prices to populate
        await asyncio.sleep(2)
        while not self._stop:
            try:
                # build message from latest_prices and prev_sent_prices
                message = format_telegram_message(self.latest_prices, self.prev_sent_prices)
                await self._send_telegram(message)
                # update prev_sent_prices only for non-None values
                for sid, val in list(self.latest_prices.items()):
                    if val is not None:
                        self.prev_sent_prices[sid] = val
                await asyncio.sleep(SEND_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic sender: {e}")
                await asyncio.sleep(5)

    async def _send_telegram(self, message: str):
        try:
            maybe = self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode="Markdown")
            if asyncio.iscoroutine(maybe):
                await maybe
            logger.info(f"Telegram message sent at {datetime.now().strftime('%I:%M:%S %p')}")
        except TelegramError as e:
            logger.error(f"Telegram API error: {e}")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")

    async def _ws_loop(self):
        backoff = INITIAL_BACKOFF
        while not self._stop:
            try:
                # connect with headers
                headers = [
                    ("client-id", self.client_id),
                    ("access-token", self.access_token)
                ]
                logger.info(f"Connecting to WS {self.ws_url} (subscribe {self.security_ids})")
                async with websockets.connect(self.ws_url, extra_headers=headers, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("WebSocket connected")
                    backoff = INITIAL_BACKOFF  # reset backoff after successful connect

                    # send subscribe message (protocol used earlier)
                    subscribe_payload = {
                        "msgtype": "subscribe",
                        "exchange_segment": self.segment,
                        "security_ids": self.security_ids
                    }
                    await ws.send(json.dumps(subscribe_payload))
                    logger.info(f"Subscribed: {subscribe_payload}")

                    # receive loop
                    async for raw in ws:
                        if raw is None:
                            continue
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            logger.debug(f"Received non-json WS message: {raw}")
                            continue

                        # parse known tick shapes - be permissive
                        # sample expected keys: 'security_id', 'last_price', 'ltp', 'lastTradedPrice'
                        sid = None
                        # try multiple key names
                        for k in ("security_id", "id", "instrument", "sec_id"):
                            if k in obj:
                                try:
                                    sid = int(obj[k])
                                    break
                                except Exception:
                                    pass
                        # if no direct key, maybe nested under 'data' etc.
                        if sid is None and isinstance(obj.get("data"), dict):
                            # try to find a numeric key
                            data = obj.get("data")
                            for kk, vv in data.items():
                                try:
                                    ss = int(kk)
                                    sid = ss
                                    # set obj to vv for price extraction
                                    obj = vv if isinstance(vv, dict) else obj
                                    break
                                except Exception:
                                    continue

                        # get price fields
                        price = None
                        for price_key in ("last_price", "lastTradedPrice", "lastPrice", "ltp", "LTP"):
                            if price_key in obj:
                                price = _safe_float(obj.get(price_key))
                                break

                        # sometimes payload is like {"tick": {...}}
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

                        # fallback: if msg contains mapping of segment -> id -> {last_price:..}
                        if sid is None and isinstance(obj, dict):
                            for segk, segv in obj.items():
                                if isinstance(segv, dict):
                                    for idk, idv in segv.items():
                                        try:
                                            ss = int(idk)
                                            # idv may contain price
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

                        # if we got sid and price, update
                        if sid is not None and price is not None:
                            # update only if non-zero meaningful price; keep None for missing/0
                            if price == 0.0:
                                # treat 0.0 as unavailable artifact (you can change this)
                                logger.debug(f"Received 0.0 price for sid={sid}; treating as unavailable")
                                self.latest_prices[sid] = None
                            else:
                                self.latest_prices[sid] = price
                                logger.debug(f"Updated price sid={sid} price={price}")
                        else:
                            # log for debugging occasionally
                            logger.debug(f"WS msg no price parsed: {obj}")

            except asyncio.CancelledError:
                logger.info("Websocket loop cancelled")
                break
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
                # backoff before reconnect
                logger.info(f"Reconnecting after {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

# ---------- Entrypoint ----------
if __name__ == "__main__":
    bot = DhanWebsocketTelegramBot()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logger.info("Interrupted by user — exiting")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        logger.info("Bot stopped")
