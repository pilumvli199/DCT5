# main.py
import os
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Optional, List

import websockets  # pip install websockets
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

# ---------- WebSocket endpoint & config ----------
WS_URL = "wss://api-feed.dhan.co"  # change if your account uses a different WS URL
MCX_SEGMENT = "MCX_COMM"

# Commodities mapping with security IDs
COMMODITIES = {
    "GOLD": 114,
    "SILVER": 229,
    "CRUDE OIL": 236,
    "NATURAL GAS": 235,
    "COPPER": 256,
}

SECURITY_IDS: List[int] = [int(v) for v in COMMODITIES.values()]

# Backoff config
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 60.0

# Telegram send interval (seconds)
SEND_INTERVAL = 60

# ---------- Helpers ----------
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

# ---------- Main Bot ----------
class DhanWebsocketTelegramBot:
    def __init__(self):
        self.ws_url = WS_URL
        self.client_id = str(DHAN_CLIENT_ID)
        self.access_token = str(DHAN_ACCESS_TOKEN)
        self.segment = MCX_SEGMENT
        self.security_ids = SECURITY_IDS

        # price storage
        self.latest_prices: Dict[int, Optional[float]] = {sid: None for sid in self.security_ids}
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
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._sender_task = asyncio.create_task(self._periodic_sender())
        await asyncio.gather(self._ws_task, self._sender_task)

    async def stop(self):
        logger.info("Stopping bot")
        self._stop = True
        if self._ws_task:
            self._ws_task.cancel()
        if self._sender_task:
            self._sender_task.cancel()

    async def _periodic_sender(self):
        # Give a moment to populate initial prices
        await asyncio.sleep(2)
        while not self._stop:
            try:
                message = format_telegram_message(self.latest_prices, self.prev_sent_prices)
                await self._send_telegram(message)
                # update prev_sent_prices for non-None
                for sid, val in self.latest_prices.items():
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

    # ---------- Robust WS loop (replace previous simpler loop) ----------
    async def _ws_loop(self):
        """
        Try multiple handshake variants:
         - header tuples
         - header dict
         - Authorization: Bearer
         - query params
         - with Origin header
         - with simple subprotocols
         - plain connect
        Logs attempts and errors. Subscribes and parses incoming ticks.
        """
        backoff = INITIAL_BACKOFF
        attempt_num = 0

        async def try_connect(ws_url, connect_kwargs, attempt_label):
            nonlocal attempt_num
            attempt_num += 1
            # hide access token in logs but show label
            log_kwargs = {k: ("<hidden>" if k == "extra_headers" else str(connect_kwargs.get(k))) for k in connect_kwargs}
            logger.info(f"WS attempt #{attempt_num}: {attempt_label} -> {ws_url} kwargs={list(connect_kwargs.keys())}")
            try:
                # websockets.connect accepts extra_headers as list or dict
                return await websockets.connect(ws_url, **connect_kwargs)
            except Exception as e:
                logger.error(f"WebSocket attempt #{attempt_num} ({attempt_label}) error: {e}")
                raise

        while not self._stop:
            # prepare handshake variants
            variants = []

            # 1) header-based tuples
            variants.append({
                "label": "headers-tuples",
                "url": self.ws_url,
                "kwargs": {
                    "extra_headers": [("client-id", self.client_id), ("access-token", self.access_token)],
                    "ping_interval": 20,
                    "ping_timeout": 10,
                }
            })
            # 2) header-based dict
            variants.append({
                "label": "headers-dict",
                "url": self.ws_url,
                "kwargs": {
                    "extra_headers": {"client-id": self.client_id, "access-token": self.access_token},
                    "ping_interval": 20,
                    "ping_timeout": 10,
                }
            })
            # 3) Authorization Bearer
            variants.append({
                "label": "authorization-bearer",
                "url": self.ws_url,
                "kwargs": {
                    "extra_headers": {"Authorization": f"Bearer {self.access_token}", "client-id": self.client_id},
                    "ping_interval": 20,
                    "ping_timeout": 10,
                }
            })
            # 4) query params
            qp_url = f"{self.ws_url}?client_id={self.client_id}&access_token={self.access_token}"
            variants.append({
                "label": "query-params",
                "url": qp_url,
                "kwargs": {
                    "ping_interval": 20,
                    "ping_timeout": 10,
                }
            })
            # 5) origin + header tuples
            variants.append({
                "label": "headers-with-origin",
                "url": self.ws_url,
                "kwargs": {
                    "extra_headers": [("client-id", self.client_id), ("access-token", self.access_token), ("Origin", "https://api.dhan.co")],
                    "ping_interval": 20,
                    "ping_timeout": 10,
                }
            })
            # 6) subprotocols + headers
            variants.append({
                "label": "subprotocol-json-headers",
                "url": self.ws_url,
                "kwargs": {
                    "extra_headers": {"client-id": self.client_id, "access-token": self.access_token},
                    "subprotocols": ["json"],
                    "ping_interval": 20,
                    "ping_timeout": 10,
                }
            })
            # 7) plain connect (no headers) - to inspect server response
            variants.append({
                "label": "plain-no-headers",
                "url": self.ws_url,
                "kwargs": {
                    "ping_interval": 20,
                    "ping_timeout": 10,
                }
            })

            connected = False
            ws = None

            for v in variants:
                try:
                    ws = await try_connect(v["url"], v["kwargs"], v["label"])
                    # if connection returned, wrap in context manager style usage below
                    connected = True
                    logger.info(f"Connected using variant: {v['label']}")
                    break
                except Exception:
                    # slight pause before next variant
                    await asyncio.sleep(0.25)
                    continue

            if not connected:
                logger.info(f"All WS handshake variants failed. Reconnecting after {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

            # we have a WebSocket connection object (ws). Use it in async-with pattern
            try:
                async with ws as socket:
                    backoff = INITIAL_BACKOFF  # reset backoff on successful connect

                    # subscribe
                    subscribe_payload = {
                        "msgtype": "subscribe",
                        "exchange_segment": self.segment,
                        "security_ids": self.security_ids
                    }
                    try:
                        await socket.send(json.dumps(subscribe_payload))
                        logger.info(f"Subscribed with payload: {subscribe_payload}")
                    except Exception as e:
                        logger.error(f"Failed to send subscribe payload: {e}")

                    # read loop
                    async for raw in socket:
                        if raw is None:
                            continue
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            logger.debug(f"Received non-json WS message: {raw}")
                            continue

                        # parse sid
                        sid = None
                        for k in ("security_id", "id", "instrument", "sec_id"):
                            if k in obj:
                                try:
                                    sid = int(obj[k])
                                    break
                                except Exception:
                                    pass

                        if sid is None and isinstance(obj.get("data"), dict):
                            data = obj.get("data")
                            for kk, vv in data.items():
                                try:
                                    ss = int(kk)
                                    sid = ss
                                    obj = vv if isinstance(vv, dict) else obj
                                    break
                                except Exception:
                                    continue

                        # parse price
                        price = None
                        for price_key in ("last_price", "lastTradedPrice", "lastPrice", "ltp", "LTP"):
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

                        # fallback nested segment/id mapping
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

                        if sid is not None and price is not None:
                            if price == 0.0:
                                logger.debug(f"Received 0.0 price for sid={sid}; treating as unavailable")
                                self.latest_prices[sid] = None
                            else:
                                self.latest_prices[sid] = price
                                logger.debug(f"Updated price sid={sid} price={price}")
                        else:
                            logger.debug(f"WS msg no price parsed: {obj}")

            except asyncio.CancelledError:
                logger.info("Websocket loop cancelled")
                break
            except Exception as e:
                logger.error(f"WebSocket read loop error after connect: {e}")
                # wait and reconnect with backoff
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
