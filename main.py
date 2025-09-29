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

# ---------- WebSocket base & config ----------
WS_BASE = "wss://api-feed.dhan.co"  # base (we will append query string per docs)
# Per Dhan docs for v2: must include version=2, token, clientId, authType=2
WS_QUERY_TEMPLATE = "?version=2&token={token}&clientId={clientId}&authType=2"

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

# Telegram interval
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

# ---------- Bot ----------
class DhanWebsocketTelegramBot:
    def __init__(self):
        self.ws_base = WS_BASE
        self.token = DHAN_ACCESS_TOKEN
        self.client_id = str(DHAN_CLIENT_ID)
        self.segment = MCX_SEGMENT
        self.security_ids = SECURITY_IDS

        self.latest_prices: Dict[int, Optional[float]] = {sid: None for sid in self.security_ids}
        self.prev_sent_prices: Dict[int, Optional[float]] = {}

        # Telegram
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID

        self._stop = False
        self._ws_task: Optional[asyncio.Task] = None
        self._sender_task: Optional[asyncio.Task] = None

    def _build_ws_url(self) -> str:
        # build URL with required query params per Dhan docs
        qs = WS_QUERY_TEMPLATE.format(token=self.token, clientId=self.client_id)
        return f"{self.ws_base}{qs}"

    async def start(self):
        logger.info("Starting Dhan WebSocket -> Telegram bot (query-param handshake)")
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
        await asyncio.sleep(2)
        while not self._stop:
            try:
                message = format_telegram_message(self.latest_prices, self.prev_sent_prices)
                await self._send_telegram(message)
                # update prev_sent_prices
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

    async def _ws_loop(self):
        backoff = INITIAL_BACKOFF
        while not self._stop:
            ws_url = self._build_ws_url()
            # Do not log token (sensitive); log only masked
            masked = f"{self.ws_base}?version=2&token=<hidden>&clientId={self.client_id}&authType=2"
            logger.info(f"Connecting to WS {masked} (subscribe {self.security_ids})")
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("WebSocket connected (query-param auth)")
                    backoff = INITIAL_BACKOFF  # reset

                    # subscribe message (same as before)
                    subscribe_payload = {
                        "msgtype": "subscribe",
                        "exchange_segment": self.segment,
                        "security_ids": self.security_ids
                    }
                    await ws.send(json.dumps(subscribe_payload))
                    logger.info(f"Subscribed: {subscribe_payload}")

                    async for raw in ws:
                        if raw is None:
                            continue
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            logger.debug(f"Non-json WS message: {raw}")
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

                        # fallback nested
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
                logger.info("WebSocket loop cancelled")
                break
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
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
