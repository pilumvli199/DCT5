# marketfeed_v2.py  (replace your mixed file with this v2-only version)
import asyncio
import json
import logging
from typing import List, Tuple, Callable, Optional, Dict, Any

import websockets

logger = logging.getLogger("dhan-marketfeed")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class MarketFeedV2:
    """
    Minimal v2 JSON WebSocket client for DhanHQ market feed.
    Usage:
        mf = MarketFeedV2(client_id, access_token, instruments)
        mf.on_tick = lambda tick: print("tick", tick)
        await mf.connect()
    instruments: list of tuples (exchange_code_int, security_id_int) e.g. [(5, 114), (5,229)]
    """
    WS_BASE = "wss://api-feed.dhan.co"
    def __init__(self, client_id: str, access_token: str, instruments: List[Tuple[int, int]], loop: Optional[asyncio.AbstractEventLoop]=None):
        self.client_id = str(client_id)
        self.access_token = str(access_token)
        self.instruments = instruments[:]  # list of (exchange, security_id)
        self.ws_url = f"{self.WS_BASE}?version=2&token={self.access_token}&clientId={self.client_id}&authType=2"
        self.ws = None
        self._loop = loop or asyncio.get_event_loop()
        self.on_tick: Optional[Callable[[Dict[str, Any]], None]] = None
        self._connected = False
        self._stop = False

    async def connect(self):
        backoff = 1.0
        while not self._stop:
            try:
                logger.info(f"Connecting to {self.ws_url}")
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
                    self.ws = ws
                    self._connected = True
                    logger.info("Connected (v2)")
                    # subscribe current instruments (send JSON batches)
                    await self._send_subscriptions(self.instruments)
                    # read loop
                    async for raw in ws:
                        # try parse JSON; if fails, ignore or log
                        if raw is None:
                            continue
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            logger.debug("Received non-JSON frame; skipping (v2 client expects JSON).")
                            continue
                        # parse typical shapes:
                        # 1) {"data": {"114": {"LTP": 1234.0, ...}, ...}}
                        # 2) {"security_id":114, "LTP": 1234.0}
                        # 3) {"tick": {...}}
                        parsed = self._extract_price_from_obj(obj)
                        if parsed and self.on_tick:
                            try:
                                self.on_tick(parsed)
                            except Exception as e:
                                logger.exception(f"on_tick handler error: {e}")
                    logger.info("WebSocket loop ended (connection closed)")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"WS error: {e}; reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue

    async def _send_subscriptions(self, instruments: List[Tuple[int,int]]):
        """
        Send subscribe message in batches of up to 100 (matching Dhan expectations).
        JSON format:
        {
          "RequestCode": 15,
          "InstrumentCount": N,
          "InstrumentList": [{"ExchangeSegment": "MCX_COMM", "SecurityId": 114}, ...]
        }
        """
        if not instruments:
            return
        # Helper to map exchange code -> segment string (same as your previous map)
        def ex_seg(code: int) -> str:
            mapping = {0: "IDX_I", 1: "NSE_EQ", 2: "NSE_FNO", 3: "NSE_CURRENCY", 4: "BSE_EQ", 5: "MCX_COMM", 7: "BSE_CURRENCY", 8: "BSE_FNO"}
            return mapping.get(code, str(code))

        batch_size = 100
        for i in range(0, len(instruments), batch_size):
            batch = instruments[i:i+batch_size]
            msg = {
                "RequestCode": 15,  # Ticker / minimal
                "InstrumentCount": len(batch),
                "InstrumentList": [{"ExchangeSegment": ex_seg(ex), "SecurityId": int(sec)} for ex, sec in batch]
            }
            try:
                await self.ws.send(json.dumps(msg))
                logger.info(f"Sent subscribe (count={len(batch)})")
            except Exception as e:
                logger.warning(f"Failed to send subscribe: {e}")

    def _extract_price_from_obj(self, obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Heuristic extraction of security_id and ltp from known JSON shapes.
        Returns dict: {"security_id": id, "ltp": float, "raw": obj}
        """
        # Case: {"security_id":114, "LTP": 123.4}
        for price_key in ("LTP", "ltp", "last_price", "lastPrice", "lastTradedPrice"):
            if isinstance(obj, dict) and price_key in obj and ("security_id" in obj or "id" in obj):
                sid = obj.get("security_id") or obj.get("id")
                try:
                    return {"security_id": int(sid), "ltp": float(obj.get(price_key)), "raw": obj}
                except Exception:
                    return None
        # Case: {"data": {"114": {"LTP":...}, "229": {...}}}
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
                                return {"security_id": sid, "ltp": float(v.get(price_key)), "raw": obj}
                            except Exception:
                                continue
        # Case nested 'tick'
        if isinstance(obj, dict) and "tick" in obj and isinstance(obj["tick"], dict):
            tick = obj["tick"]
            sid = tick.get("security_id") or tick.get("id")
            for price_key in ("LTP","ltp","last_price","lastPrice"):
                if price_key in tick:
                    try:
                        return {"security_id": int(sid), "ltp": float(tick.get(price_key)), "raw": obj}
                    except Exception:
                        return None
        return None

    async def close(self):
        self._stop = True
        try:
            if self.ws:
                await self.ws.close()
        except Exception:
            pass

# Example usage:
# async def demo():
#     mf = MarketFeedV2(client_id="...", access_token="...", instruments=[(5,114),(5,229)])
#     mf.on_tick = lambda tick: print("tick", tick)
#     await mf.connect()
#
# asyncio.run(demo())
