import asyncio
import hashlib
import hmac
import json
import logging
import random
import secrets
import time
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple, List

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from .config import QFEXConfig
from .models import BalanceState, PositionState, BBO, Side, OrderType, TimeInForce
from .strategy import TakerStrategy


class QFEXTakerClient:
    def __init__(self, cfg: QFEXConfig, strategy: TakerStrategy):
        self.cfg = cfg
        self.strategy = strategy

        self.log = logging.getLogger("qfex.taker")
        if not self.log.hasHandlers():
            # Add a NullHandler so we don't complain if the user doesn't configure logging
            self.log.addHandler(logging.NullHandler())

        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []

        # Public state snapshots (safe to read; updated on event loop thread)
        self.bbo_by_symbol: Dict[str, BBO] = {}
        self.balance: BalanceState = BalanceState()
        self.positions: Dict[str, PositionState] = {}

        # Sequence tracking for MDS feeds
        self._mds_seq_last: Dict[Tuple[str, str], int] = {}  # (channel, symbol) -> seq

        # Trade WS order tracking
        # QFEX OrderStatus docs: FILLED can be partial, so we compute deltas using quantity_remaining.
        self._order_last_remaining: Dict[
            str, Decimal
        ] = {}  # order_id -> last quantity_remaining
        self._client_oid_to_fut: Dict[str, asyncio.Future] = {}

        # Outbound trade messages go through a single writer task
        self._trade_out_q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(
            maxsize=cfg.outbound_queue_max
        )

        # Websocket handles
        self._mds_ws: Optional[ClientConnection] = None
        self._trade_ws: Optional[ClientConnection] = None

    # ---------- Public API ----------

    async def __aenter__(self) -> "QFEXTakerClient":
        self.log.info(
            "starting qfex taker client; is_prod=%s symbols=%s",
            self.cfg.is_prod,
            self.cfg.symbol_list,
        )
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._run_mds_loop(), name="mds_loop"),
            asyncio.create_task(self._run_trade_loop(), name="trade_loop"),
        ]
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.log.info("stopping qfex taker client")
        self.stop()

        for t in self._tasks:
            t.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []

        await self._safe_close(self._mds_ws)
        await self._safe_close(self._trade_ws)

    def stop(self) -> None:
        self._stop.set()

    def get_position(self, symbol: str) -> PositionState:
        return self.positions.get(symbol, PositionState(symbol=symbol))

    def get_margin_usage(self) -> Dict[str, Decimal]:
        """
        Simple margin view derived from balances stream (pulsed every second).
        """
        return {
            "order_margin": self.balance.order_margin,
            "position_margin": self.balance.position_margin,
            "available_balance": self.balance.available_balance,
        }

    async def send_ioc_limit(
        self,
        symbol: str,
        side: Side,
        quantity: Decimal,
        price: Decimal,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Sends a LIMIT IOC order via trade websocket add_order.
        Returns a dict describing the final outcome we observed for this client_order_id
        (ACK/FILLED/CANCELLED/REJECTED, etc.).
        """
        if symbol not in self.cfg.symbol_list:
            raise ValueError(f"symbol {symbol} not in configured symbol_list")

        client_oid = client_order_id or str(secrets.token_hex(16))
        if client_oid in self._client_oid_to_fut:
            raise ValueError("client_order_id already in use")

        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._client_oid_to_fut[client_oid] = fut

        msg = {
            "type": "add_order",
            "params": {
                "symbol": symbol,
                "side": side.value,
                "order_type": OrderType.LIMIT.value,
                "order_time_in_force": TimeInForce.IOC.value,
                "quantity": float(quantity),
                "price": float(price),
                "take_profit": 0,
                "stop_loss": 0,
                "client_order_id": client_oid,
            },
        }

        try:
            await self._enqueue_trade(msg)
            return await asyncio.wait_for(fut, timeout=self.cfg.order_update_timeout_s)
        finally:
            self._client_oid_to_fut.pop(client_oid, None)

    # ---------- Run loop ----------

    async def run(self) -> None:
        """
        Starts MDS + Trade connections and runs until stop() is called.
        """
        async with self:
            await self._stop.wait()

    # ---------- Internals ----------

    def _domain(self) -> str:
        return "qfex.com" if self.cfg.is_prod else "qfex.io"

    def _mds_url(self) -> str:
        return f"wss://mds.{self._domain()}"

    def _trade_url(self) -> str:
        return f"wss://trade.{self._domain()}?api_key={self.cfg.public_key}"

    def _build_auth_message(self) -> Dict[str, Any]:
        """
        HMAC auth: signature = HMAC_SHA256(secret, f"{nonce}:{unix_ts}")
        """
        nonce = secrets.token_hex(16)
        unix_ts = int(time.time())
        payload = f"{nonce}:{unix_ts}".encode("utf-8")
        signature = hmac.new(
            self.cfg.secret_key.encode("utf-8"), payload, hashlib.sha256
        ).hexdigest()
        return {
            "type": "auth",
            "params": {
                "hmac": {
                    "public_key": self.cfg.public_key,
                    "nonce": nonce,
                    "unix_ts": unix_ts,
                    "signature": signature,
                }
            },
        }

    async def _enqueue_trade(self, msg: Dict[str, Any]) -> None:
        # If we’re stopping, refuse new orders
        if self._stop.is_set():
            raise RuntimeError("client is stopping; cannot send")
        try:
            self._trade_out_q.put_nowait(msg)
        except asyncio.QueueFull:
            raise RuntimeError("trade outbound queue full (backpressure)")

    async def _run_mds_loop(self) -> None:
        delay = self.cfg.reconnect_base_delay_s
        while not self._stop.is_set():
            try:
                await self._mds_session()
                delay = self.cfg.reconnect_base_delay_s
            except asyncio.CancelledError:
                return
            except Exception:
                self.log.exception("mds loop error; reconnecting")
                await asyncio.sleep(self._backoff(delay))
                delay = min(delay * 2, self.cfg.reconnect_max_delay_s)

    async def _run_trade_loop(self) -> None:
        delay = self.cfg.reconnect_base_delay_s
        while not self._stop.is_set():
            try:
                await self._trade_session()
                delay = self.cfg.reconnect_base_delay_s
            except asyncio.CancelledError:
                return
            except Exception:
                self.log.exception("trade loop error; reconnecting")
                await asyncio.sleep(self._backoff(delay))
                delay = min(delay * 2, self.cfg.reconnect_max_delay_s)

    def _backoff(self, base_delay: float) -> float:
        # exponential backoff with jitter
        return base_delay * (0.7 + random.random() * 0.6)

    async def _mds_session(self) -> None:
        url = self._mds_url()
        self.log.info("connecting mds: %s", url)

        async with websockets.connect(
            url,
            open_timeout=self.cfg.connect_timeout_s,
            ping_interval=self.cfg.heartbeat_interval_s,
            ping_timeout=self.cfg.heartbeat_timeout_s,
            close_timeout=2.0,
            max_queue=1024,
        ) as ws:
            self._mds_ws = ws
            await self._mds_subscribe(ws)

            async for raw in ws:
                await self._handle_mds_message(raw)

    async def _mds_subscribe(self, ws: ClientConnection) -> None:
        # Subscribe to BBO and public trades for configured symbols.
        sub_bbo = {
            "type": "subscribe",
            "channels": ["bbo"],
            "symbols": self.cfg.symbol_list,
        }
        sub_trd = {
            "type": "subscribe",
            "channels": ["trade"],
            "symbols": self.cfg.symbol_list,
        }
        await ws.send(json.dumps(sub_bbo))
        await ws.send(json.dumps(sub_trd))
        self.log.info("mds subscribed: bbo+trade symbols=%s", self.cfg.symbol_list)

    async def _handle_mds_message(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        msg = json.loads(raw)

        mtype = msg.get("type")
        if mtype == "bbo":
            b = self._parse_bbo(msg)
            self._sequence_check("bbo", b.symbol, b.sequence)
            self.bbo_by_symbol[b.symbol] = b
            await self.strategy.on_bbo(b)

        elif mtype == "trade":
            # public trades stream
            sym = msg.get("symbol")
            seq = msg.get("sequence")
            if isinstance(sym, str) and isinstance(seq, int):
                self._sequence_check("trade", sym, seq)
            await self.strategy.on_trade(msg)

        else:
            return

    def _sequence_check(self, channel: str, symbol: str, seq: Optional[int]) -> None:
        if seq is None:
            return
        key = (channel, symbol)
        last = self._mds_seq_last.get(key)
        if last is not None and seq <= last:
            self.log.warning(
                "mds out-of-order: channel=%s symbol=%s last=%s now=%s",
                channel,
                symbol,
                last,
                seq,
            )
        self._mds_seq_last[key] = seq

    def _parse_bbo(self, msg: Dict[str, Any]) -> BBO:
        symbol = msg["symbol"]
        bid = msg.get("bid") or []
        ask = msg.get("ask") or []
        bid_px, bid_qty = (None, None)
        ask_px, ask_qty = (None, None)

        if bid and isinstance(bid, list) and len(bid[0]) >= 2:
            bid_px = Decimal(str(bid[0][0]))
            bid_qty = Decimal(str(bid[0][1]))
        if ask and isinstance(ask, list) and len(ask[0]) >= 2:
            ask_px = Decimal(str(ask[0][0]))
            ask_qty = Decimal(str(ask[0][1]))

        return BBO(
            symbol=symbol,
            bid_px=bid_px,
            bid_qty=bid_qty,
            ask_px=ask_px,
            ask_qty=ask_qty,
            sequence=msg.get("sequence"),
            ts=msg.get("time"),
        )

    async def _trade_session(self) -> None:
        url = self._trade_url()
        self.log.info("connecting trade: %s", url)

        async with websockets.connect(
            url,
            open_timeout=self.cfg.connect_timeout_s,
            ping_interval=self.cfg.heartbeat_interval_s,
            ping_timeout=self.cfg.heartbeat_timeout_s,
            close_timeout=2.0,
            max_queue=4096,
        ) as ws:
            self._trade_ws = ws

            # Authenticate within 1 minute of connecting
            await ws.send(json.dumps(self._build_auth_message()))

            # Enable cancel-on-disconnect each reconnect
            await ws.send(
                json.dumps(
                    {
                        "type": "cancel_on_disconnect",
                        "params": {"cancel_on_disconnect": True},
                    }
                )
            )

            # Subscribe to private channels we need
            await ws.send(
                json.dumps(
                    {
                        "type": "subscribe",
                        "params": {
                            "channels": ["order_responses", "balances", "positions"]
                        },
                    }
                )
            )

            # Start a single writer that flushes outbound messages
            writer = asyncio.create_task(self._trade_writer(ws), name="trade_writer")

            try:
                async for raw in ws:
                    await self._handle_trade_message(raw)
            finally:
                writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)

    async def _trade_writer(self, ws: ClientConnection) -> None:
        while True:
            msg = await self._trade_out_q.get()
            try:
                await ws.send(json.dumps(msg))
            except ConnectionClosed:
                # put it back so it can be retried after reconnect (best-effort)
                try:
                    self._trade_out_q.put_nowait(msg)
                except asyncio.QueueFull:
                    self.log.error(
                        "dropping outbound trade msg due to full queue after disconnect: %s",
                        msg,
                    )
                raise

    async def _handle_trade_message(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        msg = json.loads(raw)

        # Errors envelope
        if "err" in msg:
            err = msg["err"]
            code = err.get("error_code")
            self.log.error("trade error: %s", err)

            # If we rate-limit, slow down a bit
            if code == "RateLimited":
                await asyncio.sleep(0.25 + random.random() * 0.5)
            return

        if "balance_response" in msg:
            self._update_balance(msg["balance_response"])
            return

        if "position_response" in msg:
            self._update_position(msg["position_response"])
            return

        if "order_response" in msg:
            await self._handle_order_response(msg["order_response"])
            return

    def _update_balance(self, br: Dict[str, Any]) -> None:
        self.balance.deposit = Decimal(str(br.get("deposit", 0)))
        self.balance.realised_pnl = Decimal(str(br.get("realised_pnl", 0)))
        self.balance.unrealised_pnl = Decimal(str(br.get("unrealised_pnl", 0)))
        self.balance.net_funding = Decimal(str(br.get("net_funding", 0)))
        self.balance.order_margin = Decimal(str(br.get("order_margin", 0)))
        self.balance.position_margin = Decimal(str(br.get("position_margin", 0)))
        self.balance.available_balance = Decimal(str(br.get("available_balance", 0)))

    def _update_position(self, pr: Dict[str, Any]) -> None:
        sym = pr["symbol"]
        ps = self.positions.get(sym, PositionState(symbol=sym))
        ps.position = Decimal(str(pr.get("position", 0)))
        ps.margin_alloc = Decimal(str(pr.get("margin_alloc", 0)))
        ps.realised_pnl = Decimal(str(pr.get("realised_pnl", 0)))
        ps.unrealised_pnl = Decimal(str(pr.get("unrealised_pnl", 0)))
        ps.net_funding = Decimal(str(pr.get("net_funding", 0)))
        ps.open_orders = Decimal(str(pr.get("open_orders", 0)))
        ps.open_quantity = Decimal(str(pr.get("open_quantity", 0)))
        ps.leverage = Decimal(str(pr.get("leverage", 0)))
        ps.initial_margin = Decimal(str(pr.get("initial_margin", 0)))
        ps.maintenance_margin = Decimal(str(pr.get("maintenance_margin", 0)))
        ps.average_price = Decimal(str(pr.get("average_price", 0)))
        self.positions[sym] = ps

    async def _handle_order_response(self, orsp: Dict[str, Any]) -> None:
        status = orsp.get("status")
        order_id = orsp.get("order_id")
        client_oid = orsp.get("client_order_id") or ""
        symbol = orsp.get("symbol")

        qty = Decimal(str(orsp.get("quantity", 0)))
        remaining = Decimal(str(orsp.get("quantity_remaining", qty)))
        px = Decimal(str(orsp.get("price", 0)))

        # Detect fill delta for this order_id
        if isinstance(order_id, str) and order_id:
            last_rem = self._order_last_remaining.get(order_id, qty)
            # filled = qty - remaining, so delta fill = last_remaining - remaining
            filled_qty_delta = last_rem - remaining
            if filled_qty_delta > 0:
                fill_payload = {
                    "order_response": orsp,
                    "filled_qty_delta": filled_qty_delta,
                    "filled_notional_delta": filled_qty_delta * px,
                    "symbol": symbol,
                    "client_order_id": client_oid,
                    "order_id": order_id,
                }
                await self.strategy.on_fill(fill_payload)
            self._order_last_remaining[order_id] = remaining

        # Complete any awaiting future keyed by client_order_id (IOC convenience)
        fut = self._client_oid_to_fut.get(client_oid)
        if fut and not fut.done():
            is_terminal = status in (
                "CANCELLED",
                "REJECTED",
                "NO_SUCH_ORDER",
                "INVALID_ORDER_TYPE",
                "BAD_SYMBOL",
                "FAILED_MARGIN_CHECK",
            )
            # Also treat FILLED with no remaining as terminal
            if status == "FILLED" and remaining == 0:
                is_terminal = True
            if is_terminal:
                fut.set_result({"final_order_response": orsp})

    async def _safe_close(self, ws: Optional[ClientConnection]) -> None:
        if ws is None:
            return
        try:
            await ws.close()
        except Exception:
            return

