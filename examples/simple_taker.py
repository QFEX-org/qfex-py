import argparse
import asyncio
import logging
import os
import signal
from decimal import Decimal
from typing import Dict, Any

from qfex import QFEXTakerClient, QFEXConfig, TakerStrategy, BBO, Side

# ----------------------------
# Example user strategy + CLI
# ----------------------------


class ExampleTaker(TakerStrategy):
    async def on_bbo(self, bbo: BBO) -> None:
        # Example: print top-of-book
        if bbo.bid_px is not None and bbo.ask_px is not None:
            self.client.log.info(
                "BBO %s bid=%s@%s ask=%s@%s",
                bbo.symbol,
                bbo.bid_qty,
                bbo.bid_px,
                bbo.ask_qty,
                bbo.ask_px,
            )

    async def on_trade(self, trade_msg: Dict[str, Any]) -> None:
        # Example: ignore
        return

    async def on_fill(self, fill: Dict[str, Any]) -> None:
        # Example: log fills
        orsp = fill["order_response"]
        self.client.log.info(
            "FILL %s side=%s px=%s dqty=%s rem=%s status=%s trade_id=%s",
            orsp.get("symbol"),
            orsp.get("side"),
            orsp.get("price"),
            fill["filled_qty_delta"],
            orsp.get("quantity_remaining"),
            orsp.get("status"),
            orsp.get("trade_id"),
        )


async def main() -> None:
    parser = argparse.ArgumentParser("qfex-taker")
    parser.add_argument(
        "--is-prod",
        action="store_true",
        help="use qfex.com (prod). default uses qfex.io (uat)",
    )
    parser.add_argument(
        "--symbols",
        required=True,
        help="comma-separated symbols, e.g. AAPL-USD,SP500-USD",
    )
    parser.add_argument(
        "--public-key",
        default="",
        help="qfex public key (or set QFEX_PUBLIC_KEY env var)",
    )
    parser.add_argument(
        "--secret-key",
        default="",
        help="qfex secret key (or set QFEX_SECRET_KEY env var)",
    )
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()

    # Configure logging (Application responsibility)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Environment variables are preferred over CLI argv; argv can leak keys into
    # shell history and process listings, so env vars are the safer option.
    public_key = os.environ.get("QFEX_PUBLIC_KEY", args.public_key)
    secret_key = os.environ.get("QFEX_SECRET_KEY", args.secret_key)

    cfg = QFEXConfig(
        is_prod=bool(args.is_prod),
        symbol_list=[s.strip() for s in args.symbols.split(",") if s.strip()],
        public_key=public_key,
        secret_key=secret_key,
        log_level=logging.INFO,  # Passed to config but logic moved to app setup
    )

    # build client + strategy
    client = QFEXTakerClient(cfg)
    strat = ExampleTaker(client)
    client.set_strategy(strat)  # late bind

    # graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, client.stop)
        except NotImplementedError:
            pass

    await client.run()


if __name__ == "__main__":
    asyncio.run(main())
