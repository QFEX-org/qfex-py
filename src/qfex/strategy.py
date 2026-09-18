from __future__ import annotations
from typing import TYPE_CHECKING, Dict, Any

if TYPE_CHECKING:
    from .client import QFEXTakerClient
    from .models import BBO


class TakerStrategy:
    """
    Base class for user strategies.
    Subclasses can override any callback of interest without being forced
    to implement every hook. Default implementations are no-ops so that
    unimplemented callbacks do not throw unhandled exceptions in the client loops.
    """

    def __init__(self, client: "QFEXTakerClient"):
        self.client = client

    async def on_bbo(self, bbo: "BBO") -> None:
        """Called when top-of-book best bid/offer updates."""
        pass

    async def on_trade(self, trade_msg: Dict[str, Any]) -> None:
        """Called on public trade executions from the market data stream."""
        pass

    async def on_fill(self, fill: Dict[str, Any]) -> None:
        """
        Called when a fill delta is detected for an order belonging to this account.
        Payload keys:
          - order_response (raw dict)
          - filled_qty_delta (Decimal)
          - filled_notional_delta (Decimal)
          - symbol (str)
          - client_order_id (str)
          - order_id (str)
        """
        pass
