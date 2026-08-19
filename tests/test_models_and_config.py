from decimal import Decimal

from qfex.config import QFEXConfig
from qfex.models import BBO, OrderType, Side, TimeInForce


def test_config_defaults_are_safe_for_a_new_client() -> None:
    config = QFEXConfig(
        is_prod=False,
        symbol_list=["BTC-USD"],
        public_key="public",
        secret_key="secret",
    )

    assert config.connect_timeout_s == 10.0
    assert config.heartbeat_interval_s == 15.0
    assert config.heartbeat_timeout_s == 10.0
    assert config.reconnect_base_delay_s == 0.5
    assert config.reconnect_max_delay_s == 15.0
    assert config.order_update_timeout_s == 5.0
    assert config.outbound_queue_max == 10_000


def test_bbo_preserves_decimal_market_values() -> None:
    bbo = BBO(
        symbol="BTC-USD",
        bid_px=Decimal("100.25"),
        bid_qty=Decimal("0.5"),
        ask_px=Decimal("100.50"),
        ask_qty=Decimal("0.75"),
        sequence=42,
        ts="2026-08-20T00:00:00Z",
    )

    assert bbo.bid_px == Decimal("100.25")
    assert bbo.ask_qty == Decimal("0.75")
    assert bbo.sequence == 42


def test_order_enums_match_wire_values() -> None:
    assert Side.BUY.value == "BUY"
    assert Side.SELL.value == "SELL"
    assert OrderType.LIMIT.value == "LIMIT"
    assert OrderType.MARKET.value == "MARKET"
    assert TimeInForce.IOC.value == "IOC"
    assert TimeInForce.GTC.value == "GTC"
    assert TimeInForce.FOK.value == "FOK"
