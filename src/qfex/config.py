import logging
from dataclasses import dataclass
from typing import List


# SDK-wide configuration object shared by all QFEX SDK components (meta, order
# routing, streams). Taker-specific knobs live next to the TakerStrategy usage.
@dataclass(frozen=True)
class QFEXConfig:
    is_prod: bool
    symbol_list: List[str]
    public_key: str
    secret_key: str

    # Reliability knobs
    connect_timeout_s: float = 10.0
    heartbeat_interval_s: float = 15.0
    heartbeat_timeout_s: float = 10.0
    reconnect_base_delay_s: float = 0.5
    reconnect_max_delay_s: float = 15.0

    # Order handling
    order_update_timeout_s: float = 5.0  # IOC should resolve quickly; tune if needed

    # Backpressure
    outbound_queue_max: int = 10_000

    # Logging
    log_level: int = logging.INFO
