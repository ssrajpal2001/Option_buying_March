"""
Client config loader — reads client-specific configuration from environment variables
injected by the instance manager when spawning a client bot subprocess.

Usage in main.py:
    if args.client_mode:
        from hub.client_config import load_client_config
        cfg = load_client_config()
"""

import os
from dataclasses import dataclass, field


@dataclass
class ClientConfig:
    client_id: int
    username: str
    broker: str
    instrument: str
    quantity: int
    strategy_version: str
    trading_mode: str          # "paper" or "live"
    api_key: str
    access_token: str
    instance_id: int

    @property
    def is_paper(self) -> bool:
        return self.trading_mode == "paper"

    @property
    def is_live(self) -> bool:
        return self.trading_mode == "live"

    def __repr__(self):
        return (
            f"ClientConfig(client_id={self.client_id}, username={self.username!r}, "
            f"broker={self.broker!r}, instrument={self.instrument!r}, "
            f"qty={self.quantity}, mode={self.trading_mode!r}, "
            f"strategy={self.strategy_version!r})"
        )


def load_client_config() -> ClientConfig:
    """Load client config from environment variables set by instance_manager."""
    required = [
        "CLIENT_ID", "CLIENT_USERNAME", "CLIENT_BROKER",
        "CLIENT_INSTRUMENT", "CLIENT_QUANTITY", "CLIENT_STRATEGY_VERSION",
        "CLIENT_TRADING_MODE", "CLIENT_API_KEY", "CLIENT_ACCESS_TOKEN",
        "CLIENT_INSTANCE_ID",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(
            f"ClientConfig: Missing required env vars: {', '.join(missing)}"
        )

    return ClientConfig(
        client_id=int(os.environ["CLIENT_ID"]),
        username=os.environ["CLIENT_USERNAME"],
        broker=os.environ["CLIENT_BROKER"],
        instrument=os.environ["CLIENT_INSTRUMENT"],
        quantity=int(os.environ["CLIENT_QUANTITY"]),
        strategy_version=os.environ["CLIENT_STRATEGY_VERSION"],
        trading_mode=os.environ["CLIENT_TRADING_MODE"],
        api_key=os.environ["CLIENT_API_KEY"],
        access_token=os.environ["CLIENT_ACCESS_TOKEN"],
        instance_id=int(os.environ["CLIENT_INSTANCE_ID"]),
    )
