# AlgoSoft Trading Bot

## Overview
A Python-based algorithmic trading bot for Indian markets (NSE/BSE). Supports multiple brokers (Zerodha, Upstox, Dhan, AngelOne, PaperTrade) and multiple instruments (NIFTY, BANKNIFTY, SENSEX, MIDCAP, FINNIFTY). Supports both live trading and backtesting modes.

## Architecture
- **main.py**: Entry point, orchestrates all components
- **hub/**: Core business logic — orchestrators, managers, data feeds, event bus
- **brokers/**: Broker client implementations (Zerodha, Dhan, AngelOne, PaperTrade)
- **utils/**: Utilities — config manager, database manager, logger, auth managers, indicators
- **scripts/**: One-off utility scripts
- **analysis/**: Data analysis tools
- **tools/**: Verification tools
- **config/**: Configuration files (config_trader.ini, credentials.ini, strategy_logic.json, schema.sql)

## Configuration
- `config/config_trader.ini`: Main application config (instruments, brokers, strategy settings, database, Redis)
- `config/credentials.ini`: API keys and tokens for all brokers
- `config/strategy_logic.json`: Strategy parameters
- `config/schema.sql`: PostgreSQL schema

## Key Settings
- **Mode**: Backtest enabled by default (`backtest_enabled = true`)
- **Instrument**: NIFTY
- **Active broker**: Zerodha_Client_Live_1
- **Database**: PostgreSQL (optional, `required = false`)
- **Redis**: Disabled by default

## Runtime
- Python 3.12
- Single workflow: `python main.py` (console output)
- No frontend, no web server

## Formula-Aware Indicator Evaluation
- `hub/signal_evaluator.py`: Only evaluates indicators (VWAP, slope, R1, S1) that appear in the entry_formula from strategy_logic.json. Slope evaluation wrapped in try/except with descriptive error logging.
- `hub/exit_evaluator.py`: Exit slope evaluation wrapped in try/except with descriptive error logging.
- `hub/signal_monitor.py`: `format_state()` only displays indicators present in the formula (no hardcoded R1/S1 exceptions).
- Status log only shows formula-relevant indicators (e.g., with entry_formula="vwap_slope", only slope info is displayed).

## Dependencies
All dependencies from requirements.txt:
numpy, scipy, pandas, upstox-python-sdk, websockets, requests, pytest, pytest-asyncio, pytest-mock, aiohttp, pytz, protobuf, sqlalchemy, aiopg, asyncpg, cryptography, redis, hiredis, smartapi-python, pyotp, kiteconnect, dhanhq
