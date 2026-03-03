# AlgoSoft Trading Bot

## Overview
A Python-based algorithmic trading bot for Indian markets (NSE/BSE). Supports multiple brokers (Zerodha, Upstox, Dhan, AngelOne, PaperTrade) and multiple instruments (NIFTY, BANKNIFTY, SENSEX, MIDCAP, FINNIFTY). Supports both live trading and backtesting modes. Includes a web-based dashboard for live monitoring and management.

## Architecture

### Core Bot
- **main.py**: Entry point, orchestrates all components
- **hub/**: Core business logic — orchestrators, managers, data feeds, event bus
  - `live_orchestrator.py` — wires all managers + creates StatusWriter
  - `tick_processor.py` — per-tick routing; calls StatusWriter.maybe_write()
  - `sell_manager.py` — short strangle leg management (NRML)
  - `oi_exit_monitor.py` — OI-based exit logic via Upstox websocket
  - `status_writer.py` — writes `config/bot_status.json` every 5s for dashboard
- **brokers/**: Broker client implementations (Zerodha, Dhan, AngelOne, PaperTrade)
- **utils/**: Utilities — config manager, database manager, logger, auth managers, indicators
- **scripts/**: One-off utility scripts
- **analysis/**: Data analysis tools
- **config/**: Configuration files

### Web Dashboard
- **web/server.py**: FastAPI app, Jinja2 templates, mounts /static, page routes
- **web/status_api.py**: `GET /api/status` — reads bot_status.json (returns {bot_running:false} if stale >30s)
- **web/config_api.py**: `GET/POST /api/strategy` — reads/writes strategy_logic.json atomically
- **web/broker_api.py**: Token refresh, active broker switch, credential edit (masked output)
- **web/bot_control.py**: `POST /api/bot/restart` — SIGTERM + Popen main.py
- **web/templates/**: Jinja2 HTML (base.html, dashboard.html, strategy.html, brokers.html)
- **web/static/app.js**: Shared JS (polling, toast notifications, nav status)

## Configuration Files
- `config/config_trader.ini`: Main application config (instruments, brokers, database, Redis)
- `config/credentials.ini`: API keys and tokens for all brokers
- `config/strategy_logic.json`: All strategy parameters (read live every tick, no restart needed)
- `config/bot_status.json`: Written by bot every 5s, read by web dashboard
- `config/schema.sql`: PostgreSQL schema

## Key Settings
- **Mode**: Backtest enabled by default (`backtest_enabled = true`)
- **Instrument**: NIFTY
- **Active broker**: PaperTrade (configured in config_trader.ini)
- **Database**: PostgreSQL (optional, `required = false`)
- **Redis**: Disabled by default

## Strategy Logic (strategy_logic.json)
All strategy parameters live in `config/strategy_logic.json` — no hardcoding in Python.

### Buy Side (directional MIS trades)
- **Start time**: `NIFTY.buy.start_time` = `"09:17"`
- **Product type**: MIS (via trade_executor)
- **Strike**: Always ATM (signal_monitor picks current ATM)
- **Entry formula**: `vwap_slope` (rising slope)
- **Exit formula**: `vwap_slope` (falling slope), TSL, ATR-TSL

### Sell Side (short strangle — individual legs, NRML)
- **Start time**: `NIFTY.sell.start_time` = `"09:20"`
- **Product type**: NRML
- **Candidates**: ATM + ITM strikes (`itm_count = 2`, so 3 per side)
- **LTP filter**: `ltp_min = 50`, `ltp_target = 50`
- **Entry trigger**: VWAP slope DECREASING on selected candidate (CE and PE independent)
- **Exit conditions**:
  1. LTP < `ltp_exit_min` (20) → exit leg → restart search
  2. VWAP slope rising on side → exit that leg
  3. OI rise above threshold → exit paired legs (OI Exit Monitor)

## OI Exit Monitor
- Monitors Call OI and Put OI live from Upstox websocket
- Call OI rise > `call_oi_increase_pct` → exit Sell PE + Buy CE
- Put OI rise > `put_oi_increase_pct` → exit Sell CE + Buy PE
- All params in `strategy_logic.json.oi_exit`: `enabled`, `strikes_range`, `call_oi_increase_pct`, `put_oi_increase_pct`, `check_interval_seconds`

## Workflows
Two separate processes that can run simultaneously:
1. **Start application** (webview, port 5000): `python -m uvicorn web.server:app --host 0.0.0.0 --port 5000 --reload`
2. **Trading Bot** (console): `python main.py`

## Web Dashboard Pages
- `/` — Live Dashboard: ATM, spot, P&L cards, buy/sell position panels, OI snapshot, bot log tail
- `/strategy` — Strategy Settings: all toggles + parameter inputs, saved instantly to JSON
- `/brokers` — Broker Management: daily token refresh, active broker selector, credential edit modal

## EC2 Deployment
Run both processes permanently on EC2:
```bash
# Start web dashboard (keep alive)
nohup python -m uvicorn web.server:app --host 0.0.0.0 --port 5000 &

# Start trading bot (keep alive)
nohup python main.py &
```
Use systemd services for auto-restart on reboot. Access dashboard at `http://<ec2-ip>:5000`.

### Daily Workflow (Upstox token refresh)
1. Open browser → `http://<ec2-ip>:5000/brokers`
2. Select broker account (upstox_1 / upstox_2)
3. Paste new access token from Upstox developer portal
4. Click "Update Token & Restart Bot"

## Inter-process Communication
- **Bot → Dashboard**: `config/bot_status.json` written every 5s by StatusWriter
- **Dashboard → Bot**: `config/strategy_logic.json` written atomically; bot reads on next tick
- **Dashboard → Bot**: `config/credentials.ini` updated by broker API; bot restart required for broker change

## Dependencies
All dependencies from requirements.txt:
numpy, scipy, pandas, upstox-python-sdk, websockets, requests, pytest, pytest-asyncio, pytest-mock, aiohttp, pytz, protobuf, sqlalchemy, aiopg, asyncpg, cryptography, redis, hiredis, smartapi-python, pyotp, kiteconnect, dhanhq, fastapi, uvicorn, jinja2, python-multipart

## Formula-Aware Indicator Evaluation
- `hub/signal_evaluator.py`: Only evaluates indicators (VWAP, slope, R1, S1) that appear in the entry_formula from strategy_logic.json.
- `hub/exit_evaluator.py`: Exit slope evaluation with descriptive error logging.
- `hub/signal_monitor.py`: Displays only indicators present in the formula.
