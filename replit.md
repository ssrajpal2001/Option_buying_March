# AlgoSoft Trading Platform

## Overview
Multi-tenant SaaS algorithmic trading platform for Indian markets (NSE/BSE).
- **Admin**: manages Upstox global data provider token, activates clients, monitors all trades
- **Clients**: log in, connect their own Zerodha broker (API key + access token), set quantity/instrument, click "Start Bot"
- Each client gets an **isolated bot subprocess** with their own config, log file, and order book

## Architecture

### Auth
- JWT cookie-based sessions (`access_token` cookie, 24hr)
- Passwords hashed with bcrypt (direct, not passlib — see web/auth.py)
- Fernet symmetric encryption for stored broker credentials
- Default admin: `admin` / `Admin@123` (auto-created on first run)

### Database
- **Dev**: SQLite at `config/algosoft.db` (auto-migrated on startup)
- **Prod**: PostgreSQL (schema in `config/schema.sql`)
- Tables: users, data_providers, client_broker_instances, trade_history, order_failures, audit_log

### API Routes
- `/api/auth/*` — register, login, logout, me
- `/api/admin/*` — data provider config, client management, overview
- `/api/client/*` — broker setup, bot start/stop/status, trade history

### Page Routes
- `/` → redirect based on role
- `/login`, `/register` — auth pages
- `/admin`, `/admin/clients`, `/admin/clients/{id}`, `/admin/data-provider` — admin pages
- `/dashboard` — client trading dashboard (Settings + Order Book tabs)

## Key Files

| File | Purpose |
|------|---------|
| `web/server.py` | FastAPI app, all route mounts |
| `web/auth.py` | bcrypt password hash, JWT, Fernet encryption |
| `web/db.py` | SQLite connection, auto-migration, seed |
| `web/auth_api.py` | /api/auth/* routes |
| `web/admin_api.py` | /api/admin/* routes |
| `web/client_api.py` | /api/client/* routes |
| `web/deps.py` | Auth dependencies (get_current_user, require_admin) |
| `hub/instance_manager.py` | Spawns/stops per-client bot subprocesses |
| `hub/order_state_machine.py` | Atomic CE+PE entry with 2x retry + leg rollback |
| `hub/client_config.py` | Reads client config from env vars (injected by instance_manager) |
| `config/schema.sql` | PostgreSQL production schema |
| `EC2_SETUP.md` | EC2 deployment guide |

## Strategy
- V3 sell strategy: `hub/sell_manager_v3.py` (branch: v3-logic-enhancement-*)
- ATM entry at 09:16, 12% profit target, Combined VWAP/RSI exit, Dynamic TSL
- Entry state persisted to `config/sell_v3_state_<instrument>.json`

## Subscription Tiers
- **FREE**: 1 broker (Zerodha)
- **PREMIUM**: 3 brokers

## Order Safety
- Atomic CE+PE: if one leg fills and the other fails → immediately exit the filled leg
- 2x retry with 5s/15s backoff before abort
- Failure logged to `order_failures` table

## EC2 Deployment
- Instance: i-0fbce3bd332ddff2b
- Dashboard: http://13.234.185.209:5000
- Repo: ssrajpal2001/Option_buying_March (branch: master)
- See `EC2_SETUP.md` for full setup instructions

## Running Locally
```bash
python -m uvicorn web.server:app --host 0.0.0.0 --port 5000 --reload
```
