# AlgoSoft EC2 Setup Guide

## Prerequisites
- EC2: i-0fbce3bd332ddff2b (Ubuntu 22.04)
- Python 3.11+ installed
- Port 5000 open in security group

---

## 1. Pull Latest Code from GitHub

```bash
cd /home/ubuntu/Option_buying_March
git pull origin master
```

---

## 2. Install Python Dependencies

```bash
pip install bcrypt python-jose[cryptography] cryptography passlib
# Or update from requirements.txt:
pip install -r requirements.txt
```

---

## 3. SQLite Setup (Development / Single-Server)

The DB auto-creates at `config/algosoft.db` on first startup.  
Default admin: **username=`admin`** / **password=`Admin@123`**

No extra setup needed — just start the server.

---

## 4. PostgreSQL Setup (Production)

### Install PostgreSQL
```bash
sudo apt update && sudo apt install -y postgresql postgresql-contrib
sudo systemctl start postgresql && sudo systemctl enable postgresql
```

### Create DB and User
```bash
sudo -u postgres psql << SQL
CREATE DATABASE algosoft;
CREATE USER algosoft_user WITH ENCRYPTED PASSWORD 'yourpassword';
GRANT ALL PRIVILEGES ON DATABASE algosoft TO algosoft_user;
SQL
```

### Run Schema
```bash
sudo -u postgres psql -d algosoft -f /home/ubuntu/Option_buying_March/config/schema.sql
```

### Seed Default Admin
```bash
# Generate bcrypt hash
python3 -c "import bcrypt; print(bcrypt.hashpw(b'Admin@123', bcrypt.gensalt()).decode())"
# Then insert:
sudo -u postgres psql -d algosoft << SQL
INSERT INTO users (username, email, password_hash, role, is_active)
VALUES ('admin', 'admin@algosoft.com', '<HASH_FROM_ABOVE>', 'admin', true)
ON CONFLICT (username) DO NOTHING;
SQL
```

---

## 5. Environment Variables

```bash
export ALGOSOFT_SECRET="your-long-random-secret-at-least-32-chars"
export ALGOSOFT_DB_PATH="config/algosoft.db"   # SQLite
# For PostgreSQL (future): export DATABASE_URL="postgresql://algosoft_user:yourpassword@localhost/algosoft"
```

Add to `/etc/systemd/system/algosoft.service` for persistence.

---

## 6. Start the Server

```bash
cd /home/ubuntu/Option_buying_March
python -m uvicorn web.server:app --host 0.0.0.0 --port 5000 --reload
```

Or with systemd:
```bash
sudo systemctl start algosoft
sudo systemctl enable algosoft
```

---

## 7. Verify

- Dashboard: http://13.234.185.209:5000
- Login: admin / Admin@123
- Admin panel: http://13.234.185.209:5000/admin

---

## 8. First-Time Admin Tasks

1. Log in as admin → go to **Data Provider** → enter Upstox API key + token
2. Go to **Clients** → activate registered clients
3. Clients log in → go to **Settings** → enter Zerodha API key → click **Start Bot**

---

## 9. Client Bot Logs

Each client bot logs to: `logs/client_{client_id}_{broker}.log`

```bash
tail -f logs/client_1_zerodha.log
```
