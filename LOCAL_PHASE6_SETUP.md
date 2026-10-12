# Local Phase 6 + MT5 API setup (demo only)

This branch replaces Phase 6's MetaApi order path with the local FastAPI service. Keep the `main` branch and existing Render deployment unchanged until local testing passes.

## 1. Windows prerequisites

- Windows laptop, connected to power and the internet.
- MetaTrader 5 installed and logged in to the intended **demo** account.
- Python 3.11 or 3.12 recommended.
- Existing Telegram API credentials and Phase 6 StringSession available locally.

Do not commit or share your `.env` file. Do not paste API keys, Telegram session strings, or passwords into chat.

## 2. Install packages

Open PowerShell in the repository folder and run:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -r api_requirements.txt
```

If PowerShell blocks activation, use `.\.venv\Scripts\python.exe -m pip ...` rather than changing system-wide execution policy.

## 3. Configure the local .env

Create or update a local `.env` in the repository root. Keep all real values only on this computer. Required settings include:

```dotenv
# Local API authentication: these two values must match exactly
API_KEY=replace_with_a_long_random_secret
EXECUTION_API_KEY=replace_with_the_same_long_random_secret
EXECUTION_API_URL=http://127.0.0.1:8000

# Explicit demo-only order gate
ENABLE_DEMO_TRADING=true

# Phase 6 settings
TRADE_SYMBOL=XAUUSD_i
LOT_SIZE=0.01

# Existing Telegram values: fill in locally, never commit them
TELEGRAM_API_ID=your_telegram_api_id
TELEGRAM_API_HASH=your_telegram_api_hash
TELEGRAM_SESSION_STRING=your_existing_string_session
```

Keep any other Telegram notification settings already used by your working Phase 6 setup. The API currently expects the MT5 terminal at `C:\\Program Files\\MetaTrader 5\\terminal64.exe`; change `TERMINAL_PATH` in `main.py` if your installation is elsewhere.

## 4. Start in this order

**Terminal 1 — MT5 API:** Open MT5, log in to the demo account, ensure the exact broker symbol `XAUUSD_i` is visible in Market Watch, then run:

```powershell
.\.venv\Scripts\Activate.ps1
uvicorn main:app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000/health`. It must report `mt5_connected: true` and `account_authenticated: true`. Then open `http://127.0.0.1:8000/docs` to inspect the API. Do not expose port 8000 to the public internet.

**Terminal 2 — Phase 6:** In the same repository folder and with the same `.env`, run:

```powershell
.\.venv\Scripts\Activate.ps1
python -u phase6_demo_executor_FIXED_EXECUTION_ONLY.py
```

Phase 6 should confirm the local API is responding, verify that the account is demo, verify `XAUUSD_i`, and then start the Telegram listener.

## 5. Safety and operating rules

- The API is designed to reject order execution unless `ENABLE_DEMO_TRADING=true` and the MT5 account is confirmed as demo.
- Each signal opens two separate positions, each fixed at **0.01 lot**. Do not set Phase 6 `LOT_SIZE` to another value.
- The API records stable request IDs to reduce duplicate submissions. If an order request times out or returns an uncertain result, inspect the API ledger, MT5 open positions, and history before any manual retry.
- Start with observation and diagnostics. Do not assume the integration is proven until a demo signal has been received, both positions have been reconciled, SL/TP are correct, and a closed position's history/P&L has been verified.
- Keep the laptop on AC power, disable sleep/hibernate while operating, maintain internet access, and keep both terminal windows open. If Windows restarts, MT5 closes, the laptop sleeps, or the internet drops, execution and monitoring may stop.
- This is not a 24/7 server. For dependable unattended operation, a Windows VPS is generally more suitable once local demo testing is complete.

## 6. Rollback

This work is isolated on `replace-metaapi-with-own-api`. Do not merge it into `main` or change Render until the local test checklist passes. The existing Render deployment should remain on its current known-good branch/version during testing.
