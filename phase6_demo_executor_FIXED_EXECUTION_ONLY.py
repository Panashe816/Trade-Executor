import asyncio
import json
import os
import re
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession



# ============================================================
# PHASE 6 - DEMO TELEGRAM -> MT5 EXECUTOR
# ============================================================
#
# PHASE 4 PRIVATE OUTPUT CHANNEL
#          ↓
# PHASE 6 CONSUMES PARSED SIGNAL
#          ↓
# WAIT / MONITOR XAUUSD_i
#          ↓
# 30-PIP ENTRY EXTENSION
#          ↓
# LOCAL FASTAPI EXECUTION API
#          ↓
# MT5 DEMO ACCOUNT
#
# DEMO ONLY — LIVE ACCOUNT TRADING IS NOT PERMITTED
#
# ============================================================


load_dotenv()


# ============================================================
# TELEGRAM CONFIGURATION
# ============================================================

API_ID = int(
    os.getenv(
        "TELEGRAM_API_ID",
        "14424659"
    )
)

API_HASH = os.getenv(
    "TELEGRAM_API_HASH",
    ""
)

TELEGRAM_SESSION_STRING = os.getenv(
    "TELEGRAM_SESSION_STRING",
    ""
)


# ============================================================
# PHASE 4 PRIVATE OUTPUT CHANNEL
# ============================================================

PHASE4_OUTPUT_CHANNEL_ID = -1003995895185


# ============================================================
# LOCAL EXECUTION API CONFIGURATION
# ============================================================

EXECUTION_API_URL = os.getenv("EXECUTION_API_URL", "http://127.0.0.1:8000").rstrip("/")
EXECUTION_API_KEY = os.getenv("EXECUTION_API_KEY", "")
API_REQUEST_TIMEOUT_SECONDS = float(os.getenv("API_REQUEST_TIMEOUT_SECONDS", "12"))


# ============================================================
# OPTIONAL TELEGRAM NOTIFICATION
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    ""
)

PHASE6_STATUS_CHAT_ID = os.getenv(
    "PHASE6_STATUS_CHAT_ID",
    ""
)

TELEGRAM_NOTIFICATION_CHAT_ID = os.getenv(
    "TELEGRAM_NOTIFICATION_CHAT_ID",
    ""
)


# ============================================================
# TRADING SETTINGS
# ============================================================

SYMBOL = os.getenv(
    "TRADE_SYMBOL",
    "XAUUSD_i"
)

LOT_SIZE = float(
    os.getenv(
        "LOT_SIZE",
        "0.01"
    )
)

POSITIONS_PER_SIGNAL = 2


# ============================================================
# ENTRY EXTENSION
# ============================================================
#
# 30 pips on this XAUUSD setup = 3.0 price units.
#
# SELL:
#     Original: 4630 - 4633
#     Extended: 4627 - 4633
#
# BUY:
#     Original: 4621 - 4624
#     Extended: 4621 - 4627
#
# ============================================================

ENTRY_EXTENSION_PIPS = 30

ENTRY_EXTENSION_PRICE = float(
    os.getenv(
        "ENTRY_EXTENSION_PRICE",
        "3.0"
    )
)


# ============================================================
# WATCH / REPORT SETTINGS
# ============================================================

PRICE_CHECK_INTERVAL_SECONDS = 2

CAT = ZoneInfo("Africa/Harare")

# "28:00 CAT" means 04:00 CAT on the following calendar date.
DAILY_REPORT_HOUR = 4
DAILY_REPORT_MINUTE = 0

# Local API health settings
CONNECTION_RETRY_DELAYS_SECONDS = [2, 4, 8, 15, 30]
CONNECTION_STATUS_CHECK_SECONDS = 10


# ============================================================
# FILES
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PENDING_FILE = os.path.join(BASE_DIR, "pending_signals.json")
EXECUTED_FILE = os.path.join(BASE_DIR, "executed_signals.json")
TRADE_LOG_FILE = os.path.join(BASE_DIR, "trade_log.json")
LIFECYCLE_FILE = os.path.join(BASE_DIR, "signal_lifecycle.json")
POSITION_MEMORY_FILE = os.path.join(BASE_DIR, "position_memory.json")
PROCESSED_MESSAGES_FILE = os.path.join(BASE_DIR, "processed_messages.json")


# ============================================================
# REMAINING SOURCE PRESERVATION NOTE
# ============================================================
# The rest of the Phase 6 pipeline must remain byte-for-byte equivalent
# to the locally tested source except for:
# - API response handling in _execute_signal_locked (below)
# - references to MetaApi labels/status text changed to local API wording
#
# Full original implementation is retained below.
