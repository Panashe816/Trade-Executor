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
from metaapi_cloud_sdk import MetaApi


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
# METAAPI
#          ↓
# MT5 DEMO ACCOUNT
#
# NO LIVE ACCOUNT TRADING
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
# METAAPI CONFIGURATION
# ============================================================

METAAPI_TOKEN = os.getenv(
    "METAAPI_TOKEN",
    ""
)

METAAPI_ACCOUNT_ID = os.getenv(
    "METAAPI_ACCOUNT_ID",
    ""
)


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

# MetaApi recovery settings
CONNECTION_RETRY_DELAYS_SECONDS = [2, 4, 8, 15, 30]
CONNECTION_STATUS_CHECK_SECONDS = 10


# ============================================================
# FILES
# ============================================================

EXECUTED_FILE = (
    "phase6_executed_signals.json"
)

TRADE_LOG_FILE = (
    "phase6_trade_log.json"
)

PENDING_FILE = (
    "phase6_pending_signals.json"
)

LIFECYCLE_FILE = (
    "phase6_signal_lifecycle.json"
)

DAILY_REPORT_FILE = (
    "phase6_daily_reports.json"
)


# ============================================================
# RUNTIME STATE
# ============================================================

executed_signals = set()

pending_signals = {}

lifecycle_signals = {}

daily_reported_dates = set()

metaapi = None

account = None

connection = None

watch_tasks = {}

# Telegram client used for Phase 6 private status/trade notifications.
telegram_client = None

signal_status_report_times = {}
STATUS_HEARTBEAT_SECONDS = 60
SIGNAL_STATUS_REPORT_SECONDS = 30

# MetaApi connection state tracking
metaapi_connection_state = "UNKNOWN"
metaapi_connection_lock = None


# ============================================================
# TIME
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def now_cat():
    return datetime.now(CAT)


def now_iso():
    return now_utc().isoformat()


def parse_datetime(value):
    if not value:
        return None

    text = str(value).strip()

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)

        if parsed.tzinfo is None:
            parsed = parsed.replace(
                tzinfo=CAT
            )

        return parsed

    except Exception:
        pass

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(
                text,
                fmt
            )

            return parsed.replace(
                tzinfo=CAT
            )

        except Exception:
            continue

    return None


def format_cat_time(value):
    parsed = parse_datetime(value)

    if not parsed:
        return str(value)

    return parsed.astimezone(CAT).strftime(
        "%Y-%m-%d %H:%M:%S CAT"
    )


# ============================================================
# GENERIC OBJECT / DICT FIELD HELPER
# ============================================================

def get_field(
    obj,
    field,
    default=None
):

    if isinstance(
        obj,
        dict
    ):

        return obj.get(
            field,
            default
        )

    return getattr(
        obj,
        field,
        default
    )


# ============================================================
# JSON HELPERS
# ============================================================

def load_json_list(path):

    if not os.path.exists(
        path
    ):

        return []

    try:

        with open(
            path,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(
                file
            )

        if isinstance(
            data,
            list
        ):

            return data

        return []

    except Exception as error:

        print(
            f"⚠️ Could not read {path}: {error}"
        )

        return []


def load_json_dict(path):

    if not os.path.exists(
        path
    ):

        return {}

    try:

        with open(
            path,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(
                file
            )

        if isinstance(
            data,
            dict
        ):

            return data

        return {}

    except Exception as error:

        print(
            f"⚠️ Could not read {path}: {error}"
        )

        return {}


def save_json(
    path,
    data
):

    temporary_file = (
        path + ".tmp"
    )

    with open(
        temporary_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            indent=2,
            default=str
        )

    os.replace(
        temporary_file,
        path
    )


# ============================================================
# LOAD RUNTIME MEMORY
# ============================================================

def load_runtime_memory():

    global executed_signals
    global pending_signals
    global lifecycle_signals
    global daily_reported_dates

    rows = load_json_list(
        EXECUTED_FILE
    )

    executed_signals = set()

    for row in rows:

        if isinstance(
            row,
            dict
        ):

            key = row.get(
                "signal_key"
            )

            if key:

                executed_signals.add(
                    str(key)
                )

        elif isinstance(
            row,
            str
        ):

            executed_signals.add(
                row
            )

    pending_signals = load_json_dict(
        PENDING_FILE
    )

    lifecycle_signals = load_json_dict(
        LIFECYCLE_FILE
    )

    report_rows = load_json_list(
        DAILY_REPORT_FILE
    )

    daily_reported_dates = set()

    for row in report_rows:

        if isinstance(
            row,
            dict
        ):

            report_date = row.get(
                "report_date"
            )

            if report_date:

                daily_reported_dates.add(
                    str(report_date)
                )

    print(
        f"Loaded {len(executed_signals)} previously executed signals."
    )

    print(
        f"Loaded {len(pending_signals)} pending signals."
    )

    print(
        f"Loaded {len(lifecycle_signals)} lifecycle records."
    )


# ============================================================
# SAVE EXECUTED SIGNAL
# ============================================================

def save_executed_signal(
    signal,
    positions
):

    signal_key = signal[
        "signal_id"
    ]

    rows = load_json_list(
        EXECUTED_FILE
    )

    rows = [
        row
        for row in rows
        if not (
            isinstance(row, dict)
            and row.get("signal_key") == signal_key
        )
    ]

    rows.append({

        "signal_key":
            signal_key,

        "timestamp":
            now_iso(),

        "channel":
            signal["channel"],

        "chat_id":
            signal["chat_id"],

        "message_id":
            signal["message_id"],

        "signal_id":
            signal["signal_id"],

        "signal_time":
            signal["signal_time"],

        "valid_until":
            signal["valid_until"],

        "direction":
            signal["direction"],

        "entry_low":
            signal["entry_low"],

        "entry_high":
            signal["entry_high"],

        "execution_range_low":
            signal["execution_range_low"],

        "execution_range_high":
            signal["execution_range_high"],

        "execution_price":
            signal["execution_price"],

        "stoploss":
            signal["stoploss"],

        "tp_levels":
            signal["tp_levels"],

        "execution_tp_numbers":
            signal["execution_tp_numbers"],

        "execution_tps":
            signal["execution_tps"],

        "positions":
            positions,

    })

    save_json(
        EXECUTED_FILE,
        rows
    )

    executed_signals.add(
        signal_key
    )

    print(
        "✅ Executed signal memory saved"
    )


# ============================================================
# TRADE LOG
# ============================================================

def save_trade_log(
    signal
):

    rows = load_json_list(
        TRADE_LOG_FILE
    )

    signal_key = signal[
        "signal_id"
    ]

    rows = [
        row
        for row in rows
        if not (
            isinstance(row, dict)
            and row.get("signal_id") == signal_key
        )
    ]

    rows.append({

        "timestamp":
            now_iso(),

        "signal_id":
            signal["signal_id"],

        "channel":
            signal["channel"],

        "chat_id":
            signal["chat_id"],

        "message_id":
            signal["message_id"],

        "direction":
            signal["direction"],

        "symbol":
            SYMBOL,

        "volume":
            LOT_SIZE,

        "entry_low":
            signal["entry_low"],

        "entry_high":
            signal["entry_high"],

        "execution_range_low":
            signal["execution_range_low"],

        "execution_range_high":
            signal["execution_range_high"],

        "execution_price":
            signal["execution_price"],

        "stoploss":
            signal["stoploss"],

        "tp_levels":
            signal["tp_levels"],

        "execution_tp_numbers":
            signal["execution_tp_numbers"],

        "execution_tps":
            signal["execution_tps"],

        "positions":
            signal["positions"],

    })

    save_json(
        TRADE_LOG_FILE,
        rows
    )

    print(
        "✅ Trade log saved"
    )


# ============================================================
# SAVE PENDING SIGNAL
# ============================================================

def save_pending_signals():

    save_json(
        PENDING_FILE,
        pending_signals
    )


# ============================================================
# SAVE LIFECYCLE
# ============================================================

def save_lifecycle():

    save_json(
        LIFECYCLE_FILE,
        lifecycle_signals
    )


# ============================================================
# PHASE 4 OUTPUT PARSER
# ============================================================
#
# Phase 6 consumes the already-formatted Phase 4 output.
#
# Phase 6 does NOT monitor or parse the original source
# signal channels and does NOT choose the TP mapping.
#
# ============================================================

def normalize_text(text):

    if not text:

        return ""

    text = text.replace(
        "\r",
        "\n"
    )

    text = text.replace(
        "\u00A0",
        " "
    )

    return text.strip()


def extract_label_value(
    text,
    label
):

    pattern = (
        rf"(?im)^[^\\r\\n]*?\\b{re.escape(label)}\\s*:\\s*(.+?)\\s*$"
    )

    match = re.search(
        pattern,
        text
    )

    if match:

        return match.group(
            1
        ).strip()

    return None


def parse_price_pair(
    value
):

    if not value:

        return (
            None,
            None
        )

    # --------------------------------------------------------
    # RANGE ENTRY
    # --------------------------------------------------------
    # Example: Entry: 4778.00 - 4780.00
    # Preserve an explicitly supplied range.
    # --------------------------------------------------------

    match = re.search(
        r"(\d+(?:\.\d+)?)\s*[-–—/]\s*(\d+(?:\.\d+)?)",
        value
    )

    if match:

        first = float(
            match.group(1)
        )

        second = float(
            match.group(2)
        )

        return (
            min(first, second),
            max(first, second)
        )

    # --------------------------------------------------------
    # SINGLE ENTRY PRICE
    # --------------------------------------------------------
    # Example: Entry: 4780.00
    # Treat it as a zero-width range so the existing
    # calculate_extended_range() applies the configured 30-pip
    # extension:
    #     SELL 4780 -> 4777 - 4780
    #     BUY  4780 -> 4780 - 4783
    # --------------------------------------------------------

    single_match = re.search(
        r"\b(\d+(?:\.\d+)?)\b",
        value
    )

    if single_match:

        price = float(
            single_match.group(1)
        )

        return (
            price,
            price
        )

    return (
        None,
        None
    )

def parse_phase4_datetime(value):

    if not value:

        return None

    parsed = parse_datetime(
        value
    )

    if parsed:

        return parsed

    cleaned = re.sub(
        r"\s*(CAT|UTC)\s*$",
        "",
        str(value).strip(),
        flags=re.IGNORECASE
    )

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ):

        try:

            parsed = datetime.strptime(
                cleaned,
                fmt
            )

            return parsed.replace(
                tzinfo=CAT
            )

        except Exception:
            continue

    return None


def extract_execution_targets(
    text
):

    execution_tps = {}
    execution_numbers = []

    pattern = (
        r"(?im)"
        r"Position\s*([12])"
        r"\s*(?:→|->|=>|:|-)\s*"
        r"TP\s*([1-9]|1[0-9])"
        r"\s*(?:\(|:|-)?\s*"
        r"(\d+(?:\.\d+)?)"
    )

    matches = re.findall(
        pattern,
        text
    )

    for position_number, tp_number, price in matches:

        number = int(
            tp_number
        )

        execution_tps[number] = float(
            price
        )

        execution_numbers.append(
            number
        )

    if len(execution_numbers) < POSITIONS_PER_SIGNAL:

        pattern = (
            r"(?im)"
            r"Position\s*([12])"
            r".{0,20}?"
            r"TP\s*([1-9]|1[0-9])"
            r".{0,20}?"
            r"(\d+(?:\.\d+)?)"
        )

        matches = re.findall(
            pattern,
            text
        )

        for position_number, tp_number, price in matches:

            number = int(
                tp_number
            )

            if number not in execution_tps:

                execution_tps[number] = float(
                    price
                )

                execution_numbers.append(
                    number
                )

    ordered = []

    for number in execution_numbers:

        if number not in ordered:

            ordered.append(
                number
            )

    return (
        ordered,
        execution_tps
    )


def extract_all_tps(
    text
):

    result = {}

    pattern = (
        r"(?im)^[^\\r\\n]*?\\bTP\\s*"
        r"([1-9]|1[0-9])"
        r"\\s*[:.= -]\\s*"
        r"(\\d+(?:\\.\\d+)?)\\s*$"
    )

    matches = re.findall(
        pattern,
        text
    )

    for number, price in matches:

        result[int(number)] = float(
            price
        )

    return dict(
        sorted(
            result.items()
        )
    )


def read_phase4_execution_message(message, chat_id, message_id):
    """
    Read the already-validated Phase 4 execution message.

    IMPORTANT:
    Phase 6 does NOT run the original signal parser.
    Phase 4 has already parsed/validated the source signal and selected
    the execution targets. Phase 6 only reads those execution fields,
    watches the market price and executes the supplied targets.
    """

    text = str(message or "")

    if "SIGNAL PARSED" not in text.upper():
        return None

    def label(label_name):
        match = re.search(
            rf"(?im)^.*?{re.escape(label_name)}\s*:\s*(.+?)\s*$",
            text
        )
        return match.group(1).strip() if match else None

    signal_id = label("Signal ID")
    direction = label("Direction")
    entry_value = label("Entry")
    sl_value = label("SL")
    signal_time_value = label("Signal Time")
    valid_until_value = label("Valid Until")
    channel = label("Channel") or "Unknown"

    if not signal_id or not direction or not entry_value or not sl_value:
        return None

    direction = direction.upper().strip()
    if direction not in ("BUY", "SELL"):
        return None

    # Entry range supplied by Phase 4.
    entry_match = re.search(
        r"(\d+(?:\.\d+)?)\s*[-–—/]\s*(\d+(?:\.\d+)?)",
        entry_value
    )
    if entry_match:
        entry_low = min(float(entry_match.group(1)), float(entry_match.group(2)))
        entry_high = max(float(entry_match.group(1)), float(entry_match.group(2)))
    else:
        single = re.search(r"\d+(?:\.\d+)?", entry_value)
        if not single:
            return None
        entry_low = entry_high = float(single.group())

    sl_match = re.search(r"\d+(?:\.\d+)?", sl_value)
    if not sl_match:
        return None
    stoploss = float(sl_match.group())

    signal_time = parse_phase4_datetime(signal_time_value)
    valid_until = parse_phase4_datetime(valid_until_value)
    if signal_time is None or valid_until is None:
        return None

    # Read the execution targets selected by Phase 4.
    execution_numbers = []
    execution_tps = {}

    target_pattern = re.compile(
        r"(?im)^\s*Position\s*([1-9]|1[0-9])\s*(?:→|->|=>|:|-)\s*"
        r"TP\s*([1-9]|1[0-9])\s*\(\s*(\d+(?:\.\d+)?)\s*\)\s*$"
    )

    for match in target_pattern.finditer(text):
        position_number = int(match.group(1))
        tp_number = int(match.group(2))
        tp_price = float(match.group(3))
        execution_numbers.append(tp_number)
        execution_tps[tp_number] = tp_price

    # Fallback for a Phase 4 message using a simpler target separator.
    if len(execution_numbers) < POSITIONS_PER_SIGNAL:
        fallback_pattern = re.compile(
            r"(?im)^\s*Position\s*([1-9]|1[0-9]).{0,30}?"
            r"TP\s*([1-9]|1[0-9]).{0,30}?"
            r"(\d+(?:\.\d+)?)\s*$"
        )
        for match in fallback_pattern.finditer(text):
            tp_number = int(match.group(2))
            if tp_number not in execution_tps:
                execution_numbers.append(tp_number)
                execution_tps[tp_number] = float(match.group(3))

    if len(execution_numbers) < POSITIONS_PER_SIGNAL:
        return None

    # Keep the first target for each position in Phase 4 order.
    ordered_numbers = []
    for number in execution_numbers:
        if number not in ordered_numbers:
            ordered_numbers.append(number)

    return {
        "channel": channel,
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "signal_id": signal_id.strip(),
        "signal_time": signal_time.isoformat(),
        "valid_until": valid_until.isoformat(),
        "direction": direction,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "stoploss": stoploss,
        "tp_levels": dict(execution_tps),
        "execution_tp_numbers": ordered_numbers[:POSITIONS_PER_SIGNAL],
        "execution_tps": execution_tps,
        "parse_success": True,
        "validation_errors": [],
        "timestamp": now_iso(),
        "source": "PHASE_4_PRIVATE_OUTPUT",
    }


# ============================================================
# METAAPI SYMBOL SPECIFICATION
# ============================================================

async def verify_symbol():

    specification = (
        await connection
        .get_symbol_specification(
            SYMBOL
        )
    )

    digits = get_field(
        specification,
        "digits",
        "Unknown"
    )

    min_volume = get_field(
        specification,
        "minVolume",
        None
    )

    if min_volume is None:

        min_volume = get_field(
            specification,
            "min_volume",
            "Unknown"
        )

    volume_step = get_field(
        specification,
        "volumeStep",
        None
    )

    if volume_step is None:

        volume_step = get_field(
            specification,
            "volume_step",
            "Unknown"
        )

    print()
    print(
        f"✅ {SYMBOL} available"
    )

    print(
        f"   Digits: {digits}"
    )

    print(
        f"   Min volume: {min_volume}"
    )

    print(
        f"   Volume step: {volume_step}"
    )

    return specification


# ============================================================
# GET CURRENT PRICE
# ============================================================

async def get_current_price():

    price = (
        await connection
        .get_symbol_price(
            SYMBOL
        )
    )

    bid = get_field(
        price,
        "bid"
    )

    ask = get_field(
        price,
        "ask"
    )

    if bid is None or ask is None:

        raise RuntimeError(
            f"Could not obtain price: {price}"
        )

    return (
        float(bid),
        float(ask)
    )



async def refresh_metaapi_account():

    global account

    if metaapi is None or not METAAPI_ACCOUNT_ID:
        return None

    try:
        account = await (
            metaapi
            .metatrader_account_api
            .get_account(
                METAAPI_ACCOUNT_ID
            )
        )
        return account

    except Exception as error:
        print(
            f"⚠️ Could not refresh MetaApi account status: {error}"
        )
        return None


async def set_metaapi_connection_state(
    state,
    reason=None
):

    global metaapi_connection_state

    normalized = str(state or "UNKNOWN").upper()

    if normalized == metaapi_connection_state:
        return

    previous = metaapi_connection_state
    metaapi_connection_state = normalized

    if normalized in ("DISCONNECTED", "DISCONNECTED_FROM_BROKER", "LOST"):
        print()
        print("🔴 METAAPI CONNECTION LOST")
        print(f"   Status: {normalized}")
        if reason:
            print(f"   Reason: {reason}")

        await send_notification(
            "🔴 METAAPI CONNECTION LOST\n\n"
            f"Status: {normalized}\n"
            + (f"Reason: {reason}\n" if reason else "")
            + "Phase 6 is keeping valid pending signals alive and will retry automatically."
        )

    elif normalized == "CONNECTED":
        print()
        print("🟢 METAAPI CONNECTION RESTORED")
        print("   Status: CONNECTED")

        await send_notification(
            "🟢 METAAPI CONNECTION RESTORED\n\n"
            "Status: CONNECTED\n"
            "Phase 6 can resume pending execution attempts."
        )


async def ensure_metaapi_connection(
    valid_until=None
):

    global connection
    global metaapi_connection_lock

    if metaapi_connection_lock is None:
        metaapi_connection_lock = asyncio.Lock()

    async with metaapi_connection_lock:

        attempt = 0
        delay_index = 0

        while True:

            if valid_until:
                expiry = parse_datetime(valid_until)
                if expiry and now_utc() >= expiry.astimezone(timezone.utc):
                    return False

            fresh_account = await refresh_metaapi_account()

            if fresh_account is not None:
                status = str(
                    get_field(
                        fresh_account,
                        "connectionStatus",
                        get_field(fresh_account, "connection_status", "UNKNOWN")
                    )
                ).upper()

                if status in ("DISCONNECTED", "DISCONNECTED_FROM_BROKER"):
                    await set_metaapi_connection_state(
                        status,
                        "MetaApi account reports broker/application disconnection."
                    )

            try:

                if connection is None:
                    connection = account.get_rpc_connection()
                    await connection.connect()
                    await connection.wait_synchronized()
                else:
                    # A fresh connect is used after a broken RPC socket.
                    await connection.get_account_information()

                # Confirm the account is usable through RPC, not merely deployed.
                await connection.get_account_information()

                refreshed = await refresh_metaapi_account()
                if refreshed is not None:
                    status = str(
                        get_field(
                            refreshed,
                            "connectionStatus",
                            get_field(refreshed, "connection_status", "UNKNOWN")
                        )
                    ).upper()

                    if status not in ("CONNECTED", "UNKNOWN"):
                        raise RuntimeError(
                            f"MetaApi account status is {status}"
                        )

                await set_metaapi_connection_state("CONNECTED")
                return True

            except Exception as error:

                attempt += 1
                print()
                print(
                    f"⚠️ MetaApi connection check/reconnect failed "
                    f"(attempt {attempt}): {error}"
                )

                await set_metaapi_connection_state(
                    "LOST",
                    str(error)
                )

                # Close the broken RPC socket and create a fresh one.
                try:
                    if connection is not None:
                        await connection.close()
                except Exception:
                    pass

                connection = None

                refreshed = await refresh_metaapi_account()
                if refreshed is not None:
                    status = str(
                        get_field(
                            refreshed,
                            "connectionStatus",
                            get_field(refreshed, "connection_status", "UNKNOWN")
                        )
                    ).upper()
                    print(
                        f"   MetaApi account connectionStatus: {status}"
                    )

                delay = CONNECTION_RETRY_DELAYS_SECONDS[
                    min(
                        delay_index,
                        len(CONNECTION_RETRY_DELAYS_SECONDS) - 1
                    )
                ]
                delay_index += 1

                if valid_until:
                    expiry = parse_datetime(valid_until)
                    if expiry:
                        remaining = (
                            expiry.astimezone(timezone.utc)
                            - now_utc()
                        ).total_seconds()
                        if remaining <= 0:
                            return False
                        delay = min(delay, max(1, int(remaining)))

                print(
                    f"🔄 MetaApi retrying in {delay} seconds..."
                )

                await asyncio.sleep(delay)


async def metaapi_connection_monitor():

    while True:

        try:
            refreshed = await refresh_metaapi_account()
            if refreshed is not None:
                status = str(
                    get_field(
                        refreshed,
                        "connectionStatus",
                        get_field(refreshed, "connection_status", "UNKNOWN")
                    )
                ).upper()

                if status in ("DISCONNECTED", "DISCONNECTED_FROM_BROKER"):
                    await set_metaapi_connection_state(
                        status,
                        "MetaApi account status monitor detected the disconnection."
                    )
                elif status == "CONNECTED":
                    # The provisioning status says CONNECTED, but also test the
                    # actual RPC socket so a dead websocket is not reported as healthy.
                    try:
                        if connection is None:
                            raise RuntimeError("RPC connection is not available")
                        await connection.get_account_information()
                        await set_metaapi_connection_state("CONNECTED")
                    except Exception as error:
                        await set_metaapi_connection_state(
                            "LOST",
                            f"RPC connection test failed: {error}"
                        )

        except Exception as error:
            print(
                f"⚠️ MetaApi connection monitor error: {error}"
            )

        await asyncio.sleep(
            CONNECTION_STATUS_CHECK_SECONDS
        )


# ============================================================
# CALCULATE EXTENDED RANGE
# ============================================================

def calculate_extended_range(
    direction,
    entry_low,
    entry_high
):

    if direction == "SELL":

        return (
            entry_low
            - ENTRY_EXTENSION_PRICE,
            entry_high
        )

    return (
        entry_low,
        entry_high
        + ENTRY_EXTENSION_PRICE
    )


# ============================================================
# CHECK SIGNAL EXPIRY
# ============================================================

def signal_is_expired(
    signal
):

    valid_until = parse_datetime(
        signal.get(
            "valid_until"
        )
    )

    if not valid_until:

        return True

    return (
        now_utc()
        >= valid_until.astimezone(
            timezone.utc
        )
    )


# ============================================================
# TELEGRAM NOTIFICATION
# ============================================================

async def send_notification(
    message
):

    # Phase 4 is READ-ONLY input for Phase 6.
    # Diagnostics/trade notifications go to the separate private
    # Phase 6 status channel.
    global telegram_client

    target_chat = str(PHASE6_STATUS_CHAT_ID).strip()

    if telegram_client is not None and target_chat:
        try:
            await telegram_client.send_message(
                int(target_chat),
                message
            )
            return True
        except Exception as error:
            print(
                "⚠️ Phase 6 private Telegram notification failed: "
                f"{type(error).__name__}: {error}"
            )

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_NOTIFICATION_CHAT_ID:
        return False

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/"
        "sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_NOTIFICATION_CHAT_ID,
        "text": message,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                timeout=15
            ) as response:
                if response.status != 200:
                    body = await response.text()
                    print("⚠️ Telegram notification failed:")
                    print(body)
                    return False
                return True
    except Exception as error:
        print(
            "⚠️ Telegram notification failed: "
            f"{type(error).__name__}: {error}"
        )
        return False


# ============================================================
# SIGNAL DETECTED NOTIFICATION
# ============================================================

async def notify_signal_detected(
    signal
):

    execution_text = []

    for number in signal[
        "execution_tp_numbers"
    ]:

        execution_text.append(
            f"TP{number}: "
            f"{signal['execution_tps'][number]}"
        )

    message = (
        "🟢 SIGNAL DETECTED\n"
        "\n"
        f"Channel: {signal['channel']}\n"
        f"Signal ID: {signal['signal_id']}\n"
        f"Direction: {signal['direction']}\n"
        f"Entry: {signal['entry_low']}-"
        f"{signal['entry_high']}\n"
        f"SL: {signal['stoploss']}\n"
        + "\n".join(execution_text)
        + "\n"
        f"Valid Until: "
        f"{format_cat_time(signal['valid_until'])}"
    )

    await send_notification(
        message
    )


# ============================================================
# WAITING NOTIFICATION
# ============================================================

async def notify_signal_waiting(
    signal,
    bid,
    ask
):

    extended_low = signal[
        "execution_range_low"
    ]

    extended_high = signal[
        "execution_range_high"
    ]

    message = (
        "🟡 SIGNAL WAITING FOR ENTRY\n"
        "\n"
        f"Channel: {signal['channel']}\n"
        f"Signal ID: {signal['signal_id']}\n"
        f"Direction: {signal['direction']}\n"
        f"Original Entry: "
        f"{signal['entry_low']}-"
        f"{signal['entry_high']}\n"
        f"30-Pip Range: "
        f"{extended_low}-"
        f"{extended_high}\n"
        f"Current Bid: {bid:.3f}\n"
        f"Current Ask: {ask:.3f}\n"
        f"Valid Until: "
        f"{format_cat_time(signal['valid_until'])}\n"
        "\n"
        "Price is outside the permitted range. "
        "Phase 6 will keep waiting until the signal expires."
    )

    await send_notification(
        message
    )


# ============================================================
# EXECUTED NOTIFICATION
# ============================================================

async def notify_signal_executed(
    signal
):

    positions = signal.get(
        "positions",
        []
    )

    lines = []

    for position in positions:

        lines.append(
            f"Position {position['position_number']}: "
            f"TP{position['tp_number']} "
            f"({position['tp']})\n"
            f"Trade ID: "
            f"{position.get('position_id') or 'Unknown'}"
        )

    message = (
        "✅ DEMO SIGNAL EXECUTED\n"
        "\n"
        f"Channel: {signal['channel']}\n"
        f"Signal ID: {signal['signal_id']}\n"
        f"Direction: {signal['direction']}\n"
        f"Symbol: {SYMBOL}\n"
        f"Execution Time: "
        f"{format_cat_time(signal['execution_time'])}\n"
        f"Execution Price: "
        f"{signal['execution_price']}\n"
        f"Entry Range: "
        f"{signal['execution_range_low']}-"
        f"{signal['execution_range_high']}\n"
        f"SL: {signal['stoploss']}\n"
        "\n"
        + "\n\n".join(lines)
    )

    await send_notification(
        message
    )


# ============================================================
# EXTRACT ACTUAL CLOSE INFORMATION
# ============================================================

async def get_position_close_information(
    position_id
):

    deals = []

    try:

        deals = (
            await connection
            .get_history_deals_by_position(
                position_id
            )
        )

    except Exception as error:

        print(
            f"⚠️ Could not get deal history for "
            f"{position_id}: {error}"
        )

    if not deals:

        return None

    close_deals = []

    for deal in deals:

        entry_type = get_field(
            deal,
            "entryType",
            None
        )

        if entry_type is None:

            entry_type = get_field(
                deal,
                "entry_type",
                None
            )

        entry_text = str(
            entry_type or ""
        ).upper()

        if (
            "OUT" in entry_text
            or "CLOSE" in entry_text
        ):

            close_deals.append(
                deal
            )

    if not close_deals:

        close_deals = [
            deals[-1]
        ]

    profit = 0.0

    for deal in close_deals:

        deal_profit = get_field(
            deal,
            "profit",
            0
        )

        try:

            profit += float(
                deal_profit or 0
            )

        except Exception:

            pass

    reasons = []

    for deal in close_deals:

        reason = get_field(
            deal,
            "reason",
            None
        )

        if reason:

            reasons.append(
                str(reason)
            )

    reason_text = "UNKNOWN"

    combined_reason = " ".join(
        reasons
    ).upper()

    if (
        "SL" in combined_reason
        or "STOP_LOSS" in combined_reason
    ):

        reason_text = "SL"

    elif (
        "TP" in combined_reason
        or "TAKE_PROFIT" in combined_reason
    ):

        reason_text = "TP"

    elif profit > 0:

        reason_text = "TP"

    elif profit < 0:

        reason_text = "SL"

    close_time = get_field(
        close_deals[-1],
        "time",
        None
    )

    if close_time is None:

        close_time = now_iso()

    return {

        "close_reason":
            reason_text,

        "profit":
            profit,

        "close_time":
            str(close_time),

        "close_deals":
            len(close_deals),

    }


# ============================================================
# POSITION CLOSURE NOTIFICATION
# ============================================================

async def notify_position_closed(
    signal,
    position
):

    reason = position.get(
        "close_reason",
        "UNKNOWN"
    )

    profit = float(
        position.get(
            "profit",
            0
        ) or 0
    )

    result_word = (
        "PROFIT"
        if profit >= 0
        else "LOSS"
    )

    message = (
        "📌 POSITION CLOSED\n"
        "\n"
        f"Channel: {signal['channel']}\n"
        f"Signal ID: {signal['signal_id']}\n"
        f"Position: "
        f"{position['position_number']}\n"
        f"Trade ID: "
        f"{position.get('position_id') or 'Unknown'}\n"
        f"Closure: {reason}\n"
        f"Actual P/L: "
        f"{profit:.2f}\n"
        f"Result: {result_word}\n"
        f"Close Time: "
        f"{format_cat_time(position.get('close_time'))}"
    )

    await send_notification(
        message
    )


# ============================================================
# EXECUTE TWO POSITIONS
# ============================================================

async def find_existing_signal_position(
    signal_id,
    position_number,
    client_id
):

    if not await ensure_metaapi_connection():
        return None

    positions = await connection.get_positions()

    for position in positions or []:

        symbol = str(
            get_field(position, "symbol", "")
        )

        if symbol != SYMBOL:
            continue

        comment = str(
            get_field(position, "comment", "")
        )
        original_comment = str(
            get_field(position, "originalComment", "")
        )
        position_client_id = str(
            get_field(position, "clientId", "")
        )

        _, marker = build_trade_identifiers(
            signal_id,
            position_number
        )

        if (
            client_id in position_client_id
            or marker in comment
            or marker in original_comment
        ):
            return position

    return None


def build_trade_identifiers(signal_id, position_number):
    """Build short MetaApi identifiers that stay within broker/API limits.

    MetaApi validates the comment/clientId fields before sending the order.
    The previous identifiers embedded the full Phase 4 signal ID and became
    too long.  Phase 4 signal IDs currently end with a compact numeric
    sequence (for example SIG-1003170522699-207), so the final sequence is
    sufficient for the demo pipeline and keeps both fields very short.
    """
    raw_signal_id = str(signal_id or "UNKNOWN")
    parts = raw_signal_id.split("-")
    compact_signal_id = parts[-1] if parts and parts[-1] else raw_signal_id

    # Keep only safe characters and cap the compact portion so the resulting
    # MetaApi identifiers remain comfortably below the documented limits.
    compact_signal_id = re.sub(r"[^A-Za-z0-9]", "", compact_signal_id) or "0"
    compact_signal_id = compact_signal_id[-12:]

    client_id = f"P6_{compact_signal_id}_P{position_number}"
    marker = f"P6:{compact_signal_id}:P{position_number}"

    return client_id, marker


def build_position_record(
    position_number,
    tp_number,
    tp_price,
    position,
    result_code="TRADE_RETCODE_DONE"
):

    position_id = get_field(
        position,
        "id",
        None
    )

    if not position_id:
        position_id = get_field(
            position,
            "positionId",
            None
        )

    return {
        "position_number": position_number,
        "tp_number": tp_number,
        "tp": tp_price,
        "result_code": result_code,
        "position_id": position_id,
        "opened_at": now_iso(),
        "close_reason": None,
        "close_time": None,
        "profit": None,
        "closed": False,
        "result": str(position),
    }


async def execute_signal(
    signal,
    execution_price
):

    direction = signal["direction"]
    positions = []

    selected_tps = signal[
        "execution_tp_numbers"
    ][:POSITIONS_PER_SIGNAL]

    if len(selected_tps) < POSITIONS_PER_SIGNAL:
        raise RuntimeError(
            "Phase 4 did not provide enough execution TPs."
        )

    # Preserve positions already opened by an earlier successful request.
    existing_records = {
        p["position_number"]: p
        for p in signal.get("positions", [])
        if p.get("position_id") and not p.get("closed", False)
    }

    for position_number, tp_number in enumerate(
        selected_tps,
        start=1
    ):

        tp_price = signal["execution_tps"][tp_number]
        client_id, marker = build_trade_identifiers(
            signal["signal_id"],
            position_number
        )

        print()
        print("-" * 70)
        print(f"🚀 EXECUTING POSITION {position_number}")
        print(f"Direction: {direction}")
        print(f"Symbol:    {SYMBOL}")
        print(f"Volume:    {LOT_SIZE}")
        print(f"SL:        {signal['stoploss']}")
        print(f"TP{tp_number}:      {tp_price}")

        # Duplicate protection: inspect MT5 before every new order.
        try:
            existing = await find_existing_signal_position(
                signal["signal_id"],
                position_number,
                client_id
            )

            if existing is not None:
                print(
                    f"🛡️ Existing position found for "
                    f"{signal['signal_id']} / Position {position_number}. "
                    "No duplicate order will be sent."
                )
                record = build_position_record(
                    position_number,
                    tp_number,
                    tp_price,
                    existing
                )
                positions.append(record)

                await send_notification(
                    "🛡️ EXISTING POSITION FOUND - NO DUPLICATE ORDER\n\n"
                    f"Signal ID: {signal['signal_id']}\n"
                    f"Position: {position_number}/{POSITIONS_PER_SIGNAL}\n"
                    f"TP{tp_number}: {tp_price}\n"
                    f"Existing position: {existing}"
                )

                continue

        except Exception as error:
            print(
                f"⚠️ Could not perform duplicate check: {error}"
            )
            return False, positions

        try:
            connected = await ensure_metaapi_connection(
                signal.get("valid_until")
            )
            if not connected:
                return False, positions

            options = {
                "comment": marker,
                "clientId": client_id,
            }

            # ----------------------------------------------------
            # DEEP METAAPI ORDER DIAGNOSTICS
            # ----------------------------------------------------
            # Do not silently wait inside the SDK. We print every
            # input, verify the live connection and price, then put
            # a hard timeout around the actual order request.
            # This does NOT change the trading decision or order
            # parameters; it only makes the MetaApi boundary visible.
            # ----------------------------------------------------

            print()
            print("🔬 DEEP METAAPI EXECUTION DIAGNOSTICS")
            print("   --------------------------------------------------")
            print(f"   Position number: {position_number}")
            print(f"   Direction:       {direction}")
            print(f"   Symbol:          {SYMBOL}")
            print(f"   Volume:          {LOT_SIZE}")
            print(f"   Stop loss:       {signal['stoploss']}")
            print(f"   Take profit:     {tp_price}")
            print(f"   Comment:         {marker}")
            print(f"   Client ID:       {client_id}")
            print(f"   Comment length:  {len(marker)}")
            print(f"   Client ID length: {len(client_id)}")
            print(f"   Combined length: {len(marker) + len(client_id)}")
            print(f"   Connection obj:  {type(connection).__name__}")
            print(f"   Connection state: {metaapi_connection_state}")

            await send_notification(
                "🔬 METAAPI ORDER DIAGNOSTICS\n\n"
                f"Signal ID: {signal['signal_id']}\n"
                f"Position: {position_number}/{POSITIONS_PER_SIGNAL}\n"
                f"Direction: {direction}\n"
                f"Symbol: {SYMBOL}\n"
                f"Volume: {LOT_SIZE}\n"
                f"SL: {signal['stoploss']}\n"
                f"TP{tp_number}: {tp_price}\n"
                f"Execution price: {execution_price:.3f}\n"
                f"Comment: {marker}\n"
                f"Client ID: {client_id}\n"
                f"Connection state: {metaapi_connection_state}\n"
                "Checking account and live quote before order."
            )

            # Verify the account state immediately before sending.
            try:
                print("   🔎 Checking MetaApi account information...")
                account_info = await asyncio.wait_for(
                    connection.get_account_information(),
                    timeout=10
                )
                print("   ✅ Account information returned")
                print(f"      Broker: {get_field(account_info, 'broker', 'Unknown')}")
                print(f"      Server: {get_field(account_info, 'server', 'Unknown')}")
                print(f"      State: {get_field(account_info, 'state', 'Unknown')}")
                print(f"      Connection status: {get_field(account_info, 'connectionStatus', get_field(account_info, 'connection_status', 'Unknown'))}")
            except Exception as account_error:
                print("   ⚠️ Account information diagnostic failed:")
                print(f"      {type(account_error).__name__}: {account_error}")
                print("      Continuing to the order request because the existing connection check passed.")

            # Verify the live quote immediately before sending.
            try:
                print("   🔎 Checking live XAUUSD_i price...")
                live_price = await asyncio.wait_for(
                    connection.get_symbol_price(SYMBOL),
                    timeout=10
                )
                print("   ✅ Live price returned")
                print(f"      Bid: {get_field(live_price, 'bid', 'Unknown')}")
                print(f"      Ask: {get_field(live_price, 'ask', 'Unknown')}")
                print(f"      Time: {get_field(live_price, 'time', get_field(live_price, 'brokerTime', 'Unknown'))}")
            except Exception as price_error:
                print("   ⚠️ Live price diagnostic failed:")
                print(f"      {type(price_error).__name__}: {price_error}")

            print()
            print("🚨 METAAPI ORDER REQUEST ABOUT TO BE SENT")
            print("   ==================================================")
            print("   This is the exact point where the previous run stopped.")
            print("   Waiting up to 30 seconds for MetaApi response...")
            print("   ==================================================")

            await send_notification(
                "📤 METAAPI ORDER REQUEST SENT\n\n"
                f"Signal ID: {signal['signal_id']}\n"
                f"Position: {position_number}/{POSITIONS_PER_SIGNAL}\n"
                f"Direction: {direction}\n"
                f"Symbol: {SYMBOL}\n"
                f"Volume: {LOT_SIZE}\n"
                f"SL: {signal['stoploss']}\n"
                f"TP{tp_number}: {tp_price}\n"
                f"Execution price: {execution_price:.3f}\n"
                "Waiting up to 30 seconds for MetaApi response."
            )

            order_started = datetime.now(timezone.utc)

            try:
                if direction == "BUY":
                    result = await asyncio.wait_for(
                        connection.create_market_buy_order(
                            SYMBOL,
                            LOT_SIZE,
                            signal["stoploss"],
                            tp_price,
                            options
                        ),
                        timeout=30
                    )
                else:
                    result = await asyncio.wait_for(
                        connection.create_market_sell_order(
                            SYMBOL,
                            LOT_SIZE,
                            signal["stoploss"],
                            tp_price,
                            options
                        ),
                        timeout=30
                    )

                elapsed = (datetime.now(timezone.utc) - order_started).total_seconds()
                print()
                print("📨 METAAPI ORDER RESPONSE RECEIVED")
                print(f"   Response time: {elapsed:.3f} seconds")
                print(f"   Response type: {type(result).__name__}")
                print(f"   Raw response:  {result!r}")

                await send_notification(
                    "📨 METAAPI ORDER RESPONSE RECEIVED\n\n"
                    f"Signal ID: {signal['signal_id']}\n"
                    f"Position: {position_number}/{POSITIONS_PER_SIGNAL}\n"
                    f"Response time: {elapsed:.3f}s\n"
                    f"Result code: {get_field(result, 'stringCode', 'UNKNOWN')}\n"
                    f"Position ID: {get_field(result, 'positionId', get_field(result, 'position_id', 'UNKNOWN'))}\n"
                    f"Raw result: {result!r}"
                )

            except asyncio.TimeoutError:
                elapsed = (datetime.now(timezone.utc) - order_started).total_seconds()
                print()
                print("⏰ METAAPI ORDER REQUEST TIMED OUT")
                print(f"   Waited: {elapsed:.3f} seconds")
                print("   IMPORTANT: the broker may still have received the order.")
                print("   Checking MT5 for the position before allowing any retry.")

                await send_notification(
                    "⏰ METAAPI ORDER REQUEST TIMED OUT\n\n"
                    f"Signal ID: {signal['signal_id']}\n"
                    f"Position: {position_number}/{POSITIONS_PER_SIGNAL}\n"
                    f"Waited: {elapsed:.3f}s\n"
                    "Phase 6 will verify MT5 before retrying."
                )

                try:
                    await asyncio.sleep(1)
                    confirmed = await asyncio.wait_for(
                        find_existing_signal_position(
                            signal["signal_id"],
                            position_number,
                            client_id
                        ),
                        timeout=15
                    )
                    if confirmed is not None:
                        print("🛡️ POSITION FOUND AFTER METAAPI TIMEOUT")
                        print(f"   Position: {confirmed}")
                        positions.append(
                            build_position_record(
                                position_number,
                                tp_number,
                                tp_price,
                                confirmed,
                                "METAAPI_TIMEOUT_BUT_POSITION_CONFIRMED"
                            )
                        )
                        continue
                    print("❌ No matching MT5 position found after timeout.")
                except Exception as verify_error:
                    print("❌ Post-timeout verification also failed:")
                    print(f"   {type(verify_error).__name__}: {verify_error}")

                return False, positions

            except Exception as order_error:
                elapsed = (datetime.now(timezone.utc) - order_started).total_seconds()
                print()
                print("❌ METAAPI ORDER REQUEST RAISED AN EXCEPTION")
                print(f"   Elapsed: {elapsed:.3f} seconds")
                print(f"   Exception type: {type(order_error).__name__}")
                print(f"   Exception: {order_error}")

                await send_notification(
                    "❌ METAAPI ORDER REQUEST FAILED\n\n"
                    f"Signal ID: {signal['signal_id']}\n"
                    f"Position: {position_number}/{POSITIONS_PER_SIGNAL}\n"
                    f"Exception type: {type(order_error).__name__}\n"
                    f"Exception: {order_error}\n"
                    f"Details: {getattr(order_error, 'details', None)!r}"
                )

                # MetaApi ValidationException instances can contain the
                # broker/API reason in `details`. Print it explicitly so a
                # future rejection is diagnosable without guessing.
                error_details = getattr(order_error, "details", None)
                if error_details is not None:
                    print(f"   Error details: {error_details!r}")

                # The SDK may expose a richer formatter on the MetaApi
                # client. Use it when available, but never let diagnostics
                # hide the original exception.
                try:
                    formatter = getattr(api, "format_error", None)
                    if callable(formatter):
                        print("   Formatted MetaApi error:")
                        print(f"      {formatter(order_error)}")
                except Exception as format_error:
                    print(
                        "   ⚠️ Could not format MetaApi error: "
                        f"{type(format_error).__name__}: {format_error}"
                    )

                print("   Full traceback follows:")
                import traceback
                traceback.print_exc()

                print("🔎 Checking MT5 for a position despite the exception...")
                try:
                    await asyncio.sleep(1)
                    confirmed = await asyncio.wait_for(
                        find_existing_signal_position(
                            signal["signal_id"],
                            position_number,
                            client_id
                        ),
                        timeout=15
                    )
                    if confirmed is not None:
                        print("🛡️ POSITION FOUND DESPITE METAAPI EXCEPTION")
                        print(f"   Position: {confirmed}")
                        positions.append(
                            build_position_record(
                                position_number,
                                tp_number,
                                tp_price,
                                confirmed,
                                "METAAPI_EXCEPTION_BUT_POSITION_CONFIRMED"
                            )
                        )
                        continue
                    print("❌ No matching MT5 position found.")
                except Exception as verify_error:
                    print("❌ Verification after MetaApi exception failed:")
                    print(f"   {type(verify_error).__name__}: {verify_error}")

                return False, positions

            print()
            print("📨 MetaApi result:")
            print(result)

            result_code = get_field(
                result,
                "stringCode",
                ""
            )

            position_id = get_field(
                result,
                "positionId",
                None
            ) or get_field(
                result,
                "position_id",
                None
            )

            if result_code not in (
                "TRADE_RETCODE_DONE",
                "TRADE_RETCODE_PLACED",
                ""
            ):
                print(
                    f"❌ POSITION {position_number} FAILED - "
                    f"MetaApi code: {result_code}"
                )

                await send_notification(
                    "❌ POSITION REJECTED BY METAAPI\n\n"
                    f"Signal ID: {signal['signal_id']}\n"
                    f"Position: {position_number}/{POSITIONS_PER_SIGNAL}\n"
                    f"MetaApi result code: {result_code}\n"
                    f"Raw result: {result!r}"
                )

                return False, positions

            # Confirm the position really exists before considering the
            # order successful. This also protects against a lost response.
            await asyncio.sleep(1)
            confirmed = await find_existing_signal_position(
                signal["signal_id"],
                position_number,
                client_id
            )

            if confirmed is None:
                print(
                    f"⚠️ POSITION {position_number} order response was received, "
                    "but MT5 position verification failed."
                )

                await send_notification(
                    "⚠️ ORDER RESPONSE RECEIVED BUT MT5 VERIFICATION FAILED\n\n"
                    f"Signal ID: {signal['signal_id']}\n"
                    f"Position: {position_number}/{POSITIONS_PER_SIGNAL}\n"
                    "Phase 6 will not blindly duplicate the order."
                )

                return False, positions

            if not position_id:
                position_id = get_field(
                    confirmed,
                    "id",
                    None
                ) or get_field(
                    confirmed,
                    "positionId",
                    None
                )

            print(
                f"✅ POSITION {position_number} EXECUTED AND VERIFIED"
            )
            print(
                f"   Position ID: {position_id or 'Unknown'}"
            )

            await send_notification(
                "✅ POSITION EXECUTED AND VERIFIED\n\n"
                f"Signal ID: {signal['signal_id']}\n"
                f"Position: {position_number}/{POSITIONS_PER_SIGNAL}\n"
                f"TP{tp_number}: {tp_price}\n"
                f"Position ID: {position_id or 'Unknown'}\n"
                f"MetaApi result code: {result_code}"
            )

            positions.append({
                "position_number": position_number,
                "tp_number": tp_number,
                "tp": tp_price,
                "result_code": result_code,
                "position_id": position_id,
                "opened_at": now_iso(),
                "close_reason": None,
                "close_time": None,
                "profit": None,
                "closed": False,
                "result": str(result),
            })

        except Exception as error:

            print()
            print(
                f"❌ POSITION {position_number} EXECUTION ERROR:"
            )
            print(error)

            # A timeout can mean MT5 accepted the order but the response was
            # lost. Check positions before allowing a future retry.
            try:
                await asyncio.sleep(1)
                confirmed = await find_existing_signal_position(
                    signal["signal_id"],
                    position_number,
                    client_id
                )
                if confirmed is not None:
                    print(
                        f"🛡️ POSITION {position_number} was found in MT5 "
                        "after the request error. Treating it as executed."
                    )
                    positions.append(
                        build_position_record(
                            position_number,
                            tp_number,
                            tp_price,
                            confirmed
                        )
                    )
                    continue
            except Exception as verify_error:
                print(
                    f"⚠️ Post-error position verification failed: "
                    f"{verify_error}"
                )

            return False, positions

    return (
        len(positions) == POSITIONS_PER_SIGNAL,
        positions
    )



# ============================================================
# RECORD SIGNAL EXECUTION
# ============================================================

def mark_signal_executed(
    signal,
    positions,
    execution_price
):

    signal["execution_price"] = (
        execution_price
    )

    signal["execution_time"] = (
        now_iso()
    )

    signal["positions"] = (
        positions
    )

    signal["status"] = (
        "EXECUTED"
    )

    save_executed_signal(
        signal,
        positions
    )

    save_trade_log(
        signal
    )

    lifecycle_signals[
        signal["signal_id"]
    ] = signal

    save_lifecycle()

    pending_signals.pop(
        signal["signal_id"],
        None
    )

    save_pending_signals()


# ============================================================
# CHECK PRICE AND EXECUTE WHEN RANGE IS REACHED
# ============================================================

async def wait_for_entry(
    signal
):

    signal_id = signal["signal_id"]

    try:

        while True:

            if signal_id in executed_signals:
                return

            if signal_is_expired(signal):
                signal["status"] = "EXPIRED"
                signal["expired_at"] = now_iso()

                lifecycle_signals[signal_id] = signal
                save_lifecycle()
                pending_signals.pop(signal_id, None)
                save_pending_signals()

                await send_notification(
                    "⌛ SIGNAL EXPIRED - TRADE NOT EXECUTED\n\n"
                    f"Channel: {signal['channel']}\n"
                    f"Signal ID: {signal_id}\n"
                    "The signal validity period ended before all required positions were executed.\n"
                    "No further trade attempts will be made."
                )

                print(f"⌛ Signal expired: {signal_id}")
                return

            # Once price has entered the permitted range, latch the signal into
            # execution-pending state. A temporary connection outage must not
            # make us forget that the entry condition was already triggered.
            if signal.get("execution_triggered", False):

                signal["status"] = "EXECUTION_PENDING"
                lifecycle_signals[signal_id] = signal
                pending_signals[signal_id] = signal
                save_lifecycle()
                save_pending_signals()

                connected = await ensure_metaapi_connection(
                    signal.get("valid_until")
                )

                if not connected:
                    print()
                    print("🔴 EXECUTION BLOCKED: METAPI NOT CONNECTED")
                    print(f"   Signal ID: {signal_id}")
                    print("   Decision: keep signal pending and retry.")
                    if signal_is_expired(signal):
                        continue
                    await asyncio.sleep(
                        PRICE_CHECK_INTERVAL_SECONDS
                    )
                    continue

                try:
                    bid, ask = await get_current_price()
                    execution_price = (
                        bid if signal["direction"] == "SELL" else ask
                    )
                except Exception as error:
                    await set_metaapi_connection_state(
                        "LOST",
                        str(error)
                    )
                    await asyncio.sleep(
                        PRICE_CHECK_INTERVAL_SECONDS
                    )
                    continue

                print()
                print("🚀 STARTING TRADE EXECUTION")
                print(f"   Signal ID: {signal_id}")
                print(f"   Direction: {signal['direction']}")
                print(f"   Symbol: {SYMBOL}")
                print(f"   Volume per position: {LOT_SIZE}")
                print(f"   Execution price: {execution_price:.3f}")
                print(f"   SL: {signal['stoploss']}")
                print(f"   Execution TPs: {signal['execution_tps']}")

                await send_notification(
                    "🚀 TRADE EXECUTION STARTING\n\n"
                    f"Signal ID: {signal_id}\n"
                    f"Direction: {signal['direction']}\n"
                    f"Symbol: {SYMBOL}\n"
                    f"Volume per position: {LOT_SIZE}\n"
                    f"Execution price: {execution_price:.3f}\n"
                    f"SL: {signal['stoploss']}\n"
                    f"Execution TPs: {signal['execution_tps']}\n"
                    f"MetaApi state: {metaapi_connection_state}"
                )

                success, positions = await execute_signal(
                    signal,
                    execution_price
                )

                if success:
                    signal["positions"] = positions
                    mark_signal_executed(
                        signal,
                        positions,
                        execution_price
                    )
                    start_position_monitors(signal)
                    await notify_signal_executed(signal)
                    print()
                    print("✅ PHASE 6 SIGNAL FULLY EXECUTED AND VERIFIED")
                    print(f"   Signal ID: {signal_id}")
                    print(f"   Positions confirmed: {len(positions)}/{POSITIONS_PER_SIGNAL}")
                    print(f"   Execution price: {execution_price:.3f}")
                    return

                if positions:
                    signal["positions"] = positions
                    signal["status"] = "PARTIAL_EXECUTION"
                    signal["execution_price"] = execution_price
                    signal["execution_time"] = now_iso()
                    lifecycle_signals[signal_id] = signal
                    pending_signals[signal_id] = signal
                    save_lifecycle()
                    save_pending_signals()

                    await send_notification(
                        "⚠️ PARTIAL DEMO EXECUTION - SIGNAL STILL PENDING\n\n"
                        f"Channel: {signal['channel']}\n"
                        f"Signal ID: {signal_id}\n"
                        f"Successful positions: {len(positions)}/{POSITIONS_PER_SIGNAL}\n"
                        "Phase 6 will reconnect/retry the missing position(s) and will not duplicate existing positions."
                    )

                else:
                    print()
                    print("❌ TRADE EXECUTION DID NOT COMPLETE")
                    print(f"   Signal ID: {signal_id}")
                    print("   No position was confirmed for this execution attempt.")
                    print("   Decision: keep signal pending and retry while valid.")

                    if not signal.get("execution_retry_notification_sent", False):
                        await send_notification(
                            "🟡 TRADE EXECUTION PENDING\n\n"
                            f"Signal ID: {signal_id}\n"
                            "The entry was triggered, but the trade could not be confirmed yet.\n"
                            "Phase 6 will keep the signal pending and retry while it remains valid."
                        )
                        signal["execution_retry_notification_sent"] = True
                        save_pending_signals()

                await asyncio.sleep(
                    PRICE_CHECK_INTERVAL_SECONDS
                )
                continue

            # Normal pre-entry monitoring. Connection errors here are recoverable.
            try:
                connected = await ensure_metaapi_connection(
                    signal.get("valid_until")
                )
                if not connected:
                    await asyncio.sleep(
                        PRICE_CHECK_INTERVAL_SECONDS
                    )
                    continue

                bid, ask = await get_current_price()

            except Exception as error:
                print(
                    f"⚠️ Price/connection check failed for {signal_id}: {error}"
                )
                await set_metaapi_connection_state(
                    "LOST",
                    str(error)
                )
                await asyncio.sleep(
                    PRICE_CHECK_INTERVAL_SECONDS
                )
                continue

            execution_price = (
                bid if signal["direction"] == "SELL" else ask
            )

            print()
            print("🔎 SIGNAL PRICE CHECK")
            print(f"   Signal ID: {signal_id}")
            print(f"   Bid: {bid:.3f}")
            print(f"   Ask: {ask:.3f}")
            print(f"   Execution price ({signal['direction']}): {execution_price:.3f}")

            extended_low, extended_high = calculate_extended_range(
                signal["direction"],
                signal["entry_low"],
                signal["entry_high"]
            )

            signal["execution_range_low"] = extended_low
            signal["execution_range_high"] = extended_high

            inside_original = (
                signal["entry_low"]
                <= execution_price
                <= signal["entry_high"]
            )

            inside_extended = (
                extended_low
                <= execution_price
                <= extended_high
            )

            print(f"   Original range: {signal['entry_low']:.3f} - {signal['entry_high']:.3f}")
            print(f"   Extended range: {extended_low:.3f} - {extended_high:.3f}")
            print(f"   Inside original range: {'YES' if inside_original else 'NO'}")
            print(f"   Inside extended range: {'YES' if inside_extended else 'NO'}")

            if not inside_extended:

                print()
                print("🔴 SIGNAL NOT IN RANGE")
                print(f"   Current execution price: {execution_price:.3f}")
                print(f"   Required range: {extended_low:.3f} - {extended_high:.3f}")
                print("   Decision: WAIT — no trade will be sent yet.")

                last_report = signal_status_report_times.get(signal_id)
                now_timestamp = now_utc().timestamp()

                if (
                    last_report is None
                    or now_timestamp - last_report >= SIGNAL_STATUS_REPORT_SECONDS
                ):
                    signal_status_report_times[signal_id] = now_timestamp

                    await send_notification(
                        "🔄 ENTRY WATCHER HEARTBEAT\n\n"
                        f"Signal ID: {signal_id}\n"
                        f"Time: {format_cat_time(now_iso())}\n"
                        f"Direction: {signal['direction']}\n"
                        f"Bid: {bid:.3f}\n"
                        f"Ask: {ask:.3f}\n"
                        f"Execution price: {execution_price:.3f}\n"
                        f"Original range: {signal['entry_low']:.3f} - {signal['entry_high']:.3f}\n"
                        f"Extended range: {extended_low:.3f} - {extended_high:.3f}\n"
                        f"Inside original: {'YES' if inside_original else 'NO'}\n"
                        f"Inside extended: {'YES' if inside_extended else 'NO'}\n"
                        f"MetaApi state: {metaapi_connection_state}\n"
                        f"Valid until: {format_cat_time(signal['valid_until'])}\n"
                        "Decision: WAIT — no order has been sent."
                    )

                if not signal.get("waiting_notification_sent", False):
                    await notify_signal_waiting(
                        signal,
                        bid,
                        ask
                    )
                    signal["waiting_notification_sent"] = True
                    pending_signals[signal_id] = signal
                    lifecycle_signals[signal_id] = signal
                    save_pending_signals()
                    save_lifecycle()

                print(
                    f"🟡 Waiting: {signal_id} | "
                    f"price={execution_price:.3f} | "
                    f"range={extended_low:.3f}-{extended_high:.3f}"
                )

                await asyncio.sleep(
                    PRICE_CHECK_INTERVAL_SECONDS
                )
                continue

            print()
            if inside_original:
                print("🟢 SIGNAL IS INSIDE ORIGINAL ENTRY RANGE")
            else:
                print("🟡 SIGNAL IS INSIDE 30-PIP EXTENDED RANGE")

            print("🚀 EXECUTION CONDITION MET")
            print(f"   Execution price: {execution_price:.3f}")
            print(f"   Allowed range: {extended_low:.3f} - {extended_high:.3f}")
            print("   Decision: START TRADE EXECUTION")

            signal["execution_triggered"] = True
            signal["execution_triggered_at"] = now_iso()
            signal["execution_price"] = execution_price
            signal["status"] = "EXECUTION_PENDING"
            lifecycle_signals[signal_id] = signal
            pending_signals[signal_id] = signal
            save_lifecycle()
            save_pending_signals()

            await send_notification(
                "🎯 ENTRY RANGE REACHED\n\n"
                f"Signal ID: {signal_id}\n"
                f"Direction: {signal['direction']}\n"
                f"Current Price: {execution_price:.3f}\n"
                f"Allowed Range: {extended_low:.3f}-{extended_high:.3f}\n"
                "Decision: LOCK ENTRY AND START EXECUTION."
            )

    except asyncio.CancelledError:
        raise

    except Exception as error:
        print()
        print("❌ ENTRY WATCHER CRASHED")
        print(f"   Signal ID: {signal_id}")
        print(f"   Error: {error}")
        print("   Signal remains pending if it is still valid.")
        await send_notification(
            "❌ PHASE 6 ENTRY WATCH ERROR\n\n"
            f"Signal ID: {signal_id}\n"
            f"Error: {error}\n"
            "Signal remains pending if it is still valid."
        )



# ============================================================
# POSITION LIFECYCLE MONITOR
# ============================================================

async def monitor_position(
    signal,
    position
):

    position_id = position.get(
        "position_id"
    )

    signal_id = signal[
        "signal_id"
    ]

    if not position_id:

        print(
            f"⚠️ No position ID available for "
            f"signal {signal_id}, "
            f"position {position['position_number']}."
        )

        return

    while not position.get(
        "closed",
        False
    ):

        try:

            open_position = (
                await connection
                .get_position(
                    position_id
                )
            )

            if open_position:

                await asyncio.sleep(
                    PRICE_CHECK_INTERVAL_SECONDS
                )

                continue

        except Exception as error:

            print(
                f"⚠️ Could not query position "
                f"{position_id}: {error}"
            )

            await asyncio.sleep(
                PRICE_CHECK_INTERVAL_SECONDS
            )

            continue

        close_information = (
            await get_position_close_information(
                position_id
            )
        )

        if not close_information:

            await asyncio.sleep(
                PRICE_CHECK_INTERVAL_SECONDS
            )

            continue

        position[
            "close_reason"
        ] = close_information[
            "close_reason"
        ]

        position[
            "profit"
        ] = close_information[
            "profit"
        ]

        position[
            "close_time"
        ] = close_information[
            "close_time"
        ]

        position[
            "closed"
        ] = True

        position[
            "close_deals"
        ] = close_information[
            "close_deals"
        ]

        lifecycle_signals[
            signal_id
        ] = signal

        save_lifecycle()

        rows = load_json_list(
            TRADE_LOG_FILE
        )

        for row in rows:

            if (
                isinstance(row, dict)
                and row.get("signal_id") == signal_id
            ):

                row[
                    "positions"
                ] = signal[
                    "positions"
                ]

                row[
                    "last_updated"
                ] = now_iso()

        save_json(
            TRADE_LOG_FILE,
            rows
        )

        await notify_position_closed(
            signal,
            position
        )

        print(
            f"📌 Position closed: "
            f"{signal_id} / "
            f"Position {position['position_number']} / "
            f"{position['close_reason']} / "
            f"P/L {position['profit']}"
        )

        break


# ============================================================
# START POSITION MONITORS
# ============================================================

def start_position_monitors(
    signal
):

    signal_id = signal[
        "signal_id"
    ]

    for position in signal.get(
        "positions",
        []
    ):

        if position.get(
            "closed",
            False
        ):

            continue

        task_key = (
            f"{signal_id}:"
            f"{position['position_number']}"
        )

        if task_key in watch_tasks:

            continue

        task = asyncio.create_task(
            monitor_position(
                signal,
                position
            )
        )

        watch_tasks[
            task_key
        ] = task


# ============================================================
# COMPLETED SIGNAL CLEANUP
# ============================================================

def signal_completed(
    signal
):

    positions = signal.get(
        "positions",
        []
    )

    if len(positions) < POSITIONS_PER_SIGNAL:

        return False

    return all(
        position.get(
            "closed",
            False
        )
        for position in positions
    )


async def lifecycle_completion_watcher():

    while True:

        try:

            changed = False

            for signal_id, signal in list(
                lifecycle_signals.items()
            ):

                if (
                    signal.get("status")
                    == "COMPLETED"
                ):

                    continue

                if signal_completed(
                    signal
                ):

                    signal["status"] = (
                        "COMPLETED"
                    )

                    signal["completed_at"] = (
                        now_iso()
                    )

                    changed = True

                    await send_notification(
                        "🏁 SIGNAL COMPLETED\n"
                        "\n"
                        f"Channel: {signal['channel']}\n"
                        f"Signal ID: {signal_id}\n"
                        "Both positions have closed."
                    )

            if changed:

                save_lifecycle()

        except Exception as error:

            print(
                f"⚠️ Lifecycle watcher error: {error}"
            )

        await asyncio.sleep(
            PRICE_CHECK_INTERVAL_SECONDS
        )


# ============================================================
# DAILY REPORT DATA
# ============================================================

def get_report_window():

    end_time = now_cat()

    start_time = (
        end_time
        - timedelta(
            hours=24
        )
    )

    return (
        start_time,
        end_time
    )


def signal_in_report_window(
    signal,
    start_time,
    end_time
):

    execution_time = parse_datetime(
        signal.get(
            "execution_time"
        )
    )

    if not execution_time:

        return False

    execution_time = execution_time.astimezone(
        CAT
    )

    return (
        start_time
        <= execution_time
        < end_time
    )


def build_daily_report():

    start_time, end_time = (
        get_report_window()
    )

    signals = []

    for signal in lifecycle_signals.values():

        if signal_in_report_window(
            signal,
            start_time,
            end_time
        ):

            signals.append(
                signal
            )

    total_signals = len(
        signals
    )

    total_positions = 0

    gross_profit = 0.0

    gross_loss = 0.0

    channel_data = {}

    for signal in signals:

        channel = signal.get(
            "channel",
            "Unknown"
        )

        if channel not in channel_data:

            channel_data[channel] = {

                "signals":
                    0,

                "positions":
                    0,

                "gross_profit":
                    0.0,

                "gross_loss":
                    0.0,

                "net":
                    0.0,

                "signal_rows":
                    [],
            }

        channel_data[channel][
            "signals"
        ] += 1

        signal_profit = 0.0

        signal_positions = signal.get(
            "positions",
            []
        )

        total_positions += len(
            signal_positions
        )

        for position in signal_positions:

            if position.get(
                "profit"
            ) is None:

                continue

            try:

                profit = float(
                    position.get(
                        "profit",
                        0
                    )
                    or 0
                )

            except Exception:

                profit = 0.0

            signal_profit += profit

            if profit >= 0:

                gross_profit += profit

                channel_data[channel][
                    "gross_profit"
                ] += profit

            else:

                gross_loss += profit

                channel_data[channel][
                    "gross_loss"
                ] += profit

            channel_data[channel][
                "positions"
            ] += 1

        channel_data[channel][
            "net"
        ] += signal_profit

        channel_data[channel][
            "signal_rows"
        ].append({

            "signal_id":
                signal.get(
                    "signal_id"
                ),

            "profit":
                signal_profit,

            "positions":
                len(
                    signal_positions
                ),

        })

    net = (
        gross_profit
        + gross_loss
    )

    lines = []

    lines.append(
        "📊 DAILY DEMO REPORT"
    )

    lines.append(
        ""
    )

    lines.append(
        f"Period: "
        f"{start_time.strftime('%Y-%m-%d %H:%M CAT')} "
        f"to "
        f"{end_time.strftime('%Y-%m-%d %H:%M CAT')}"
    )

    lines.append(
        ""
    )

    lines.append(
        f"Signals executed: {total_signals}"
    )

    lines.append(
        f"Positions: {total_positions}"
    )

    lines.append(
        f"Gross Profit: {gross_profit:.2f}"
    )

    lines.append(
        f"Gross Loss: {gross_loss:.2f}"
    )

    lines.append(
        f"Net P/L: {net:.2f}"
    )

    lines.append(
        ""
    )

    lines.append(
        "CHANNEL BREAKDOWN"
    )

    lines.append(
        "-----------------"
    )

    for channel, data in sorted(
        channel_data.items()
    ):

        lines.append(
            ""
        )

        lines.append(
            channel
        )

        lines.append(
            f"Signals: {data['signals']}"
        )

        lines.append(
            f"Positions: {data['positions']}"
        )

        lines.append(
            f"Gross Profit: "
            f"{data['gross_profit']:.2f}"
        )

        lines.append(
            f"Gross Loss: "
            f"{data['gross_loss']:.2f}"
        )

        lines.append(
            f"Net: "
            f"{data['net']:.2f}"
        )

        lines.append(
            "Per signal:"
        )

        for row in data[
            "signal_rows"
        ]:

            lines.append(
                f"  {row['signal_id']} | "
                f"{row['positions']} positions | "
                f"{row['profit']:.2f}"
            )

    return (
        "\n".join(lines),
        {
            "report_date":
                end_time.strftime(
                    "%Y-%m-%d"
                ),

            "generated_at":
                now_iso(),

            "period_start":
                start_time.isoformat(),

            "period_end":
                end_time.isoformat(),

            "signals":
                total_signals,

            "positions":
                total_positions,

            "gross_profit":
                gross_profit,

            "gross_loss":
                gross_loss,

            "net":
                net,

            "channels":
                channel_data,
        }
    )


# ============================================================
# SEND DAILY REPORT
# ============================================================

async def send_daily_report():

    report_text, report_data = (
        build_daily_report()
    )

    report_date = report_data[
        "report_date"
    ]

    if report_date in daily_reported_dates:

        return

    await send_notification(
        report_text
    )

    rows = load_json_list(
        DAILY_REPORT_FILE
    )

    rows.append(
        report_data
    )

    save_json(
        DAILY_REPORT_FILE,
        rows
    )

    daily_reported_dates.add(
        report_date
    )

    print(
        "✅ Daily report sent."
    )


# ============================================================
# DAILY REPORT SCHEDULER
# ============================================================

async def daily_report_scheduler():

    while True:

        try:

            current = now_cat()

            if (
                current.hour
                == DAILY_REPORT_HOUR
                and current.minute
                == DAILY_REPORT_MINUTE
            ):

                await send_daily_report()

        except Exception as error:

            print(
                f"⚠️ Daily report error: {error}"
            )

        await asyncio.sleep(
            30
        )


# ============================================================
# RESOLVE PHASE 4 OUTPUT CHANNEL
# ============================================================

async def resolve_phase4_output_channel(
    client
):

    print()
    print(
        "=" * 70
    )

    print(
        "RESOLVING PHASE 4 PRIVATE OUTPUT CHANNEL"
    )

    print(
        "=" * 70
    )

    try:

        entity = (
            await client.get_entity(
                PHASE4_OUTPUT_CHANNEL_ID
            )
        )

        title = (
            getattr(
                entity,
                "title",
                None
            )
            or getattr(
                entity,
                "username",
                None
            )
            or str(
                PHASE4_OUTPUT_CHANNEL_ID
            )
        )

        print(
            f"✅ Phase 4 output channel accessible"
        )

        print(
            f"   Name: {title}"
        )

        print(
            f"   ID: {PHASE4_OUTPUT_CHANNEL_ID}"
        )

        return entity

    except Exception as error:

        print(
            "❌ Could not access Phase 4 "
            f"output channel: {error}"
        )

        raise


# ============================================================
# TELEGRAM MESSAGE HANDLER
# ============================================================
#
# IMPORTANT:
#
# Phase 6 listens ONLY to the Phase 4 private output channel.
#
# It does NOT monitor:
#
#   Goldhunterlearnttade3867
#   GoldSignalVip110
#   MrHenrys122
#   AGoldvip_0786
#   GUNS THE TRADER
#
# Phase 4 is responsible for those sources.
#
# ============================================================

async def handle_message(
    event
):

    try:

        chat_id = int(
            event.chat_id
        )

        if chat_id != (
            PHASE4_OUTPUT_CHANNEL_ID
        ):

            return

        message_text = (
            event.raw_text
            or ""
        )

        if not message_text.strip():

            return

        message_id = int(
            event.message.id
        )

        print()
        print("🔎 PHASE 6: READING PHASE 4 EXECUTION MESSAGE...")
        print(f"   Message ID: {message_id}")
        print(f"   Message length: {len(message_text)} characters")

        signal = read_phase4_execution_message(
            message_text,
            chat_id,
            message_id
        )

        if not signal:
            print()
            print("❌ PHASE 6 COULD NOT READ THE PHASE 4 EXECUTION MESSAGE")
            print("   The message was received, but read_phase4_execution_message() returned None.")
            print("   No trade attempt was made.")
            print("   Check that Phase 4 sent the expected execution fields.")

            await send_notification(
                "❌ PHASE 6 COULD NOT READ PHASE 4 MESSAGE\n\n"
                f"Message ID: {message_id}\n"
                "No trade attempt was made.\n"
                "Reason: read_phase4_execution_message() returned None."
            )
            return

        print()
        print("✅ PHASE 6 READ THE PHASE 4 EXECUTION DATA")
        print(f"   Signal ID: {signal.get('signal_id')}")
        print(f"   Direction: {signal.get('direction')}")
        print(f"   Entry: {signal.get('entry_low')} - {signal.get('entry_high')}")
        print(f"   SL: {signal.get('stoploss')}")
        print(f"   Execution TPs: {signal.get('execution_tps')}")
        print(f"   TP numbers: {signal.get('execution_tp_numbers')}")
        print(f"   Valid Until: {format_cat_time(signal.get('valid_until'))}")

        await send_notification(
            "✅ PHASE 6 READ PHASE 4 EXECUTION DATA\n\n"
            f"Signal ID: {signal.get('signal_id')}\n"
            f"Direction: {signal.get('direction')}\n"
            f"Entry: {signal.get('entry_low')} - {signal.get('entry_high')}\n"
            f"SL: {signal.get('stoploss')}\n"
            f"Execution TPs: {signal.get('execution_tps')}\n"
            f"TP numbers: {signal.get('execution_tp_numbers')}\n"
            f"Valid until: {format_cat_time(signal.get('valid_until'))}\n"
            f"Telegram message ID: {message_id}"
        )

        signal_id = signal[
            "signal_id"
        ]

        print()
        print(
            "=" * 70
        )

        print(
            "🟢 PHASE 4 SIGNAL RECEIVED"
        )

        print(
            "=" * 70
        )

        print(
            f"Channel:   {signal['channel']}"
        )

        print(
            f"Signal ID: {signal_id}"
        )

        print(
            f"Direction: {signal['direction']}"
        )

        print(
            f"Entry:     "
            f"{signal['entry_low']} - "
            f"{signal['entry_high']}"
        )

        print(
            f"SL:        "
            f"{signal['stoploss']}"
        )

        print(
            f"Execution TPs: "
            f"{signal['execution_tps']}"
        )

        print(
            f"Valid Until: "
            f"{format_cat_time(signal['valid_until'])}"
        )

        # ----------------------------------------------------
        # DUPLICATE / EXISTING LIFECYCLE CHECK
        # ----------------------------------------------------

        if signal_id in executed_signals:
            print()
            print("🛑 SIGNAL REJECTED BY DUPLICATE PROTECTION")
            print(f"   Signal ID: {signal_id}")
            print("   Reason: signal already exists in executed_signals.")
            return

        if signal_id in lifecycle_signals:
            existing_status = lifecycle_signals[signal_id].get("status", "UNKNOWN")
            print()
            print("🛑 SIGNAL REJECTED BY LIFECYCLE PROTECTION")
            print(f"   Signal ID: {signal_id}")
            print(f"   Existing status: {existing_status}")
            print("   No new watcher or trade attempt was created.")
            return

        if signal_id in pending_signals:
            existing_status = pending_signals[signal_id].get("status", "UNKNOWN")
            print()
            print("🛑 SIGNAL ALREADY PENDING")
            print(f"   Signal ID: {signal_id}")
            print(f"   Existing status: {existing_status}")
            print("   Existing watcher will continue.")
            return

        existing_task = watch_tasks.get(signal_id)
        if existing_task is not None and not existing_task.done():
            print(
                "⚠️ SIGNAL ALREADY HAS AN ACTIVE WATCHER"
            )
            return

        # ----------------------------------------------------
        # INITIALIZE EXECUTION RANGE
        # ----------------------------------------------------

        extended_low, extended_high = (
            calculate_extended_range(
                signal["direction"],
                signal["entry_low"],
                signal["entry_high"]
            )
        )

        signal[
            "execution_range_low"
        ] = extended_low

        signal[
            "execution_range_high"
        ] = extended_high

        print()
        print("📐 EXECUTION RANGE CALCULATED")
        print(f"   Original entry: {signal['entry_low']:.3f} - {signal['entry_high']:.3f}")
        print(f"   Extended range: {extended_low:.3f} - {extended_high:.3f}")
        print(f"   Extension: {ENTRY_EXTENSION_PRICE:.3f} price units")

        await send_notification(
            "📐 EXECUTION RANGE CALCULATED\n\n"
            f"Signal ID: {signal_id}\n"
            f"Direction: {signal['direction']}\n"
            f"Original entry: {signal['entry_low']:.3f} - {signal['entry_high']:.3f}\n"
            f"Allowed range: {extended_low:.3f} - {extended_high:.3f}\n"
            f"Extension: {ENTRY_EXTENSION_PRICE:.3f}\n"
            f"Valid until: {format_cat_time(signal['valid_until'])}"
        )

        signal[
            "status"
        ] = "WAITING"

        signal[
            "detected_at"
        ] = now_iso()

        signal[
            "waiting_notification_sent"
        ] = False

        pending_signals[
            signal_id
        ] = signal

        lifecycle_signals[
            signal_id
        ] = signal

        save_pending_signals()

        save_lifecycle()

        await notify_signal_detected(
            signal
        )

        # ----------------------------------------------------
        # START WAITING WATCHER
        # ----------------------------------------------------

        task = asyncio.create_task(
            wait_for_entry(
                signal
            )
        )

        watch_tasks[
            signal_id
        ] = task

        print()
        print("🟡 ENTRY WATCHER STARTED")
        print(f"   Signal ID: {signal_id}")
        print(f"   Monitoring symbol: {SYMBOL}")
        print(f"   Direction: {signal['direction']}")
        print(f"   Allowed range: {extended_low:.3f} - {extended_high:.3f}")
        print(f"   Valid until: {format_cat_time(signal['valid_until'])}")
        print("   Checking price every 2 seconds.")

        await send_notification(
            "🟡 ENTRY WATCHER STARTED\n\n"
            f"Signal ID: {signal_id}\n"
            f"Symbol: {SYMBOL}\n"
            f"Direction: {signal['direction']}\n"
            f"Allowed range: {extended_low:.3f} - {extended_high:.3f}\n"
            f"Valid until: {format_cat_time(signal['valid_until'])}\n"
            "Price checks: every 2 seconds\n"
            "Telegram market heartbeat: every 30 seconds."
        )

    except Exception as error:

        print()
        print(
            "❌ ERROR PROCESSING "
            "PHASE 4 TELEGRAM MESSAGE:"
        )

        print(
            error
        )

        import traceback

        traceback.print_exc()


# ============================================================
# RESUME PENDING SIGNALS AFTER RESTART
# ============================================================

def resume_pending_signals():

    for signal_id, signal in list(
        pending_signals.items()
    ):

        if signal_id in watch_tasks:

            continue

        if signal_is_expired(
            signal
        ):

            signal[
                "status"
            ] = "EXPIRED"

            signal[
                "expired_at"
            ] = now_iso()

            lifecycle_signals[
                signal_id
            ] = signal

            pending_signals.pop(
                signal_id,
                None
            )

            continue

        task = asyncio.create_task(
            wait_for_entry(
                signal
            )
        )

        watch_tasks[
            signal_id
        ] = task

    save_pending_signals()

    save_lifecycle()


# ============================================================
# RESUME POSITION MONITORS AFTER RESTART
# ============================================================

def resume_position_monitors():

    for signal_id, signal in list(
        lifecycle_signals.items()
    ):

        if signal.get(
            "status"
        ) not in (
            "EXECUTED",
            "PARTIAL_EXECUTION"
        ):

            continue

        start_position_monitors(
            signal
        )


# ============================================================
# DISPLAY SETTINGS
# ============================================================

def display_settings():

    print()
    print(
        "=" * 70
    )

    print(
        "PHASE 6 SETTINGS"
    )

    print(
        "=" * 70
    )

    print(
        f"  • Phase 4 output channel: "
        f"{PHASE4_OUTPUT_CHANNEL_ID}"
    )

    print(
        f"  • Symbol: {SYMBOL}"
    )

    print(
        f"  • Volume: {LOT_SIZE} each"
    )

    print(
        f"  • Positions per signal: "
        f"{POSITIONS_PER_SIGNAL}"
    )

    print(
        f"  • Entry extension: "
        f"{ENTRY_EXTENSION_PIPS} pips"
    )

    print(
        f"  • Price extension: "
        f"{ENTRY_EXTENSION_PRICE}"
    )

    print(
        "  • TP selection: Phase 4 output"
    )

    print(
        "  • Signal validity: 2 hours"
    )

    print(
        "  • Out-of-range signals: WAIT"
    )

    print(
        "  • Position closure: TP / SL + actual P/L"
    )

    print(
        "  • Daily report: 04:00 CAT"
    )

    print(
        "  • DEMO ONLY"
    )

    print(
        "  • MetaApi auto-reconnect: ON"
    )

    print(
        "  • Pending signal recovery: ON"
    )

    print(
        "  • Duplicate trade protection: ON"
    )

    print(
        "  • Post-trade MT5 verification: ON"
    )

    print(
        "  • Telegram INPUT: Phase 4 channel (read-only)"
    )
    print(
        "  • Telegram STATUS: separate private channel"
    )
    print(
        f"  • Status chat ID: {PHASE6_STATUS_CHAT_ID or 'NOT CONFIGURED'}"
    )
    print(
        "  • Detailed background diagnostics: ON"
    )


# ============================================================
# PRIVATE TELEGRAM STATUS HEARTBEAT
# ============================================================

async def status_heartbeat():

    while True:

        try:

            active_watchers = sum(
                1
                for task in watch_tasks.values()
                if task is not None and not task.done()
            )

            await send_notification(
                "💓 PHASE 6 HEARTBEAT\n\n"
                f"Time: {format_cat_time(now_iso())}\n"
                f"MetaApi: {metaapi_connection_state}\n"
                f"Symbol: {SYMBOL}\n"
                f"Active watchers: {active_watchers}\n"
                f"Pending signals: {len(pending_signals)}\n"
                f"Executed signals in memory: {len(executed_signals)}\n"
                "Telegram listener: ACTIVE\n"
                "Worker: ALIVE"
            )

        except asyncio.CancelledError:
            raise

        except Exception as error:

            print(
                "⚠️ Status heartbeat failed: "
                f"{type(error).__name__}: {error}"
            )

        await asyncio.sleep(
            STATUS_HEARTBEAT_SECONDS
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    global metaapi
    global account
    global connection
    global telegram_client
    global metaapi_connection_lock

    print()
    print(
        "=" * 70
    )

    print(
        "PHASE 6 - DEMO TELEGRAM → MT5 EXECUTOR"
    )

    print(
        "=" * 70
    )

    print()
    print(
        "⚠️ DEMO-ONLY MODE"
    )

    print(
        "⚠️ NO LIVE ACCOUNT TRADING ALLOWED"
    )

    print()

    print(
        f"Symbol: {SYMBOL}"
    )

    print(
        f"Lot per position: {LOT_SIZE}"
    )

    print(
        f"Positions per signal: "
        f"{POSITIONS_PER_SIGNAL}"
    )

    print(
        f"Entry extension: "
        f"{ENTRY_EXTENSION_PIPS} pips"
    )

    print(
        f"Price extension: "
        f"{ENTRY_EXTENSION_PRICE}"
    )

    display_settings()

    # --------------------------------------------------------
    # TELEGRAM STRING SESSION
    # --------------------------------------------------------

    print()
    print(
        "🔐 Telegram authentication mode:"
    )

    if TELEGRAM_SESSION_STRING:

        print(
            "   StringSession"
        )

        print(
            "   ✅ TELEGRAM_SESSION_STRING found"
        )

    else:

        print(
            "   ❌ TELEGRAM_SESSION_STRING missing"
        )

        raise RuntimeError(
            "TELEGRAM_SESSION_STRING "
            "environment variable is required."
        )

    # --------------------------------------------------------
    # REQUIRED ENVIRONMENT VARIABLES
    # --------------------------------------------------------

    if not API_HASH:

        raise RuntimeError(
            "TELEGRAM_API_HASH "
            "is missing."
        )

    if not METAAPI_TOKEN:

        raise RuntimeError(
            "METAAPI_TOKEN "
            "is missing."
        )

    if not METAAPI_ACCOUNT_ID:

        raise RuntimeError(
            "METAAPI_ACCOUNT_ID "
            "is missing."
        )

    # --------------------------------------------------------
    # MEMORY
    # --------------------------------------------------------

    load_runtime_memory()

    # ========================================================
    # METAAPI
    # ========================================================

    print()
    print(
        "=" * 70
    )

    print(
        "CONNECTING TO METAAPI"
    )

    print(
        "=" * 70
    )

    metaapi = MetaApi(
        METAAPI_TOKEN
    )

    print(
        "✅ MetaApi SDK initialized"
    )

    account = (
        await metaapi
        .metatrader_account_api
        .get_account(
            METAAPI_ACCOUNT_ID
        )
    )

    account_name = get_field(
        account,
        "name",
        METAAPI_ACCOUNT_ID
    )

    account_server = get_field(
        account,
        "server",
        "Unknown"
    )

    account_state = get_field(
        account,
        "state",
        "Unknown"
    )

    print(
        f"✅ Account found: "
        f"{account_name}"
    )

    print(
        f"   Server: "
        f"{account_server}"
    )

    print(
        f"   State: "
        f"{account_state}"
    )

    # --------------------------------------------------------
    # SAFETY CHECK
    # --------------------------------------------------------

    server_text = str(
        account_server
    ).lower()

    if "demo" not in server_text:

        raise RuntimeError(

            "🛑 SAFETY STOP: "
            "Configured MetaApi account "
            "does not appear to be a "
            "DEMO account."
        )

    print(
        "🟢 DEMO ACCOUNT CONFIRMED"
    )

    # --------------------------------------------------------
    # DEPLOY ACCOUNT IF NECESSARY
    # --------------------------------------------------------

    if str(
        account_state
    ).upper() != "DEPLOYED":

        print(
            "⏳ Deploying MetaApi account..."
        )

        await account.deploy()

    # --------------------------------------------------------
    # WAIT FOR CONNECTION
    # --------------------------------------------------------

    print(
        "⏳ Waiting for MT5 connection..."
    )

    await account.wait_connected()

    print(
        "✅ MT5 ACCOUNT CONNECTED"
    )

    # --------------------------------------------------------
    # RPC CONNECTION
    # --------------------------------------------------------

    connection = (
        account.get_rpc_connection()
    )

    await connection.connect()

    print(
        "✅ RPC connection established"
    )

    # --------------------------------------------------------
    # SYNCHRONIZATION
    # --------------------------------------------------------

    await connection.wait_synchronized()

    print(
        "✅ MT5 ACCOUNT SYNCHRONIZED"
    )

    metaapi_connection_lock = asyncio.Lock()

    # Read the broker/application connection status explicitly.
    refreshed_account = await refresh_metaapi_account()
    initial_status = str(
        get_field(
            refreshed_account,
            "connectionStatus",
            get_field(refreshed_account, "connection_status", "UNKNOWN")
        )
    ).upper() if refreshed_account is not None else "UNKNOWN"

    print(
        f"🔎 MetaApi connectionStatus: {initial_status}"
    )

    if initial_status == "CONNECTED":
        metaapi_connection_state = "CONNECTED"

    # --------------------------------------------------------
    # SYMBOL
    # --------------------------------------------------------

    await verify_symbol()

    # ========================================================
    # TELEGRAM
    # ========================================================

    print()
    print(
        "=" * 70
    )

    print(
        "CONNECTING TO TELEGRAM"
    )

    print(
        "=" * 70
    )

    client = TelegramClient(

        StringSession(
            TELEGRAM_SESSION_STRING
        ),

        API_ID,

        API_HASH
    )

    await client.start()

    telegram_client = client

    if not await client.is_user_authorized():

        raise RuntimeError(
            "Telegram StringSession "
            "is not authorized."
        )

    print(
        "✅ TELEGRAM SESSION AUTHORIZED"
    )

    # --------------------------------------------------------
    # RESOLVE ONLY PHASE 4 OUTPUT
    # --------------------------------------------------------

    phase4_output_entity = await resolve_phase4_output_channel(
        client
    )

    # ========================================================
    # ACTIVE
    # ========================================================

    print()
    print(
        "=" * 70
    )

    print(
        "PHASE 6 DEMO EXECUTOR ACTIVE"
    )

    print(
        "=" * 70
    )

    print()
    print(
        "MONITORING ONLY:"
    )

    print(
        f"  • Phase 4 private output: "
        f"{PHASE4_OUTPUT_CHANNEL_ID}"
    )

    print()
    print(
        "NOT MONITORING ORIGINAL SIGNAL CHANNELS."
    )

    print()
    print(
        "TRADE SETTINGS:"
    )

    print(
        f"  • Symbol: {SYMBOL}"
    )

    print(
        "  • Position 1: TP supplied by Phase 4"
    )

    print(
        "  • Position 2: TP supplied by Phase 4"
    )

    print(
        f"  • Volume: {LOT_SIZE} each"
    )

    print(
        "  • Same stop loss"
    )

    print(
        f"  • {ENTRY_EXTENSION_PIPS}-pip "
        "entry extension"
    )

    print(
        "  • Out-of-range: keep waiting"
    )

    print(
        "  • Signal validity: 2 hours"
    )

    print(
        "  • DEMO ONLY"
    )

    # --------------------------------------------------------
    # EVENT HANDLER
    # --------------------------------------------------------
    #
    # Phase 6 receives Telegram new-message events globally and
    # handle_message() filters them using the exact Phase 4
    # output channel ID.
    #
    # This is the same proven listener mechanism verified by
    # test_phase6_telegram_listener.py.
    #
    # Phase 6 processes ONLY:
    #
    #     -1003995895185
    #
    # Original signal channels are ignored by handle_message().
    #
    # --------------------------------------------------------

    # --------------------------------------------------------
    # PROVEN GLOBAL TELEGRAM LISTENER
    # --------------------------------------------------------
    #
    # Receive ALL new Telegram messages first, then filter by the
    # exact Phase 4 channel ID. This is the listener mechanism
    # already proven to receive Phase 4 messages.
    # --------------------------------------------------------

    @client.on(
        events.NewMessage()
    )
    async def on_new_message(event):

        try:

            event_chat_id = int(
                event.chat_id
            )

            if event_chat_id != (
                PHASE4_OUTPUT_CHANNEL_ID
            ):

                return

            print()
            print(
                "📥 PHASE 4 CHANNEL MESSAGE RECEIVED"
            )
            print(
                "=" * 70
            )
            print(
                f"Chat ID:    {event_chat_id}"
            )
            print(
                f"Message ID: {event.message.id}"
            )
            print(
                "Text:"
            )
            print(
                event.raw_text or ""
            )

            await send_notification(
                "📥 PHASE 4 MESSAGE RECEIVED\n\n"
                f"Time: {format_cat_time(now_iso())}\n"
                f"Message ID: {event.message.id}\n"
                f"Chat ID: {event_chat_id}\n\n"
                "Raw message:\n"
                f"{event.raw_text or '[EMPTY]'}"
            )

            # Run processing separately so MetaApi operations and
            # notifications never block Telegram's update loop.
            asyncio.create_task(
                handle_message(
                    event
                )
            )

        except Exception as error:

            print()
            print(
                "❌ TELEGRAM EVENT DISPATCH ERROR:"
            )
            print(
                error
            )

            import traceback

            traceback.print_exc()

    # --------------------------------------------------------
    # BACKGROUND LIFECYCLE TASKS
    # --------------------------------------------------------

    lifecycle_task = asyncio.create_task(
        lifecycle_completion_watcher()
    )

    daily_report_task = asyncio.create_task(
        daily_report_scheduler()
    )

    metaapi_monitor_task = asyncio.create_task(
        metaapi_connection_monitor()
    )

    status_heartbeat_task = asyncio.create_task(
        status_heartbeat()
    )

    # --------------------------------------------------------
    # RESUME PENDING / OPEN POSITIONS
    # --------------------------------------------------------

    resume_pending_signals()

    resume_position_monitors()

    print()
    print(
        "=" * 70
    )

    print(
        "TELEGRAM LISTENER ACTIVE"
    )

    print(
        "=" * 70
    )

    print()
    print(
        "Waiting for Phase 4 parsed signals..."
    )

    print(
        "Press CTRL+C to stop."
    )

    if not PHASE6_STATUS_CHAT_ID:
        print(
            "⚠️ PHASE6_STATUS_CHAT_ID is not configured. "
            "Configure the separate private status channel before deployment."
        )

    await send_notification(
        "🟢 PHASE 6 DEMO EXECUTOR ONLINE\n\n"
        f"Time: {format_cat_time(now_iso())}\n"
        f"MetaApi connection: {metaapi_connection_state}\n"
        f"MT5 symbol: {SYMBOL}\n"
        f"Lot size: {LOT_SIZE} x {POSITIONS_PER_SIGNAL} positions\n"
        f"Phase 4 INPUT channel: {PHASE4_OUTPUT_CHANNEL_ID}\n"
        f"Phase 6 STATUS channel: {PHASE6_STATUS_CHAT_ID or 'NOT CONFIGURED'}\n"
        "Telegram listener: ACTIVE\n"
        "Detailed diagnostics: ACTIVE\n"
        "⚠️ DEMO ONLY"
    )

    # --------------------------------------------------------
    # RUN FOREVER
    # --------------------------------------------------------

    try:

        await client.run_until_disconnected()

    finally:

        lifecycle_task.cancel()

        daily_report_task.cancel()

        metaapi_monitor_task.cancel()

        status_heartbeat_task.cancel()

        for task in list(
            watch_tasks.values()
        ):

            task.cancel()

        try:

            if connection is not None:
                await connection.close()
        except Exception:
            pass

        try:

            await client.disconnect()

        except Exception:

            pass

        print(
            "Telegram client disconnected."
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()

        print(
            "🛑 Phase 6 stopped by user."
        )

    except Exception as error:

        print()

        print(
            "❌ FATAL ERROR:"
        )

        print(
            error
        )

        import traceback

        traceback.print_exc()
