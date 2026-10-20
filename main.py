
import hmac
import os
import json
import hashlib
import sqlite3
import threading
import re
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

import MetaTrader5 as mt5
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

load_dotenv()

API_KEY = os.getenv("API_KEY")
ENABLE_DEMO_TRADING = os.getenv("ENABLE_DEMO_TRADING", "false").lower() == "true"
ENABLE_LIVE_TRADING = os.getenv("ENABLE_LIVE_TRADING", "false").lower() == "true"
LIVE_ACCOUNT_LOGIN = os.getenv("LIVE_ACCOUNT_LOGIN", "").strip()
LIVE_ACCOUNT_SERVER = os.getenv("LIVE_ACCOUNT_SERVER", "").strip()
TERMINAL_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"
DEFAULT_SYMBOL = "XAUUSD_i"
MAGIC = 816202610
DEMO_VOLUME = 0.01
LIVE_VOLUME = 0.1
DB_PATH = os.getenv("EXECUTION_DB_PATH", "gold_robot_execution.sqlite3")

if not API_KEY:
    raise RuntimeError("API_KEY is missing from .env")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not mt5.initialize(path=TERMINAL_PATH, timeout=60000):
        raise RuntimeError(f"MT5 initialization failed: {mt5.last_error()}")
    initialise_execution_db()
    yield
    mt5.shutdown()


app = FastAPI(
    title="Gold Robot Execution API",
    version="0.3.0",
    description="MT5 API with separately gated demo and live execution.",
    lifespan=lifespan,
)

# Durable idempotency ledger. Keep this database on persistent storage on the Windows MT5 host.
_request_lock = threading.Lock()


def db_connect():
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def initialise_execution_db():
    with db_connect() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS execution_requests (
                request_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                comment_tag TEXT NOT NULL,
                response_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def get_execution_request(request_id):
    with db_connect() as db:
        row = db.execute(
            "SELECT * FROM execution_requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        return dict(row) if row else None


def save_execution_request(request_id, state, comment_tag, response=None):
    now = utc_now()
    with db_connect() as db:
        db.execute("""
            INSERT INTO execution_requests
                (request_id, state, comment_tag, response_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(request_id) DO UPDATE SET
                state=excluded.state,
                comment_tag=excluded.comment_tag,
                response_json=COALESCE(excluded.response_json, execution_requests.response_json),
                updated_at=excluded.updated_at
        """, (
            request_id, state, comment_tag,
            json.dumps(response, default=str) if response is not None else None,
            now, now,
        ))


def find_mt5_execution_by_comment(comment_tag):
    # Reconciliation only: this function never submits an order.
    positions = mt5.positions_get()
    if positions:
        for p in positions:
            if getattr(p, "comment", "") == comment_tag:
                return {
                    "found": True, "source": "open_position",
                    "ticket": int(p.ticket),
                    "position_id": int(getattr(p, "identifier", p.ticket)),
                    "symbol": p.symbol, "volume": p.volume, "profit": p.profit,
                }

    end = datetime.now()
    start = end - timedelta(days=90)
    deals = mt5.history_deals_get(start, end)
    if deals:
        matches = [d for d in deals if getattr(d, "comment", "") == comment_tag]
        if matches:
            matches.sort(key=lambda d: getattr(d, "time_msc", getattr(d, "time", 0)))
            last = matches[-1]
            return {
                "found": True, "source": "deal_history",
                "deal_ticket": int(last.ticket),
                "order_ticket": int(getattr(last, "order", 0)),
                "position_id": int(getattr(last, "position_id", 0)),
                "symbol": getattr(last, "symbol", DEFAULT_SYMBOL),
                "volume": getattr(last, "volume", None),
                "profit": getattr(last, "profit", None),
            }
    return {"found": False}


def authorize(x_api_key: str = Header(default="")):
    if not hmac.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="Invalid API key")


def require_account():
    account = mt5.account_info()
    terminal = mt5.terminal_info()

    if account is None or terminal is None or not terminal.connected:
        raise HTTPException(status_code=503, detail="MT5 account is disconnected")

    return account


def require_demo_trading():
    if not ENABLE_DEMO_TRADING:
        raise HTTPException(
            status_code=403,
            detail="Demo execution is disabled. Enable it explicitly in .env.",
        )

    account = require_account()

    if account.trade_mode != mt5.ACCOUNT_TRADE_MODE_DEMO:
        raise HTTPException(
            status_code=403,
            detail="Refusing to execute: MT5 account is not a demo account.",
        )

    if not account.trade_allowed:
        raise HTTPException(status_code=403, detail="Trading is disabled for this account")

    terminal = mt5.terminal_info()
    if terminal is None or not terminal.trade_allowed:
        raise HTTPException(status_code=403, detail="MT5 terminal trading is disabled")

    return account


def require_live_trading():
    if not ENABLE_LIVE_TRADING:
        raise HTTPException(status_code=403, detail="Live execution is disabled. Enable it explicitly in .env.")
    if not LIVE_ACCOUNT_LOGIN or not LIVE_ACCOUNT_SERVER:
        raise HTTPException(status_code=503, detail="Live account allowlist is not configured.")
    account = require_account()
    if account.trade_mode != mt5.ACCOUNT_TRADE_MODE_REAL:
        raise HTTPException(status_code=403, detail="Refusing live execution: MT5 account is not a verified real account.")
    if str(account.login) != LIVE_ACCOUNT_LOGIN or account.server != LIVE_ACCOUNT_SERVER:
        raise HTTPException(status_code=403, detail="Refusing live execution: MT5 account login/server does not match the configured allowlist.")
    if not account.trade_allowed:
        raise HTTPException(status_code=403, detail="Trading is disabled for this account")
    terminal = mt5.terminal_info()
    if terminal is None or not terminal.trade_allowed:
        raise HTTPException(status_code=403, detail="MT5 terminal trading is disabled")
    return account


class DemoOrderRequest(BaseModel):
    request_id: str = Field(min_length=8, max_length=100)
    side: str
    stop_loss: float = Field(gt=0)
    take_profit: float = Field(gt=0)


@app.get("/health")
def health():
    terminal = mt5.terminal_info()
    account = mt5.account_info()

    return {
        "api": "ok",
        "mt5_connected": bool(terminal and terminal.connected),
        "account_authenticated": account is not None,
        "trading_enabled_by_api": ENABLE_DEMO_TRADING or ENABLE_LIVE_TRADING,
        "demo_account_only": not ENABLE_LIVE_TRADING,
        "live_trading_enabled": ENABLE_LIVE_TRADING,
    }


@app.get("/account", dependencies=[Depends(authorize)])
def account_info():
    account = require_account()
    return {
        "login": account.login,
        "server": account.server,
        "company": account.company,
        "is_demo": account.trade_mode == mt5.ACCOUNT_TRADE_MODE_DEMO,
        "currency": account.currency,
        "balance": account.balance,
        "equity": account.equity,
        "margin_free": account.margin_free,
        "trade_allowed": account.trade_allowed,
    }


@app.get("/symbols/{symbol}/quote", dependencies=[Depends(authorize)])
def get_quote(symbol: str):
    require_account()
    info = mt5.symbol_info(symbol)

    if info is None:
        raise HTTPException(status_code=404, detail=f"Symbol not found: {symbol}")

    if not info.visible and not mt5.symbol_select(symbol, True):
        raise HTTPException(status_code=503, detail="Could not enable symbol")

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        raise HTTPException(status_code=503, detail="No quote available")

    return {
        "symbol": symbol,
        "bid": tick.bid,
        "ask": tick.ask,
        "time": tick.time,
        "digits": info.digits,
        "point": info.point,
        "trade_mode": info.trade_mode,
    }


@app.get("/positions", dependencies=[Depends(authorize)])
def get_positions():
    require_account()
    positions = mt5.positions_get()

    if positions is None:
        raise HTTPException(status_code=503, detail="Could not retrieve positions")

    return {
        "count": len(positions),
        "positions": [
            {
                "ticket": p.ticket,
                "position_id": int(getattr(p, "identifier", p.ticket)),
                "symbol": p.symbol,
                "type": p.type,
                "volume": p.volume,
                "price_open": p.price_open,
                "price_current": p.price_current,
                "sl": p.sl,
                "tp": p.tp,
                "profit": p.profit,
                "magic": p.magic,
                "comment": p.comment,
            }
            for p in positions
        ],
    }


@app.get("/orders", dependencies=[Depends(authorize)])
def get_pending_orders():
    require_account()
    orders = mt5.orders_get()

    if orders is None:
        raise HTTPException(status_code=503, detail="Could not retrieve orders")

    return {
        "count": len(orders),
        "orders": [
            {
                "ticket": o.ticket,
                "symbol": o.symbol,
                "type": o.type,
                "volume": o.volume_current,
                "price_open": o.price_open,
                "sl": o.sl,
                "tp": o.tp,
            }
            for o in orders
        ],
    }


@app.post("/demo/order", dependencies=[Depends(authorize)])
@app.post("/live/order", dependencies=[Depends(authorize)])
def place_order(request: DemoOrderRequest, http_request: Request):
    live_mode = http_request.url.path == "/live/order"
    side = request.side.strip().upper()

    if side not in ("BUY", "SELL"):
        raise HTTPException(status_code=422, detail="side must be BUY or SELL")

    with _request_lock:
        # Use Phase 6's stable short marker directly when it fits MT5's comment limit.
        # Longer arbitrary request IDs use a deterministic hash to avoid truncation collisions.
        request_key = ("LIVE:" if live_mode else "DEMO:") + request.request_id
        # Phase 6 embeds the mode into live markers while preserving existing demo markers.
        # Keep the request marker intact so Phase 6 can reconcile MT5 positions by comment.
        if len(request.request_id) <= 31 and re.fullmatch(r"[A-Za-z0-9:_-]+", request.request_id):
            comment_tag = request.request_id
        else:
            comment_tag = "GR-" + hashlib.sha256(request.request_id.encode("utf-8")).hexdigest()[:27]
        previous = get_execution_request(request_key)
        if previous:
            if previous["state"] in ("CONFIRMED", "REJECTED") and previous.get("response_json"):
                return {
                    "duplicate": True,
                    "state": previous["state"],
                    "result": json.loads(previous["response_json"]),
                }
            # An in-flight request may have executed before a crash/timeout.
            # Reconcile first; never blindly resubmit an uncertain request.
            reconciled = find_mt5_execution_by_comment(previous["comment_tag"])
            if reconciled.get("found"):
                response = {
                    "request_id": request.request_id,
                    "symbol": reconciled.get("symbol", DEFAULT_SYMBOL),
                    "side": side,
                    "volume": reconciled.get("volume"),
                    "order_ticket": reconciled.get("order_ticket"),
                    "deal_ticket": reconciled.get("deal_ticket"),
                    "position_ticket": reconciled.get("ticket"),
                    "position_id": reconciled.get("position_id"),
                    "account_login": getattr(mt5.account_info(), "login", None),
                    "demo_account": not live_mode,
                    "execution_accepted": True,
                    "reconciled": True,
                    "reconciliation_source": reconciled.get("source"),
                }
                save_execution_request(request_key, "CONFIRMED", previous["comment_tag"], response)
                return {"duplicate": True, "state": "CONFIRMED", "result": response}
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "This request exists but its outcome is not confirmed. It was not resubmitted.",
                    "request_id": request.request_id,
                    "state": previous["state"],
                    "action": "Check MT5 positions and history before any manual retry.",
                },
            )

        account = require_live_trading() if live_mode else require_demo_trading()
        volume = LIVE_VOLUME if live_mode else DEMO_VOLUME

        info = mt5.symbol_info(DEFAULT_SYMBOL)
        if info is None:
            raise HTTPException(status_code=404, detail="Gold symbol not found")

        if info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED:
            raise HTTPException(status_code=403, detail="Gold trading is disabled")

        if not info.visible and not mt5.symbol_select(DEFAULT_SYMBOL, True):
            raise HTTPException(status_code=503, detail="Could not select gold symbol")

        if info.volume_min > volume or info.volume_step > volume or abs((volume / info.volume_step) - round(volume / info.volume_step)) > 1e-8:
            raise HTTPException(
                status_code=400,
                detail=f"Broker symbol volume constraints do not support the configured {volume}-lot {'live' if live_mode else 'demo'} volume",
            )

        tick = mt5.symbol_info_tick(DEFAULT_SYMBOL)
        if tick is None or tick.bid <= 0 or tick.ask <= 0:
            raise HTTPException(status_code=503, detail="No valid gold quote")

        price = tick.ask if side == "BUY" else tick.bid
        sl = round(request.stop_loss, info.digits)
        tp = round(request.take_profit, info.digits)

        if side == "BUY" and not (sl < tick.bid and tp > tick.ask):
            raise HTTPException(
                status_code=422,
                detail="For BUY: stop_loss must be below bid and take_profit above ask",
            )

        if side == "SELL" and not (sl > tick.ask and tp < tick.bid):
            raise HTTPException(
                status_code=422,
                detail="For SELL: stop_loss must be above ask and take_profit below bid",
            )

        min_distance = info.trade_stops_level * info.point
        if min_distance > 0:
            if side == "BUY" and (
                tick.bid - sl < min_distance or tp - tick.ask < min_distance
            ):
                raise HTTPException(status_code=422, detail="Stops are too close to market price")
            if side == "SELL" and (
                sl - tick.ask < min_distance or tick.bid - tp < min_distance
            ):
                raise HTTPException(status_code=422, detail="Stops are too close to market price")

        filling = (
            mt5.ORDER_FILLING_IOC
            if info.filling_mode & 2
            else mt5.ORDER_FILLING_FOK
            if info.filling_mode & 1
            else mt5.ORDER_FILLING_RETURN
        )

        order_type = mt5.ORDER_TYPE_BUY if side == "BUY" else mt5.ORDER_TYPE_SELL

        request_data = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": DEFAULT_SYMBOL,
            "volume": volume,
            "type": order_type,
            "price": price,
            "sl": sl,
            "tp": tp,
            "deviation": 20,
            "magic": MAGIC,
            "comment": comment_tag,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }

        check = mt5.order_check(request_data)
        if check is None:
            raise HTTPException(
                status_code=502,
                detail=f"MT5 order check failed: {mt5.last_error()}",
            )

        if check.retcode != 0:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "MT5 rejected the pre-trade check",
                    "retcode": check.retcode,
                    "comment": check.comment,
                },
            )

        # Record intent before sending. A crash after this point blocks automatic resubmission.
        save_execution_request(request_key, "PENDING", comment_tag)
        result = mt5.order_send(request_data)
        if result is None:
            save_execution_request(request_key, "UNKNOWN", comment_tag)
            raise HTTPException(
                status_code=502,
                detail=f"No MT5 execution response: {mt5.last_error()}. Check MT5 before retrying.",
            )

        response = {
            "request_id": request.request_id,
            "symbol": DEFAULT_SYMBOL,
            "side": side,
            "volume": volume,
            "requested_price": price,
            "stop_loss": sl,
            "take_profit": tp,
            "retcode": result.retcode,
            "comment": result.comment,
            "order_ticket": result.order,
            "deal_ticket": result.deal,
            "account_login": account.login,
            "server": account.server,
            "demo_account": not live_mode,
            "execution_accepted": result.retcode in (
                mt5.TRADE_RETCODE_DONE,
                mt5.TRADE_RETCODE_DONE_PARTIAL,
            ),
        }

        # Reconcile the accepted order to its stable MT5 position identifier if it is already visible.
        if response["execution_accepted"]:
            reconciled = find_mt5_execution_by_comment(comment_tag)
            if reconciled.get("found"):
                response["position_ticket"] = reconciled.get("ticket")
                response["position_id"] = reconciled.get("position_id")
                response["reconciled"] = True
        # Persist the final result so process restarts do not erase idempotency history.
        state = "CONFIRMED" if response["execution_accepted"] else "REJECTED"
        save_execution_request(request_key, state, comment_tag, response)
        return response



@app.get("/history/deals", dependencies=[Depends(authorize)])
def get_deal_history(position_id: int | None = None, days: int = 30, limit: int = 2000):
    """Return MT5 deal history, optionally restricted to a position identifier."""
    require_account()
    days = max(1, min(days, 90))
    limit = max(1, min(limit, 5000))
    end = datetime.now()
    start = end - timedelta(days=days)
    deals = mt5.history_deals_get(start, end)
    if deals is None:
        raise HTTPException(status_code=503, detail=f"Could not retrieve deal history: {mt5.last_error()}")

    output = []
    for d in deals:
        pid = int(getattr(d, "position_id", 0))
        if position_id is not None and pid != position_id:
            continue
        output.append({
            "ticket": int(d.ticket),
            "order": int(getattr(d, "order", 0)),
            "position_id": pid,
            "time": int(getattr(d, "time", 0)),
            "time_msc": int(getattr(d, "time_msc", 0)),
            "type": int(getattr(d, "type", -1)),
            "entry": int(getattr(d, "entry", -1)),
            "symbol": getattr(d, "symbol", ""),
            "volume": float(getattr(d, "volume", 0.0)),
            "price": float(getattr(d, "price", 0.0)),
            "commission": float(getattr(d, "commission", 0.0)),
            "swap": float(getattr(d, "swap", 0.0)),
            "profit": float(getattr(d, "profit", 0.0)),
            "fee": float(getattr(d, "fee", 0.0)),
            "reason": (
                "SL" if getattr(d, "reason", None) == getattr(mt5, "DEAL_REASON_SL", -101)
                else "TP" if getattr(d, "reason", None) == getattr(mt5, "DEAL_REASON_TP", -102)
                else str(getattr(d, "reason", ""))
            ),
            "magic": int(getattr(d, "magic", 0)),
            "comment": getattr(d, "comment", ""),
        })
    output.sort(key=lambda item: (item["time_msc"], item["ticket"]))
    return {"count": min(len(output), limit), "days": days, "deals": output[-limit:]}


@app.get("/history/positions/{position_id}", dependencies=[Depends(authorize)])
def get_position_history(position_id: int, days: int = 90):
    """Return deal records and realised results for one MT5 position identifier."""
    result = get_deal_history(position_id=position_id, days=days, limit=5000)
    deals = result["deals"]
    if not deals:
        raise HTTPException(status_code=404, detail="No history found for this position identifier")
    realised = sum(float(d.get("profit", 0)) for d in deals)
    costs = sum(float(d.get("commission", 0)) + float(d.get("swap", 0)) + float(d.get("fee", 0)) for d in deals)
    closing_entries = {
        getattr(mt5, "DEAL_ENTRY_OUT", 1),
        getattr(mt5, "DEAL_ENTRY_OUT_BY", 3),
        getattr(mt5, "DEAL_ENTRY_INOUT", 2),
    }
    closing_deals = [d for d in deals if d["entry"] in closing_entries]
    return {
        "position_id": position_id,
        "closed": bool(closing_deals),
        "close_time": closing_deals[-1]["time"] if closing_deals else None,
        "realised_profit": realised,
        "commissions_swap_fees": costs,
        "net_profit_after_costs": realised + costs,
        "deals": deals,
    }


@app.get("/")
def root():
    return {
        "service": "Gold Robot Execution API",
        "mode": "live enabled" if ENABLE_LIVE_TRADING else "demo-only" if ENABLE_DEMO_TRADING else "read-only",
        "docs": "/docs",
        "order_execution_enabled": ENABLE_DEMO_TRADING or ENABLE_LIVE_TRADING,
        "demo_account_only": not ENABLE_LIVE_TRADING,
        "live_trading_enabled": ENABLE_LIVE_TRADING,
    }