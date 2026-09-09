"""Binance USD-M Futures Position Protection (hardened edition).

Design goals
============
* Normal exits only use CLOSED 1h candles.
* MA7/MA26 are partial reductions from a risk-cycle base position.
* MA83 is a full exit only after re-validation.
* Manual adds create a new risk cycle and are immune only until the CURRENT 1h candle closes.
* Emergency exits require ATR abnormality + structure break + independent REST price confirmation.
* Exchange STOP_MARKET is a distant disaster backstop, never a moving MA stop.
* All REST traffic goes through one rate/cooldown/circuit-breaker layer.
* One action lock per symbol+side prevents duplicate concurrent orders.
* State is persisted and reconciled on restart.

The module intentionally manages only algo orders whose clientAlgoId starts with
CLIENT_ALGO_PREFIX; manually-created exchange orders are never cancelled.
"""

import asyncio
import inspect
import json
import math
import os
import random
import time
import uuid
from collections import defaultdict
from pathlib import Path

import ccxt.pro as ccxtpro
import pandas as pd
from loguru import logger

from app.services.notification_service import send_alert

STATE_FILE = Path("position_protection_state.json")
CONFIG_FILE = Path("config/config.json")
if not CONFIG_FILE.exists():
    CONFIG_FILE = Path("config.json")
CLIENT_ALGO_PREFIX = "PM_WS_SL_"

DEFAULT_CONFIG = {
    "position_protection": {
        "enabled": True,
        "stop_mode": "moving_averages",
        "timeframe": "1h",
        "n1_bars": 7,
        "n2_bars": 26,
        "n3_bars": 83,
        "tier1_ratio": 0.20,
        "tier2_ratio": 0.30,
        "ma_atr_period": 14,
        "ma_atr_buffer_multiplier": 0.30,
        "recovery_atr_buffer_multiplier": 0.30,
        "auto_reset_state_on_add": True,
        "add_immunity_mode": "until_current_candle_close",

        "emergency_timeframe": "15m",
        "emergency_atr_period": 14,
        "emergency_atr_multiplier": 3.0,
        "emergency_structure_lookback": 4,
        "emergency_confirm_sources": True,
        "emergency_recheck_delay_sec": 0.35,

        "exchange_hard_stop_enabled": True,
        "hard_stop_timeframe": "1h",
        "hard_stop_atr_period": 14,
        "hard_stop_min_distance_pct": 0.05,
        "hard_stop_atr_multiplier": 6.0,

        "positions_cache_ttl_sec": 5.0,
        "ticker_cache_ttl_sec": 2.0,
        "orders_cache_ttl_sec": 5.0,
        "ohlcv_cache_ttl_sec": 3.0,
        "api_max_retries": 5,
        "api_failure_threshold": 5,
        "api_circuit_breaker_cooldown_sec": 30,
        "api_min_interval_sec": 0.06,
        "api_max_concurrency": 4,
        "api_429_base_backoff_sec": 2.0,
        "api_418_cooldown_sec": 60.0,

        "ws_reconnect_initial_sec": 1.0,
        "ws_reconnect_max_sec": 60.0,
        "ws_reconnect_jitter_pct": 0.20,
        "ws_stale_sec": 45.0,
        "safe_mode_enabled": True,
        "safe_mode_failures": 5,
    }
}


# ---------------------------------------------------------------------------
# Configuration / persistent state
# ---------------------------------------------------------------------------

def _load_config():
    if not CONFIG_FILE.exists():
        try:
            CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
            with CONFIG_FILE.open("w", encoding="utf-8") as f:
                json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            logger.error(f"无法创建默认配置: {exc}")
        return DEFAULT_CONFIG
    try:
        with CONFIG_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception as exc:
        logger.warning(f"读取配置失败，使用默认值: {exc}")
    return DEFAULT_CONFIG


def _f(value, default=None):
    try:
        return default if value is None else float(value)
    except (TypeError, ValueError):
        return default


def _load_state():
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning(f"读取状态文件失败: {exc}")
        return {}


def _save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, STATE_FILE)
    except Exception as exc:
        logger.error(f"保存状态失败: {exc}")
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _state_key(symbol, side):
    return f"{symbol}_{side}"


def _position_side(position):
    if not position:
        return None
    side = str(position.get("side") or "").lower()
    if side in ("long", "short"):
        return side
    info = position.get("info") or {}
    ps = str(info.get("positionSide") or "").upper()
    if ps == "LONG":
        return "long"
    if ps == "SHORT":
        return "short"
    amount = _f(info.get("positionAmt"), 0.0) or 0.0
    return "long" if amount > 0 else "short" if amount < 0 else None


def _position_size(position):
    if not position:
        return 0.0
    v = _f(position.get("contracts"))
    if v is not None:
        return abs(v)
    return abs(_f((position.get("info") or {}).get("positionAmt"), 0.0) or 0.0)


def _raw_position_side(position):
    ps = str((position.get("info") or {}).get("positionSide") or "BOTH").upper()
    return ps if ps in ("BOTH", "LONG", "SHORT") else "BOTH"


def _timeframe_ms(tf):
    unit = tf[-1].lower()
    n = int(tf[:-1])
    return n * {"m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]


def _current_candle_close_ms(timeframe, now_ms=None):
    now_ms = int(now_ms or time.time() * 1000)
    size = _timeframe_ms(timeframe)
    return ((now_ms // size) + 1) * size


def _algo_methods(exchange):
    needed = ("fapiPrivateGetOpenAlgoOrders", "fapiPrivatePostAlgoOrder", "fapiPrivateDeleteAlgoOrder")
    missing = [x for x in needed if not hasattr(exchange, x)]
    if missing:
        raise RuntimeError("当前 CCXT 版本缺少 Binance Algo Order 接口: " + ", ".join(missing))


# ---------------------------------------------------------------------------
# Global REST protection, cache and action locks
# ---------------------------------------------------------------------------

class RestGuardian:
    """Shared REST governor.

    It intentionally serializes request start times enough to avoid burst limits,
    while a semaphore still allows requests to overlap in network I/O.  Critical
    requests are not blocked by circuit-breaker state, but still respect exchange
    cooldowns after an explicit 429/418 response.
    """

    def __init__(self):
        self.start_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(4)
        self.next_start = 0.0
        self.cooldown_until = 0.0
        self.failures = 0
        self.circuit_open_until = 0.0
        self.safe_mode = False

    def configure(self, conf):
        max_concurrency = max(1, int(conf.get("api_max_concurrency", 4)))
        # Replacing only when unlocked keeps implementation deterministic.
        if getattr(self.semaphore, "_value", max_concurrency) > max_concurrency:
            self.semaphore = asyncio.Semaphore(max_concurrency)

    async def call(self, func, *args, priority="NORMAL", conf=None, **kwargs):
        conf = conf or {}
        priority = priority.upper()
        retries = max(1, int(conf.get("api_max_retries", 5)))
        min_interval = max(0.0, float(conf.get("api_min_interval_sec", 0.06)))
        failure_threshold = max(1, int(conf.get("api_failure_threshold", 5)))
        circuit_cooldown = max(1.0, float(conf.get("api_circuit_breaker_cooldown_sec", 30)))
        backoff_base = max(0.2, float(conf.get("api_429_base_backoff_sec", 2)))
        now = time.monotonic()

        if priority != "HIGH" and now < self.circuit_open_until:
            raise RuntimeError("REST circuit breaker OPEN; non-critical request suppressed")
        if priority != "HIGH" and now < self.cooldown_until:
            raise RuntimeError("REST cooldown active; non-critical request suppressed")

        last_exc = None
        for attempt in range(retries):
            try:
                now = time.monotonic()
                if now < self.cooldown_until:
                    await asyncio.sleep(self.cooldown_until - now)

                async with self.start_lock:
                    now = time.monotonic()
                    if now < self.next_start:
                        await asyncio.sleep(self.next_start - now)
                    self.next_start = time.monotonic() + min_interval

                async with self.semaphore:
                    result = func(*args, **kwargs)
                    if inspect.isawaitable(result):
                        result = await result

                self.failures = 0
                if self.safe_mode:
                    self.safe_mode = False
                    logger.success("REST 通信恢复，退出 SAFE_MODE")
                return result

            except Exception as exc:
                last_exc = exc
                text = str(exc).lower()
                self.failures += 1
                is_429 = "429" in text or "too many requests" in text
                is_418 = "418" in text or "ip banned" in text
                if is_418:
                    self.cooldown_until = time.monotonic() + float(conf.get("api_418_cooldown_sec", 60))
                elif is_429:
                    self.cooldown_until = max(self.cooldown_until, time.monotonic() + backoff_base * (2 ** min(attempt, 4)))

                if self.failures >= failure_threshold:
                    self.circuit_open_until = time.monotonic() + circuit_cooldown
                    self.safe_mode = bool(conf.get("safe_mode_enabled", True))
                    logger.warning(f"REST 连续失败 {self.failures} 次，Circuit OPEN {circuit_cooldown}s")

                if attempt + 1 >= retries:
                    break
                delay = min(30.0, backoff_base * (2 ** attempt)) * random.uniform(0.8, 1.2)
                logger.warning(f"REST 请求失败，第 {attempt + 1}/{retries} 次重试，{delay:.2f}s 后: {exc}")
                await asyncio.sleep(delay)
        raise last_exc


class Runtime:
    def __init__(self):
        self.rest = RestGuardian()
        self.action_locks = {}
        self.positions_cache = (0.0, None)
        self.ticker_cache = {}
        self.orders_cache = {}
        self.ohlcv_cache = {}
        self.ws_last_update = {}
        self.ws_active = set()

    def action_lock(self, symbol, side):
        key = (symbol, side)
        if key not in self.action_locks:
            self.action_locks[key] = asyncio.Lock()
        return self.action_locks[key]


RUNTIME = Runtime()


async def _rest(exchange, method_name, *args, priority="NORMAL", conf=None, **kwargs):
    return await RUNTIME.rest.call(getattr(exchange, method_name), *args, priority=priority, conf=conf, **kwargs)


async def _fetch_positions(exchange, conf, symbols=None, priority="NORMAL", force=False):
    ttl = float(conf.get("positions_cache_ttl_sec", 5))
    now = time.monotonic()
    ts, cached = RUNTIME.positions_cache
    if not force and symbols is None and cached is not None and now - ts < ttl:
        return cached
    positions = await _rest(exchange, "fetch_positions", symbols or [], priority=priority, conf=conf)
    if symbols is None:
        RUNTIME.positions_cache = (time.monotonic(), positions)
    return positions


async def _fetch_ticker(exchange, symbol, conf, priority="NORMAL", force=False):
    ttl = float(conf.get("ticker_cache_ttl_sec", 2))
    now = time.monotonic()
    cached = RUNTIME.ticker_cache.get(symbol)
    if not force and cached and now - cached[0] < ttl:
        return cached[1]
    ticker = await _rest(exchange, "fetch_ticker", symbol, priority=priority, conf=conf)
    RUNTIME.ticker_cache[symbol] = (time.monotonic(), ticker)
    return ticker


async def _fetch_ohlcv(exchange, symbol, timeframe, limit, conf, priority="LOW", force=False):
    key = (symbol, timeframe, limit)
    ttl = float(conf.get("ohlcv_cache_ttl_sec", 3))
    now = time.monotonic()
    cached = RUNTIME.ohlcv_cache.get(key)
    if not force and cached and now - cached[0] < ttl:
        return cached[1]
    rows = await _rest(exchange, "fetch_ohlcv", symbol, timeframe, None, limit, priority=priority, conf=conf)
    RUNTIME.ohlcv_cache[key] = (time.monotonic(), rows)
    return rows


# ---------------------------------------------------------------------------
# Exchange algo orders
# ---------------------------------------------------------------------------

def _make_client_algo_id(symbol, side):
    compact = symbol.replace("/", "").replace(":", "")
    suffix = "L" if side == "long" else "S"
    return f"{CLIENT_ALGO_PREFIX}{compact}_{suffix}_{uuid.uuid4().hex[:8]}"[:36]


def _algo_status(order):
    return str(order.get("algoStatus") or order.get("status") or "").upper()


def _algo_trigger(order):
    return _f(order.get("triggerPrice") or order.get("stopPrice"))


def _is_program_stop(order, side, position_side, current_price=None):
    if not isinstance(order, dict):
        return False
    if not str(order.get("clientAlgoId") or "").startswith(CLIENT_ALGO_PREFIX):
        return False
    if str(order.get("orderType") or order.get("type") or "").upper() != "STOP_MARKET":
        return False
    status = _algo_status(order)
    if status and status not in ("NEW", "TRIGGER_PENDING", "UNTRIGGERED", ""):
        return False
    expected = "SELL" if side == "long" else "BUY"
    if str(order.get("side") or "").upper() != expected:
        return False
    ps = str(order.get("positionSide") or "BOTH").upper()
    if position_side in ("LONG", "SHORT") and ps != position_side:
        return False
    trigger = _algo_trigger(order)
    if trigger is None or current_price is None:
        return True
    return trigger < current_price if side == "long" else trigger > current_price


async def _get_open_algo_orders(exchange, symbol, conf, force=False):
    _algo_methods(exchange)
    ttl = float(conf.get("orders_cache_ttl_sec", 5))
    now = time.monotonic()
    cached = RUNTIME.orders_cache.get(symbol)
    if not force and cached and now - cached[0] < ttl:
        return cached[1]
    market_id = exchange.market(symbol)["id"]
    response = await RUNTIME.rest.call(exchange.fapiPrivateGetOpenAlgoOrders, {"symbol": market_id}, priority="NORMAL", conf=conf)
    orders = response.get("orders") or response.get("data") or [] if isinstance(response, dict) else (response or [])
    RUNTIME.orders_cache[symbol] = (time.monotonic(), orders)
    return orders


async def _find_stop_orders(exchange, symbol, side, position, current_price, conf, force=False):
    raw_ps = _raw_position_side(position)
    orders = await _get_open_algo_orders(exchange, symbol, conf, force=force)
    return [o for o in orders if _is_program_stop(o, side, raw_ps, current_price)]


async def _cancel_algo(exchange, symbol, algo_id, conf):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    result = await RUNTIME.rest.call(exchange.fapiPrivateDeleteAlgoOrder, {"symbol": market_id, "algoId": str(algo_id)}, priority="HIGH", conf=conf)
    RUNTIME.orders_cache.pop(symbol, None)
    return result


async def _cancel_stop_orders(exchange, symbol, stops, conf):
    for order in stops:
        algo_id = order.get("algoId") or order.get("id")
        if not algo_id:
            continue
        try:
            await _cancel_algo(exchange, symbol, algo_id, conf)
        except Exception as exc:
            logger.error(f"[{symbol}] 取消程序止损失败: {exc}")


async def _create_full_close_stop(exchange, symbol, position, side, stop_price, conf):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    params = {
        "algoType": "CONDITIONAL", "symbol": market_id,
        "side": "SELL" if side == "long" else "BUY",
        "type": "STOP_MARKET", "positionSide": _raw_position_side(position),
        "triggerPrice": exchange.price_to_precision(symbol, stop_price),
        "workingType": "MARK_PRICE", "closePosition": "true",
        "clientAlgoId": _make_client_algo_id(symbol, side),
    }
    result = await RUNTIME.rest.call(exchange.fapiPrivatePostAlgoOrder, params, priority="HIGH", conf=conf)
    RUNTIME.orders_cache.pop(symbol, None)
    logger.success(f"[{symbol}] 灾难兜底 STOP 已创建，触发价 {params['triggerPrice']}")
    return result


# ---------------------------------------------------------------------------
# Indicators / market validation
# ---------------------------------------------------------------------------

def _closed_df(rows):
    if not rows or len(rows) < 3:
        raise RuntimeError("K线数据不足")
    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    return df.iloc[:-1].copy()  # last row may still be forming


def _atr_from_df(df, period):
    if len(df) < period + 1:
        return None
    prev = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev).abs(),
        (df["low"] - prev).abs(),
    ], axis=1).max(axis=1)
    value = _f(tr.rolling(period).mean().iloc[-1])
    return value if value and value > 0 else None


async def _ma_levels(exchange, symbol, timeframe, n1, n2, n3, conf, force=False):
    limit = max(n3 + 25, 120)
    rows = await _fetch_ohlcv(exchange, symbol, timeframe, limit, conf, force=force)
    df = _closed_df(rows)
    if len(df) < n3:
        raise RuntimeError(f"{symbol} {timeframe} K线不足 {n3} 根")
    return (
        float(df["close"].rolling(n1).mean().iloc[-1]),
        float(df["close"].rolling(n2).mean().iloc[-1]),
        float(df["close"].rolling(n3).mean().iloc[-1]),
        int(df.iloc[-1]["timestamp"]),
        float(df.iloc[-1]["close"]),
        df,
    )


async def _closed_atr(exchange, symbol, timeframe, period, conf, force=False):
    rows = await _fetch_ohlcv(exchange, symbol, timeframe, max(period + 30, 60), conf, force=force)
    atr = _atr_from_df(_closed_df(rows), period)
    if not atr:
        raise RuntimeError(f"{symbol} {timeframe} ATR 无效")
    return float(atr)


async def _market_snapshot(exchange, symbol, conf, priority="NORMAL", force=False):
    ticker = await _fetch_ticker(exchange, symbol, conf, priority=priority, force=force)
    mark = _f(ticker.get("mark"))
    last = _f(ticker.get("last") or ticker.get("close"))
    price = mark or last
    if not price or price <= 0:
        raise RuntimeError(f"[{symbol}] 无有效 ticker/mark price")
    return {"price": price, "mark": mark or price, "last": last or price, "timestamp": time.time()}


async def _position_fresh(exchange, symbol, side, conf, priority="HIGH"):
    positions = await _fetch_positions(exchange, conf, [symbol], priority=priority, force=True)
    for p in positions:
        if _position_size(p) > 0 and _position_side(p) == side:
            return p
    return None


async def _two_source_price_ok(exchange, symbol, ws_price, side, conf):
    """REST mark/last must reasonably agree with WS-derived live price."""
    snap = await _market_snapshot(exchange, symbol, conf, priority="HIGH", force=True)
    rest_price = snap["mark"] or snap["price"]
    tolerance = max(0.003, float(conf.get("price_source_tolerance_pct", 0.01)))
    diff = abs(rest_price - ws_price) / max(rest_price, 1e-12)
    if diff > tolerance:
        logger.warning(f"[{symbol}] 行情双源不一致 WS={ws_price} REST={rest_price} diff={diff:.3%}")
        return False, snap
    return True, snap


async def _emergency_signal(exchange, symbol, side, conf, live_rows=None, force_rest=False):
    tf = str(conf.get("emergency_timeframe", "15m"))
    period = int(conf.get("emergency_atr_period", 14))
    mult = float(conf.get("emergency_atr_multiplier", 3.0))
    lookback = int(conf.get("emergency_structure_lookback", 4))
    limit = max(period + lookback + 15, 50)
    rows = live_rows or await _fetch_ohlcv(exchange, symbol, tf, limit, conf, priority="LOW", force=force_rest)
    if not rows or len(rows) < period + lookback + 2:
        return False, None
    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    closed = df.iloc[:-1].copy()
    atr = _atr_from_df(closed, period)
    if not atr:
        return False, None
    live = df.iloc[-1]
    ws_price = float(live["close"])
    threshold = atr * mult
    if side == "long":
        move = float(live["high"] - ws_price)
        structure = ws_price < float(closed["low"].iloc[-lookback:].min())
    else:
        move = float(ws_price - live["low"])
        structure = ws_price > float(closed["high"].iloc[-lookback:].max())
    return bool(move >= threshold and structure), {"ws_price": ws_price, "move": move, "threshold": threshold, "structure": structure}


def _calculate_disaster_stop(exchange, symbol, side, current_price, atr_value, conf):
    min_pct = max(0.0, float(conf.get("hard_stop_min_distance_pct", 0.05)))
    atr_mult = max(0.0, float(conf.get("hard_stop_atr_multiplier", 6.0)))
    distance = max(min_pct, (atr_value * atr_mult / current_price) if atr_value else 0.0)
    stop = current_price * (1.0 - distance if side == "long" else 1.0 + distance)
    return float(exchange.price_to_precision(symbol, stop)), distance


# ---------------------------------------------------------------------------
# Transactional order execution
# ---------------------------------------------------------------------------

async def _market_reduce_and_confirm(exchange, symbol, side, position, requested_qty, reason, conf, expected_version, pos_state):
    raw_ps = _raw_position_side(position)
    before = _position_size(position)
    if int(pos_state.get("position_version", 0)) != expected_version:
        raise RuntimeError("仓位版本已变化，拒绝按旧快照下单")
    qty = _f(exchange.amount_to_precision(symbol, min(before, requested_qty)), 0.0)
    if not qty or qty <= 0:
        raise RuntimeError(f"{reason}: 下单数量无效")

    params = {"reduceOnly": True}
    if raw_ps != "BOTH":
        params["positionSide"] = raw_ps

    order = await _rest(exchange, "create_market_order", symbol, "sell" if side == "long" else "buy", qty,
                        priority="HIGH", conf=conf, params=params)
    await asyncio.sleep(0.5)
    fresh = await _position_fresh(exchange, symbol, side, conf, priority="HIGH")
    after = _position_size(fresh)
    if after >= before - 1e-10:
        raise RuntimeError(f"{reason}: 下单后仓位未确认减少 ({before}->{after})")
    logger.success(f"[{symbol}] {reason} 已确认，仓位 {before} -> {after}")
    return order, fresh, after


async def _full_exit_with_revalidation(exchange, symbol, side, position, reason, conf, pos_state):
    """Second position + market validation immediately before a full exit."""
    expected_version = int(pos_state.get("position_version", 0))
    fresh = await _position_fresh(exchange, symbol, side, conf, priority="HIGH")
    if not fresh:
        return True, None
    if int(pos_state.get("position_version", 0)) != expected_version:
        raise RuntimeError("全平前仓位版本已变化，重新评估")
    before = _position_size(fresh)
    _, after_pos, after = await _market_reduce_and_confirm(
        exchange, symbol, side, fresh, before, reason, conf, expected_version, pos_state
    )
    return after <= 1e-10, after_pos


# ---------------------------------------------------------------------------
# Stop maintenance / state reconciliation
# ---------------------------------------------------------------------------

async def _ensure_disaster_stop(exchange, symbol, side, pos, current_price, conf, force_replace=False):
    if not bool(conf.get("exchange_hard_stop_enabled", True)):
        return
    atr = await _closed_atr(exchange, symbol, str(conf.get("hard_stop_timeframe", "1h")),
                            int(conf.get("hard_stop_atr_period", 14)), conf)
    stop, distance = _calculate_disaster_stop(exchange, symbol, side, current_price, atr, conf)
    stops = await _find_stop_orders(exchange, symbol, side, pos, current_price, conf, force=True)
    if stops and not force_replace:
        return
    if stops:
        await _cancel_stop_orders(exchange, symbol, stops, conf)
    await _create_full_close_stop(exchange, symbol, pos, side, stop, conf)
    logger.info(f"[{symbol}] 灾难 STOP 距离 {distance:.2%}")


def _reconcile_position_state(state, key, symbol, side, contracts, timeframe):
    ps = state.get(key)
    if not isinstance(ps, dict):
        ps = {
            "base_contracts": contracts,
            "contracts": contracts,
            "t1_done": False,
            "t2_done": False,
            "last_checked_time": 0,
            "position_version": 1,
            "risk_cycle_started_at": int(time.time()),
            "add_immunity_until_ms": 0,
        }
        state[key] = ps
        return ps, True
    old = _f(ps.get("contracts"), contracts) or contracts
    changed = not math.isclose(old, contracts, rel_tol=1e-8, abs_tol=1e-10)
    if changed:
        # Any externally observed size change invalidates stale action snapshots.
        ps["position_version"] = int(ps.get("position_version", 0)) + 1
        if contracts > old + max(1e-10, old * 1e-8):
            ps.update({
                "base_contracts": contracts,
                "contracts": contracts,
                "t1_done": False,
                "t2_done": False,
                "risk_cycle_started_at": int(time.time()),
                "add_immunity_until_ms": _current_candle_close_ms(timeframe),
            })
            logger.info(f"[{symbol}] 检测到手动/外部加仓 {old}->{contracts}，建立新风险周期，仅豁免至当前K线结束")
        else:
            ps["contracts"] = contracts
            logger.info(f"[{symbol}] 检测到外部减仓 {old}->{contracts}，保留已完成层级状态")
    return ps, changed


async def cleanup_orphaned_state_and_orders(exchange, conf):
    try:
        if not exchange.markets:
            await _rest(exchange, "load_markets", priority="LOW", conf=conf)
        positions = await _fetch_positions(exchange, conf, priority="NORMAL", force=True)
        active = {(p.get("symbol"), _position_side(p)) for p in positions if _position_size(p) > 0}
        state = _load_state()
        modified = False
        for key in list(state):
            if "_" not in key:
                state.pop(key, None); modified = True; continue
            symbol, side = key.rsplit("_", 1)
            if (symbol, side) not in active:
                state.pop(key, None); modified = True
        if modified:
            _save_state(state)

        # Only inspect active symbols and known state symbols.  Avoid scanning every market,
        # which is unnecessary REST load and a common source of rate-limit bursts.
        symbols = {s for s, _ in active}
        for key in state:
            symbols.add(key.rsplit("_", 1)[0])
        for symbol in symbols:
            for side in ("long", "short"):
                if (symbol, side) in active:
                    continue
                dummy = {"info": {"positionSide": "LONG" if side == "long" else "SHORT"}}
                try:
                    orders = await _find_stop_orders(exchange, symbol, side, dummy, None, conf, force=True)
                    if orders:
                        await _cancel_stop_orders(exchange, symbol, orders, conf)
                except Exception:
                    pass
    except Exception as exc:
        logger.error(f"清理孤儿状态/订单异常: {exc}")


# ---------------------------------------------------------------------------
# Per-position guardian
# ---------------------------------------------------------------------------

async def _next_ws_ohlcv(exchange, symbol, timeframe, conf):
    key = (symbol, timeframe)
    RUNTIME.ws_active.add(key)
    try:
        rows = await asyncio.wait_for(exchange.watch_ohlcv(symbol, timeframe), timeout=float(conf.get("ws_stale_sec", 45)))
        RUNTIME.ws_last_update[key] = time.time()
        return rows
    finally:
        # This only marks a single wait complete; the caller owns reconnection.
        RUNTIME.ws_active.discard(key)


async def watch_symbol_position(exchange, symbol):
    ws_delay = 0.0
    while True:
        full_config = _load_config()
        conf = full_config.get("position_protection", {})
        if not conf.get("enabled", True):
            await asyncio.sleep(5)
            continue
        timeframe = str(conf.get("timeframe", "1h"))
        try:
            if ws_delay:
                await asyncio.sleep(ws_delay)
            positions = await _fetch_positions(exchange, conf, [symbol], priority="NORMAL", force=True)
            pos = next((p for p in positions if _position_size(p) > 0 and _position_side(p) in ("long", "short")), None)
            if not pos:
                # Remove both possible state keys and only our own orphan STOPs.
                state = _load_state()
                for side0 in ("long", "short"):
                    state.pop(_state_key(symbol, side0), None)
                    dummy = {"info": {"positionSide": "LONG" if side0 == "long" else "SHORT"}}
                    try:
                        orders = await _find_stop_orders(exchange, symbol, side0, dummy, None, conf, force=True)
                        await _cancel_stop_orders(exchange, symbol, orders, conf)
                    except Exception:
                        pass
                _save_state(state)
                await asyncio.sleep(5)
                continue

            side = _position_side(pos)
            contracts = _position_size(pos)
            key = _state_key(symbol, side)
            state = _load_state()
            pos_state, changed = _reconcile_position_state(state, key, symbol, side, contracts, timeframe)
            _save_state(state)

            async with RUNTIME.action_lock(symbol, side):
                # Snapshot is shared by STOP maintenance and emergency validation.
                snap = await _market_snapshot(exchange, symbol, conf, priority="NORMAL")
                await _ensure_disaster_stop(exchange, symbol, side, pos, snap["mark"], conf, force_replace=changed)

            # WebSocket primary path.  If stale/failed, exponential reconnect with jitter.
            try:
                ohlcvs = await _next_ws_ohlcv(exchange, symbol, timeframe, conf)
                ws_delay = 0.0
            except Exception as ws_exc:
                initial = float(conf.get("ws_reconnect_initial_sec", 1))
                maximum = float(conf.get("ws_reconnect_max_sec", 60))
                jitter = float(conf.get("ws_reconnect_jitter_pct", 0.20))
                ws_delay = min(maximum, max(initial, ws_delay * 2 if ws_delay else initial))
                ws_delay *= random.uniform(max(0.0, 1 - jitter), 1 + jitter)
                logger.warning(f"[{symbol}] WS 断开/超时: {ws_exc}; {ws_delay:.2f}s 后重连，并使用 REST 校验")
                # REST fallback keeps closed-candle protection alive without busy looping.
                ohlcvs = await _fetch_ohlcv(exchange, symbol, timeframe, max(int(conf.get("n3_bars", 83)) + 25, 120), conf, force=True)

            # Re-check actual position after potentially long WS wait.
            pos = await _position_fresh(exchange, symbol, side, conf, priority="NORMAL")
            if not pos:
                continue
            contracts = _position_size(pos)
            state = _load_state()
            pos_state, changed2 = _reconcile_position_state(state, key, symbol, side, contracts, timeframe)
            if changed2:
                _save_state(state)
                continue

            # ---------------- Emergency: ATR + structure + two-source + second check ----------------
            em_rows = None
            try:
                emergency, details = await _emergency_signal(exchange, symbol, side, conf)
                if emergency:
                    ok, _ = await _two_source_price_ok(exchange, symbol, details["ws_price"], side, conf)
                    if ok:
                        await asyncio.sleep(float(conf.get("emergency_recheck_delay_sec", 0.35)))
                        emergency2, details2 = await _emergency_signal(exchange, symbol, side, conf, force_rest=True)
                        ok2, _ = await _two_source_price_ok(exchange, symbol, details2["ws_price"], side, conf) if emergency2 else (False, None)
                        if emergency2 and ok2:
                            async with RUNTIME.action_lock(symbol, side):
                                state = _load_state(); pos_state = state.get(key, pos_state)
                                fresh = await _position_fresh(exchange, symbol, side, conf, priority="HIGH")
                                if fresh:
                                    msg = f"[{symbol}] Emergency 确认：异常波动 {details2['move']:.6f} >= 阈值 {details2['threshold']:.6f} 且结构破位，双源二次确认通过，全平。"
                                    logger.warning(msg); send_alert(full_config, "风控警告: Emergency 黑天鹅熔断", msg, symbol=symbol)
                                    success, _ = await _full_exit_with_revalidation(exchange, symbol, side, fresh, "Emergency 全平", conf, pos_state)
                                    if success:
                                        current = details2["ws_price"]
                                        stops = await _find_stop_orders(exchange, symbol, side, fresh, current, conf, force=True)
                                        await _cancel_stop_orders(exchange, symbol, stops, conf)
                                        state.pop(key, None); _save_state(state)
                                        return
            except Exception as exc:
                logger.warning(f"[{symbol}] Emergency 检查异常，保持仓位并等待下一次确认: {exc}")

            # ---------------- Normal MA: CLOSED candle only ----------------
            try:
                n1, n2, n3 = int(conf.get("n1_bars", 7)), int(conf.get("n2_bars", 26)), int(conf.get("n3_bars", 83))
                t1, t2, t3, closed_ts, closed_price, _ = await _ma_levels(exchange, symbol, timeframe, n1, n2, n3, conf)
                last_ts = int(pos_state.get("last_checked_time", 0))
                if closed_ts <= last_ts:
                    continue

                atr = await _closed_atr(exchange, symbol, timeframe, int(conf.get("ma_atr_period", 14)), conf)
                br = atr * float(conf.get("ma_atr_buffer_multiplier", 0.30))
                rec = atr * float(conf.get("recovery_atr_buffer_multiplier", 0.30))
                immunity = int(time.time() * 1000) < int(pos_state.get("add_immunity_until_ms", 0))

                if side == "long":
                    t1_hit, t2_hit, t3_hit = closed_price <= t1 - br, closed_price <= t2 - br, closed_price <= t3 - br
                    if closed_price > t1 + rec: pos_state["t1_rearmed"] = True
                    if closed_price > t2 + rec: pos_state["t2_rearmed"] = True
                else:
                    t1_hit, t2_hit, t3_hit = closed_price >= t1 + br, closed_price >= t2 + br, closed_price >= t3 + br
                    if closed_price < t1 - rec: pos_state["t1_rearmed"] = True
                    if closed_price < t2 - rec: pos_state["t2_rearmed"] = True

                # The current candle is now consumed regardless of whether an order is allowed.
                pos_state["last_checked_time"] = closed_ts
                state[key] = pos_state
                _save_state(state)

                if immunity:
                    logger.info(f"[{symbol}] 加仓豁免有效至当前 1H K线结束，跳过本次 MA 动作")
                    continue

                async with RUNTIME.action_lock(symbol, side):
                    # Refresh state and position inside the lock to prevent duplicate/stale actions.
                    state = _load_state(); pos_state = state.get(key, pos_state)
                    fresh = await _position_fresh(exchange, symbol, side, conf, priority="HIGH")
                    if not fresh:
                        continue
                    current_contracts = _position_size(fresh)
                    version = int(pos_state.get("position_version", 0))

                    if t3_hit:
                        # Full exit has the strongest pre-trade validation: fresh position + fresh MA data.
                        _, _, _, check_ts, check_close, _ = await _ma_levels(exchange, symbol, timeframe, n1, n2, n3, conf, force=True)
                        if check_ts != closed_ts:
                            logger.info(f"[{symbol}] T3 重新验证时出现新K线，放弃旧信号等待下一轮")
                            continue
                        check_atr = await _closed_atr(exchange, symbol, timeframe, int(conf.get("ma_atr_period", 14)), conf, force=True)
                        valid = check_close <= t3 - check_atr * float(conf.get("ma_atr_buffer_multiplier", 0.30)) if side == "long" else check_close >= t3 + check_atr * float(conf.get("ma_atr_buffer_multiplier", 0.30))
                        if valid:
                            msg = f"[{symbol}] T3 {n3}MA 已收盘有效破位并二次确认，全平剩余仓位。"
                            logger.warning(msg); send_alert(full_config, "风控警告: T3 趋势失效", msg, symbol=symbol)
                            success, _ = await _full_exit_with_revalidation(exchange, symbol, side, fresh, "T3 全平", conf, pos_state)
                            if success:
                                snap = await _market_snapshot(exchange, symbol, conf, priority="HIGH", force=True)
                                stops = await _find_stop_orders(exchange, symbol, side, fresh, snap["mark"], conf, force=True)
                                await _cancel_stop_orders(exchange, symbol, stops, conf)
                                state.pop(key, None); _save_state(state)
                                return

                    elif t2_hit and not bool(pos_state.get("t2_done", False)):
                        qty = min(current_contracts, (_f(pos_state.get("base_contracts"), current_contracts) or current_contracts) * float(conf.get("tier2_ratio", 0.30)))
                        msg = f"[{symbol}] T2 {n2}MA 已收盘有效破位，按风险周期基准减仓 {qty}。"
                        logger.warning(msg); send_alert(full_config, "风控提示: T2", msg, symbol=symbol)
                        _, _, after = await _market_reduce_and_confirm(exchange, symbol, side, fresh, qty, "T2 减仓", conf, version, pos_state)
                        pos_state["contracts"] = after; pos_state["t2_done"] = True
                        state[key] = pos_state; _save_state(state)

                    elif t1_hit and not bool(pos_state.get("t1_done", False)):
                        qty = min(current_contracts, (_f(pos_state.get("base_contracts"), current_contracts) or current_contracts) * float(conf.get("tier1_ratio", 0.20)))
                        msg = f"[{symbol}] T1 {n1}MA 已收盘有效破位，按风险周期基准减仓 {qty}。"
                        logger.warning(msg); send_alert(full_config, "风控提示: T1", msg, symbol=symbol)
                        _, _, after = await _market_reduce_and_confirm(exchange, symbol, side, fresh, qty, "T1 减仓", conf, version, pos_state)
                        pos_state["contracts"] = after; pos_state["t1_done"] = True
                        state[key] = pos_state; _save_state(state)

            except Exception as exc:
                logger.warning(f"[{symbol}] 正常 MA 风控检查异常，不根据不完整数据交易: {exc}")

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"[{symbol}] 风控守护异常: {exc}", exc_info=True)
            await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Main coordinator
# ---------------------------------------------------------------------------

async def protect_positions_main(exchange, config=None):
    full_config = _load_config()
    conf = full_config.get("position_protection", {})
    if not conf.get("enabled", True):
        logger.info("仓位保护已禁用")
        return
    _algo_methods(exchange)
    logger.info("启动加固版仓位风控：Closed-K MA + ATR Buffer + Emergency 双源确认 + 灾难 STOP + REST/WS 限流保护")
    await cleanup_orphaned_state_and_orders(exchange, conf)

    tasks = {}
    last_cleanup = 0.0
    while True:
        try:
            full_config = _load_config(); conf = full_config.get("position_protection", {})
            positions = await _fetch_positions(exchange, conf, priority="NORMAL", force=True)
            active = set()
            for pos in positions:
                if _position_size(pos) <= 0 or _position_side(pos) not in ("long", "short"):
                    continue
                symbol = pos.get("symbol")
                if not symbol:
                    continue
                active.add(symbol)
                task = tasks.get(symbol)
                if task is None or task.done():
                    logger.info(f"[{symbol}] 启动独立风控守护协程")
                    tasks[symbol] = asyncio.create_task(watch_symbol_position(exchange, symbol), name=f"position-protect:{symbol}")

            for symbol, task in list(tasks.items()):
                if symbol not in active and task.done():
                    tasks.pop(symbol, None)

            if time.time() - last_cleanup >= 3600:
                await cleanup_orphaned_state_and_orders(exchange, conf)
                last_cleanup = time.time()

            await asyncio.sleep(max(3.0, float(conf.get("positions_cache_ttl_sec", 5))))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"风控主控循环异常: {exc}", exc_info=True)
            await asyncio.sleep(10)
