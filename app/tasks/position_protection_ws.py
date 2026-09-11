"""Binance USD-M Futures Position Protection (Rate-Limit Elimination & Visible Alert Edition).

Design goals
============
* Eliminate 429/418 Rate Limits: High-TTL OHLCV caching + Global batch Ticker fetching.
* Explicit & Transparent Rate-Limit Logging: Clear WARNINGs for 429s and Circuit Breaker without raw stack trace floods.
* Multi-Symbol Scale: Optimized for 50+ concurrent position monitoring without weight exhaustion.
"""

import asyncio
import inspect
import json
import math
import os
import random
import time
import uuid
import sys
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
        "ticker_cache_ttl_sec": 3.0,
        "orders_cache_ttl_sec": 10.0,
        "ohlcv_cache_ttl_sec": 30.0,  # <--- 重要：K线缓存提升至 30秒，大幅降低 API 权重消耗！
        "api_max_retries": 5,
        "api_failure_threshold": 5,
        "api_circuit_breaker_cooldown_sec": 30,
        "api_min_interval_sec": 0.08,
        "api_max_concurrency": 3,
        "api_429_base_backoff_sec": 2.0,
        "api_418_cooldown_sec": 60.0,
        "position_watch_interval_sec": 5.0,
        "stop_maintenance_interval_sec": 60.0,

        "safe_mode_enabled": True,
        "safe_mode_failures": 5,
    }
}


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
    try:
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
    except Exception as exc:
        logger.error(f"保存状态失败: {exc}")


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

    if amount > 0:
        return "long"
    elif amount < 0:
        return "short"
    else:
        return None


def _position_size(position):
    if not position:
        return 0.0
    v = _f(position.get("contracts"))
    if v is not None:
        return abs(v)
    return abs(_f((position.get("info") or {}).get("positionAmt"), 0.0) or 0.0)


def _raw_position_side(position):
    ps = str((position.get("info") or {}).get("positionSide") or "BOTH").upper()
    if ps in ("BOTH", "LONG", "SHORT"):
        return ps
    return "BOTH"


def _timeframe_ms(tf):
    unit = tf[-1].lower()
    n = int(tf[:-1])
    multiplier = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    return n * multiplier[unit]


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

class CircuitBreakerException(Exception):
    """自定义断路器开启异常"""
    pass


class RestGuardian:
    def __init__(self):
        self.start_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(3)
        self.next_start = 0.0
        self.cooldown_until = 0.0
        self.failures = 0
        self.circuit_open_until = 0.0
        self.last_circuit_log_time = 0.0

    async def call(self, func, *args, priority="NORMAL", conf=None, **kwargs):
        conf = conf or {}
        priority = priority.upper()
        retries = max(1, int(conf.get("api_max_retries", 5)))
        min_interval = max(0.0, float(conf.get("api_min_interval_sec", 0.08)))
        failure_threshold = max(1, int(conf.get("api_failure_threshold", 5)))
        circuit_cooldown = max(1.0, float(conf.get("api_circuit_breaker_cooldown_sec", 30)))
        backoff_base = max(0.2, float(conf.get("api_429_base_backoff_sec", 2)))
        now = time.monotonic()

        # 断路器开启时的显式检查与日志
        if priority != "HIGH" and now < self.circuit_open_until:
            rem = round(self.circuit_open_until - now, 1)
            raise CircuitBreakerException(f"REST 断路器处于 OPEN 保护状态，剩余冷却时间: {rem} 秒")

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
                return result

            except Exception as exc:
                last_exc = exc
                text = str(exc).lower()
                self.failures += 1

                is_429 = "429" in text or "too many requests" in text
                is_418 = "418" in text or "ip banned" in text

                # 显式打印限流/封禁日志！
                if is_418:
                    cool_sec = float(conf.get("api_418_cooldown_sec", 60))
                    self.cooldown_until = time.monotonic() + cool_sec
                    logger.error(f"🚨 [API 严重警告] 触发 HTTP 418 IP 封禁警告！强制休眠 {cool_sec} 秒！")
                elif is_429:
                    backoff = backoff_base * (2 ** min(attempt, 4))
                    self.cooldown_until = max(self.cooldown_until, time.monotonic() + backoff)
                    logger.warning(f"⚠️ [API 限流警报] 触发 HTTP 429 请求过快！(尝试 {attempt+1}/{retries})，退避休眠 {backoff:.1f} 秒...")

                if self.failures >= failure_threshold:
                    self.circuit_open_until = time.monotonic() + circuit_cooldown
                    if time.monotonic() - self.last_circuit_log_time > 10:
                        logger.error(f"🚨 [断路器熔断生效] REST 接口连续失败 {self.failures} 次，已开启 {circuit_cooldown} 秒熔断保护机制！")
                        self.last_circuit_log_time = time.monotonic()

                if attempt + 1 >= retries:
                    break

                delay = min(30.0, backoff_base * (2 ** attempt)) * random.uniform(0.8, 1.2)
                await asyncio.sleep(delay)

        raise last_exc


class Runtime:
    def __init__(self):
        self.rest = RestGuardian()
        self.action_locks = {}
        self.positions_cache = (0.0, None)
        self.global_positions_map = {}
        self.ticker_cache = {}
        self.orders_cache = {}
        self.ohlcv_cache = {}
        self.last_circuit_notice = 0.0

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

    target_symbols = None if not symbols else symbols
    positions = await _rest(exchange, "fetch_positions", target_symbols, priority=priority, conf=conf)

    if symbols is None:
        RUNTIME.positions_cache = (time.monotonic(), positions)
    return positions


async def _fetch_ticker(exchange, symbol, conf, priority="NORMAL", force=False):
    ttl = float(conf.get("ticker_cache_ttl_sec", 3))
    now = time.monotonic()
    cached = RUNTIME.ticker_cache.get(symbol)
    if not force and cached and now - cached[0] < ttl:
        return cached[1]

    ticker = await _rest(exchange, "fetch_ticker", symbol, priority=priority, conf=conf)
    RUNTIME.ticker_cache[symbol] = (time.monotonic(), ticker)
    return ticker


async def _fetch_ohlcv(exchange, symbol, timeframe, limit, conf, priority="LOW", force=False):
    key = (symbol, timeframe, limit)
    # 【核心优化】使用 30s 的 OHLCV 高缓存，彻底杜绝 1h K线引起的 429 限流
    ttl = float(conf.get("ohlcv_cache_ttl_sec", 30.0))
    now = time.monotonic()
    cached = RUNTIME.ohlcv_cache.get(key)
    if not force and cached and now - cached[0] < ttl:
        return cached[1]

    rows = await _rest(exchange, "fetch_ohlcv", symbol, timeframe, None, limit, priority=priority, conf=conf)
    RUNTIME.ohlcv_cache[key] = (time.monotonic(), rows)
    return rows


# ---------------------------------------------------------------------------
# Exchange algo orders & Market Functions
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

    client_id = str(order.get("clientAlgoId") or order.get("clientOrderId") or "")
    if not client_id.startswith(CLIENT_ALGO_PREFIX):
        return False

    type_upper = str(order.get("orderType") or order.get("type") or "").upper()
    if type_upper != "STOP_MARKET":
        return False

    status = _algo_status(order)
    if status and status not in ("NEW", "TRIGGER_PENDING", "UNTRIGGERED", "PARTIALLY_FILLED", ""):
        return False

    expected = "SELL" if side == "long" else "BUY"
    if str(order.get("side") or "").upper() != expected:
        return False

    return True


async def _get_open_algo_orders(exchange, symbol, conf, force=False):
    _algo_methods(exchange)
    ttl = float(conf.get("orders_cache_ttl_sec", 10))
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
        except Exception:
            pass


async def _create_full_close_stop(exchange, symbol, position, side, stop_price, conf):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    trigger_str = exchange.price_to_precision(symbol, stop_price)
    params = {
        "algoType": "CONDITIONAL", "symbol": market_id,
        "side": "SELL" if side == "long" else "BUY",
        "type": "STOP_MARKET", "positionSide": _raw_position_side(position),
        "triggerPrice": trigger_str,
        "workingType": "MARK_PRICE", "closePosition": "true",
        "clientAlgoId": _make_client_algo_id(symbol, side),
    }
    result = await RUNTIME.rest.call(exchange.fapiPrivatePostAlgoOrder, params, priority="HIGH", conf=conf)
    RUNTIME.orders_cache.pop(symbol, None)

    logger.success(f"[{symbol}] 🎯 成功下达 灾难STOP兜底单，触发价: {trigger_str}")
    return result


# ---------------------------------------------------------------------------
# Indicators & Market signals
# ---------------------------------------------------------------------------

def _closed_df(rows):
    if not rows or len(rows) < 3:
        raise RuntimeError("K线数据不足")
    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    return df.iloc[:-1].copy()


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


def _ma_levels_from_rows(rows, n1, n2, n3):
    df = _closed_df(rows)
    if len(df) < n3:
        raise RuntimeError(f"K线不足 {n3} 根")

    return (
        float(df["close"].rolling(n1).mean().iloc[-1]),
        float(df["close"].rolling(n2).mean().iloc[-1]),
        float(df["close"].rolling(n3).mean().iloc[-1]),
        int(df.iloc[-1]["timestamp"]),
        float(df.iloc[-1]["close"]),
        df,
    )


def _atr_from_rows(rows, period):
    atr = _atr_from_df(_closed_df(rows), period)
    if not atr:
        raise RuntimeError("ATR 无效")
    return float(atr)


async def _closed_atr(exchange, symbol, timeframe, period, conf, force=False):
    rows = await _fetch_ohlcv(exchange, symbol, timeframe, max(period + 30, 60), conf, force=force)
    return _atr_from_rows(rows, period)


async def _market_snapshot(exchange, symbol, conf, priority="NORMAL", force=False):
    ticker = await _fetch_ticker(exchange, symbol, conf, priority=priority, force=force)
    mark = _f(ticker.get("mark"))
    last = _f(ticker.get("last") or ticker.get("close"))
    price = mark or last
    if not price or price <= 0:
        raise RuntimeError(f"[{symbol}] 无有效 ticker/mark price")
    return {"price": price, "mark": mark or price, "last": last or price, "timestamp": time.time()}


async def _position_fresh(exchange, symbol, side, conf, priority="HIGH"):
    positions = await _fetch_positions(exchange, conf, symbols=[symbol], priority=priority, force=True)
    for p in positions:
        if _position_size(p) > 0 and _position_side(p) == side:
            return p
    return None


async def _two_source_price_ok(exchange, symbol, ws_price, side, conf):
    snap = await _market_snapshot(exchange, symbol, conf, priority="HIGH", force=True)
    rest_price = snap["mark"] or snap["price"]
    tolerance = max(0.003, float(conf.get("price_source_tolerance_pct", 0.01)))
    diff = abs(rest_price - ws_price) / max(rest_price, 1e-12)
    if diff > tolerance:
        logger.warning(f"[{symbol}] 行情双源不一致 WS={ws_price} REST={rest_price} diff={diff:.3%}")
        return False, snap
    return True, snap


async def _emergency_signal(exchange, symbol, side, conf, live_rows=None):
    tf = str(conf.get("emergency_timeframe", "15m"))
    period = int(conf.get("emergency_atr_period", 14))
    mult = float(conf.get("emergency_atr_multiplier", 3.0))
    volmult = float(conf.get("emergency_vol_multiplier", 1.0))
    lookback = int(conf.get("emergency_structure_lookback", 4))
    limit = max(period + lookback + 25, 60)

    rows = live_rows or await _fetch_ohlcv(exchange, symbol, tf, limit, conf, priority="LOW", force=False)
    if not rows or len(rows) < period + lookback + 2:
        return False, None

    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    closed = df.iloc[:-1].copy()
    atr = _atr_from_df(closed, period)
    if not atr:
        return False, None

    vol_ma = closed["volume"].rolling(20).mean().iloc[-1]

    live = df.iloc[-1]
    ws_price = float(live["close"])
    live_vol = float(live["volume"])
    threshold = atr * mult

    is_volume_spike = bool(vol_ma and vol_ma > 0 and live_vol > vol_ma * volmult)

    if side == "long":
        move = float(live["open"] - ws_price) if ws_price < live["open"] else 0.0
        structure = ws_price < float(closed["low"].iloc[-lookback:].min())
        volume_crash = is_volume_spike and move >= (atr * mult)
    else:
        move = float(ws_price - live["open"]) if ws_price > live["open"] else 0.0
        structure = ws_price > float(closed["high"].iloc[-lookback:].max())
        volume_crash = is_volume_spike and move >= (atr * mult)

    is_emergency = bool((move >= threshold and structure) or volume_crash)
    reason = "放量加速暴跌/拉升" if volume_crash else "结构性黑天鹅"

    return is_emergency, {
        "ws_price": ws_price, "move": move, "threshold": threshold,
        "structure": structure, "volume_crash": volume_crash, "reason": reason
    }


def _calculate_disaster_stop(exchange, symbol, side, current_price, atr_value, conf):
    min_pct = max(0.0, float(conf.get("hard_stop_min_distance_pct", 0.05)))
    atr_mult = max(0.0, float(conf.get("hard_stop_atr_multiplier", 6.0)))
    distance = max(min_pct, (atr_value * atr_mult / current_price) if atr_value else 0.0)
    stop = current_price * (1.0 - distance if side == "long" else 1.0 + distance)
    return float(exchange.price_to_precision(symbol, stop)), distance


# ---------------------------------------------------------------------------
# Execution Strategy
# ---------------------------------------------------------------------------

async def _market_reduce_and_confirm(exchange, symbol, side, position, requested_qty, reason, conf, expected_version, pos_state):
    """紧急止损：市价吃单 (Taker)"""
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

    logger.success(f"[{symbol}] 💰 {reason} (市价吃单) 执行成功！仓位从 {before} -> {after}")
    return order, fresh, after


async def _limit_reduce_with_maker_retry(
    exchange, symbol, side, position, requested_qty, reason, conf, expected_version, pos_state,
    max_retries=15, check_interval_sec=1.0, timeout_sec=10.0
):
    """非紧急止损：挂单追单 (Maker Limit)"""
    raw_ps = _raw_position_side(position)
    before = _position_size(position)
    if int(pos_state.get("position_version", 0)) != expected_version:
        raise RuntimeError("仓位版本已变化，拒绝按旧快照下单")

    qty = _f(exchange.amount_to_precision(symbol, min(before, requested_qty)), 0.0)
    if not qty or qty <= 0:
        raise RuntimeError(f"{reason}: 下单数量无效")

    order_side = "sell" if side == "long" else "buy"
    params = {"reduceOnly": True}
    if raw_ps != "BOTH":
        params["positionSide"] = raw_ps

    remaining_qty = qty

    for attempt in range(max_retries):
        ticker = await _fetch_ticker(exchange, symbol, conf, priority="HIGH", force=True)
        if order_side == "sell":
            limit_price = ticker.get("ask") or ticker.get("last") or ticker.get("close")
        else:
            limit_price = ticker.get("bid") or ticker.get("last") or ticker.get("close")

        price_str = exchange.price_to_precision(symbol, limit_price)
        q_str = exchange.amount_to_precision(symbol, remaining_qty)

        logger.info(f"[{symbol}] ⏳ 非紧急止损【Maker挂单】第 {attempt + 1}/{max_retries} 次尝试 | 挂单价: {price_str} | 数量: {q_str}")

        order_id = None
        try:
            order = await _rest(
                exchange, "create_limit_order", symbol, order_side, q_str, price_str,
                priority="HIGH", conf=conf, params=params
            )
            order_id = order.get("id")
        except Exception as e:
            logger.error(f"[{symbol}] 限价单下发失败: {e}")
            await asyncio.sleep(1)
            continue

        start_wait = time.time()
        order_filled = False
        while time.time() - start_wait < timeout_sec:
            await asyncio.sleep(check_interval_sec)
            try:
                fetched_order = await _rest(exchange, "fetch_order", order_id, symbol, priority="HIGH", conf=conf)
                status = str(fetched_order.get("status")).upper()
                filled = _f(fetched_order.get("filled"), 0.0)

                if status in ("CLOSED", "FILLED") or filled >= remaining_qty:
                    logger.success(f"[{symbol}] ✅ 限价止损单已完全成交！")
                    order_filled = True
                    break
                elif status == "CANCELED":
                    remaining_qty = max(0.0, remaining_qty - filled)
                    break
            except Exception as e:
                logger.warning(f"[{symbol}] 查询限价单状态异常: {e}")

        if order_filled:
            remaining_qty = 0
            break

        try:
            fetched_order = await _rest(exchange, "fetch_order", order_id, symbol, priority="HIGH", conf=conf)
            filled = _f(fetched_order.get("filled"), 0.0)
            remaining_qty = max(0.0, remaining_qty - filled)

            await _rest(exchange, "cancel_order", order_id, symbol, priority="HIGH", conf=conf)
            logger.info(f"[{symbol}] 限价单超时未成交，撤单重试。剩余: {remaining_qty}")
        except Exception as e:
            logger.warning(f"[{symbol}] 撤销限价单失败: {e}")

        if remaining_qty <= 0:
            break

    if remaining_qty > 0:
        logger.warning(f"[{symbol}] 限价单重试 {max_retries} 次后仍剩余 {remaining_qty}，启动市价单保底平仓")
        await _market_reduce_and_confirm(
            exchange, symbol, side, position, remaining_qty, f"{reason}(保底市价)", conf, expected_version, pos_state
        )

    fresh = await _position_fresh(exchange, symbol, side, conf, priority="HIGH")
    after = _position_size(fresh)
    logger.success(f"[{symbol}] 💰 {reason} 执行结束！仓位从 {before} -> {after}")
    return fresh, after


async def _full_exit_with_revalidation(exchange, symbol, side, position, reason, conf, pos_state):
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
# Stop maintenance & state reconciliation
# ---------------------------------------------------------------------------

LAST_STOP_ORDER_TIME = {}
LAST_STOP_MAINTAIN_TIME = {}
LAST_RISK_LOG_TIME = {}


async def _ensure_disaster_stop(exchange, symbol, side, pos, current_price, conf, ohlcv_rows=None, force_replace=False):
    if not bool(conf.get("exchange_hard_stop_enabled", True)):
        return

    global LAST_STOP_ORDER_TIME, LAST_STOP_MAINTAIN_TIME
    now_ts = time.time()
    interval = float(conf.get("stop_maintenance_interval_sec", 60.0))

    if now_ts - LAST_STOP_MAINTAIN_TIME.get(symbol, 0) < interval and not force_replace:
        return

    LAST_STOP_MAINTAIN_TIME[symbol] = now_ts
    if now_ts - LAST_STOP_ORDER_TIME.get(symbol, 0) < 10.0:
        return

    try:
        if ohlcv_rows:
            atr = _atr_from_rows(ohlcv_rows, 14)
        else:
            tf = str(conf.get("hard_stop_timeframe", "1h"))
            atr = await _closed_atr(exchange, symbol, tf, 14, conf)

        stop, distance = _calculate_disaster_stop(exchange, symbol, side, current_price, atr, conf)
        stops = await _find_stop_orders(exchange, symbol, side, pos, current_price, conf, force=True)

        needs_update = False
        if stops:
            existing_trigger = _algo_trigger(stops[0])
            if existing_trigger:
                if side == "long" and stop < existing_trigger:
                    stop = existing_trigger
                elif side == "short" and stop > existing_trigger:
                    stop = existing_trigger

            if existing_trigger and abs(existing_trigger - stop) / stop > 0.015:
                logger.info(f"[{symbol}] ⚠️ 止损单向盈利方向推进 (现价:{existing_trigger} 目标:{stop})，准备重置。")
                needs_update = True

        if stops and not force_replace and not needs_update:
            return

        if stops:
            await _cancel_stop_orders(exchange, symbol, stops, conf)

        await _create_full_close_stop(exchange, symbol, pos, side, stop, conf)
        LAST_STOP_ORDER_TIME[symbol] = time.time()

    except Exception as e:
        logger.error(f"[{symbol}] 维护灾难止损出错: {e}")


def _reconcile_position_state(state, key, symbol, side, contracts, timeframe):
    ps = state.get(key)
    if not isinstance(ps, dict):
        ps = {
            "base_contracts": contracts,
            "contracts": contracts,
            "t1_done": False,
            "t2_done": False,
            "last_checked_time": 0,
            "last_closed_candle_ts": 0,
            "last_risk_check_time": 0,
            "risk_state": {
                "t1": "normal",
                "t2": "normal",
                "t3": "normal",
                "black_swan": "normal",
                "atr_protection": "normal"
            },
            "position_version": 1,
            "risk_cycle_started_at": int(time.time()),
            "add_immunity_until_ms": 0,
        }
        state[key] = ps
        return ps, True

    # 兼容旧版本 position_protection_state.json，自动补充风控恢复字段
    ps.setdefault("last_closed_candle_ts", int(ps.get("last_checked_time", 0)))
    ps.setdefault("last_risk_check_time", 0)
    ps.setdefault("risk_state", {
        "t1": "done" if ps.get("t1_done") else "normal",
        "t2": "done" if ps.get("t2_done") else "normal",
        "t3": "normal",
        "black_swan": "normal",
        "atr_protection": "normal"
    })

    old = _f(ps.get("contracts"), contracts) or contracts
    changed = not math.isclose(old, contracts, rel_tol=1e-8, abs_tol=1e-10)

    if changed:
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
            logger.info(f"[{symbol}] 检测到手动/外部加仓 {old}->{contracts}，建立新风险周期。")
        else:
            ps["contracts"] = contracts
            logger.info(f"[{symbol}] 检测到外部减仓 {old}->{contracts}，保留已完成层级状态。")

    return ps, changed


async def cleanup_orphaned_state_and_orders(exchange, conf):
    try:
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

    except Exception as exc:
        logger.error(f"清理孤儿状态/订单异常: {exc}")


# ---------------------------------------------------------------------------
# Per-position guardian
# ---------------------------------------------------------------------------

async def watch_symbol_position(exchange, symbol):
    last_heartbeat_time = 0
    await asyncio.sleep(random.uniform(0.1, 2.0))
    logger.info(f"[{symbol}] 👁️ 高级风控守护协程已就位")

    while True:
        try:
            full_config = _load_config()
            conf = full_config.get("position_protection", {})
            if not conf.get("enabled", True):
                await asyncio.sleep(5)
                continue

            timeframe = str(conf.get("timeframe", "1h"))

            pos = RUNTIME.global_positions_map.get(symbol)
            if not pos or _position_size(pos) <= 0:
                state = _load_state()
                for side0 in ("long", "short"):
                    state.pop(_state_key(symbol, side0), None)
                _save_state(state)
                await asyncio.sleep(5)
                continue

            side = _position_side(pos)
            contracts = _position_size(pos)
            entry_price = _f(pos.get("entryPrice"), 0.0)
            key = _state_key(symbol, side)

            now_time = time.time()
            if now_time - last_heartbeat_time > 60:
                logger.info(f"[{symbol}] 🛡️ 巡检中... 当前{ '做多' if side=='long' else '做空' }仓位: {contracts} | 开仓均价: {entry_price}")
                last_heartbeat_time = now_time

            state = _load_state()
            pos_state, changed = _reconcile_position_state(state, key, symbol, side, contracts, timeframe)
            if changed:
                _save_state(state)

            n1, n2, n3 = int(conf.get("n1_bars", 7)), int(conf.get("n2_bars", 26)), int(conf.get("n3_bars", 83))
            limit = max(n3 + 25, 120)
            ohlcv_rows = await _fetch_ohlcv(exchange, symbol, timeframe, limit, conf, priority="LOW", force=False)

            async with RUNTIME.action_lock(symbol, side):
                snap = await _market_snapshot(exchange, symbol, conf, priority="NORMAL")
                current_price = snap["mark"]

                # 风控状态日志：确认行情链路正常
                logger.debug(f"[{symbol}] 行情检查正常 | 当前价:{current_price} | 方向:{side}")

                try:
                    await _ensure_disaster_stop(exchange, symbol, side, pos, current_price, conf, ohlcv_rows=ohlcv_rows, force_replace=changed)
                except Exception as e:
                    logger.error(f"[{symbol}] 交易所兜底止损异常，不影响内部风控继续运行: {e}")

            # ---------------- 紧急止损 1：放量加速 / 黑天鹅 (市价 Taker) ----------------
            try:
                emergency, details = await _emergency_signal(exchange, symbol, side, conf)

                if details:
                    logger.debug(
                        f"[{symbol}] 黑天鹅监控 | 当前波动:{details.get('move',0):.4f} | "
                        f"触发阈值:{details.get('threshold',0):.4f} | "
                        f"状态:{'触发' if emergency else '正常'}"
                    )

                if details and details.get("volume_crash"):
                    logger.warning(f"[{symbol}] ⚠️ 侦测到局部异常放量！伴随极速反向位移！")
                elif details and details["move"] >= details["threshold"] * 0.8:
                    logger.warning(f"[{symbol}] 🚨 行情波动逼近熔断阈值！当前波动: {details['move']:.2f}")

                if emergency:
                    ok, _ = await _two_source_price_ok(exchange, symbol, details["ws_price"], side, conf)
                    if ok:
                        async with RUNTIME.action_lock(symbol, side):
                            state = _load_state(); pos_state = state.get(key, pos_state)
                            fresh = await _position_fresh(exchange, symbol, side, conf, priority="HIGH")
                            if fresh:
                                msg = f"[{symbol}] 🚨 触发 {details['reason']}！执行紧急市价全平！"
                                logger.error(msg); send_alert(full_config, f"风控警告: {details['reason']}", msg, symbol=symbol)
                                success, _ = await _full_exit_with_revalidation(exchange, symbol, side, fresh, f"{details['reason']} 紧急市价全平", conf, pos_state)
                                if success:
                                    state.pop(key, None); _save_state(state)
                                    return
            except Exception as e:
                logger.error(f"[{symbol}] 紧急信号检查异常: {e}")

            # ---------------- 紧急止损 2：抄底 3ATR 专属保护线跌破 (市价 Taker) ----------------
            try:
                if pos_state.get("t3_waived") and pos_state.get("bottom_fish_sl"):
                    realtime_bf_sl = float(pos_state["bottom_fish_sl"])
                    if (side == "long" and current_price <= realtime_bf_sl) or \
                       (side == "short" and current_price >= realtime_bf_sl):
                        async with RUNTIME.action_lock(symbol, side):
                            fresh = await _position_fresh(exchange, symbol, side, conf, priority="HIGH")
                            if fresh:
                                msg = f"[{symbol}] 💔 跌破 3ATR 专属抄底保护线，无条件市价全平！"
                                logger.error(msg); send_alert(full_config, "风控警告: 实时抄底止损", msg, symbol=symbol)
                                success, _ = await _full_exit_with_revalidation(exchange, symbol, side, fresh, "抄底实时市价全平", conf, pos_state)
                                if success:
                                    state = _load_state()
                                    state.pop(key, None)
                                    _save_state(state)
                                    return
            except Exception as e:
                logger.error(f"[{symbol}] 实时抄底止损检查异常: {e}")

            # ---------------- 智能 MA 与 假破位识别 ----------------
            try:
                t1, t2, t3, closed_ts, closed_price, _ = _ma_levels_from_rows(ohlcv_rows, n1, n2, n3)
                atr = _atr_from_rows(ohlcv_rows, int(conf.get("ma_atr_period", 14)))
                br = atr * float(conf.get("ma_atr_buffer_multiplier", 0.30))
                rec = atr * float(conf.get("recovery_atr_buffer_multiplier", 0.30))

                # 内部MA风控透明日志（不改变触发逻辑）
                now_risk = time.time()
                if now_risk - LAST_RISK_LOG_TIME.get(symbol, 0) >= 60:
                    if side == "long":
                        t1_trigger, t2_trigger, t3_trigger = t1-br, t2-br, t3-br
                        states = (
                            closed_price <= t1_trigger,
                            closed_price <= t2_trigger,
                            closed_price <= t3_trigger,
                        )
                    else:
                        t1_trigger, t2_trigger, t3_trigger = t1+br, t2+br, t3+br
                        states = (
                            closed_price >= t1_trigger,
                            closed_price >= t2_trigger,
                            closed_price >= t3_trigger,
                        )

                    logger.info(
                        f"[{symbol}] 🛡️ 内部风控状态 | 方向:{side} | 当前价:{current_price} | 开仓:{entry_price}\n"
                        f"T1({n1}MA): {t1:.4f} 触发:{t1_trigger:.4f} 状态:{'触发' if states[0] else '正常'}\n"
                        f"T2({n2}MA): {t2:.4f} 触发:{t2_trigger:.4f} 状态:{'触发' if states[1] else '正常'}\n"
                        f"T3({n3}MA): {t3:.4f} 触发:{t3_trigger:.4f} 状态:{'触发' if states[2] else '正常'}\n"
                        f"ATR:{atr:.4f} 缓冲:{br:.4f} 最近收盘:{closed_price:.4f}"
                    )
                    LAST_RISK_LOG_TIME[symbol] = now_risk

                version = int(pos_state.get("position_version", 0))
                processed_version = int(pos_state.get("processed_entry_version", 0))

                if version != processed_version and entry_price > 0:
                    logger.info(f"[{symbol}] 🎯 检测到新开仓 (均价:{entry_price})，分配止损层级...")
                    if side == "long":
                        if entry_price <= t1: pos_state["t1_done"] = True
                        if entry_price <= t2: pos_state["t2_done"] = True
                        if entry_price <= t3:
                            pos_state["t3_waived"] = True
                            pos_state["bottom_fish_sl"] = entry_price - (atr * 3.0)
                            logger.info(f"[{symbol}] 🎣 判定为左侧抄底！豁免 T3 止损，设定 3ATR 保护价: {pos_state['bottom_fish_sl']:.4f}")
                        else:
                            pos_state["t3_waived"] = False
                    else: # Short
                        if entry_price >= t1: pos_state["t1_done"] = True
                        if entry_price >= t2: pos_state["t2_done"] = True
                        if entry_price >= t3:
                            pos_state["t3_waived"] = True
                            pos_state["bottom_fish_sl"] = entry_price + (atr * 3.0)
                            logger.info(f"[{symbol}] 🎣 判定为冲高摸顶！豁免 T3 止损，设定 3ATR 保护价: {pos_state['bottom_fish_sl']:.4f}")
                        else:
                            pos_state["t3_waived"] = False

                    pos_state["processed_entry_version"] = version
                    state[key] = pos_state
                    _save_state(state)

                last_ts = int(pos_state.get("last_checked_time", 0))

                if closed_ts > last_ts:
                    logger.info(f"[{symbol}] 📈 {timeframe} K线收盘验证！实体收盘价:{closed_price} | T1:{t1:.2f} | T2:{t2:.2f} | T3:{t3:.2f}")

                    if side == "long":
                        if closed_price > t3 + rec and pos_state.get("t3_waived"):
                            pos_state["t3_waived"] = False; logger.info(f"[{symbol}] 🚀 抄底成功！站上 T3，移动止损防护重装！")

                        t1_hit = closed_price <= t1 - br and not pos_state.get("t1_done")
                        t2_hit = closed_price <= t2 - br and not pos_state.get("t2_done")
                        t3_hit = closed_price <= t3 - br and not pos_state.get("t3_waived")

                        if (closed_price <= t3) and (closed_price > t3 - br) and not pos_state.get("t3_waived"):
                            logger.success(f"[{symbol}] 💡 识破假跌破！未超出 {br:.2f} 缓冲区，判定为假破位，继续持有。")

                    else: # Short
                        if closed_price < t3 - rec and pos_state.get("t3_waived"):
                            pos_state["t3_waived"] = False

                        t1_hit = closed_price >= t1 + br and not pos_state.get("t1_done")
                        t2_hit = closed_price >= t2 + br and not pos_state.get("t2_done")
                        t3_hit = closed_price >= t3 + br and not pos_state.get("t3_waived")

                        if (closed_price >= t3) and (closed_price < t3 + br) and not pos_state.get("t3_waived"):
                            logger.success(f"[{symbol}] 💡 识破假突破！未超出 {br:.2f} 缓冲区，判定为假突破，继续持有。")

                    pos_state["last_checked_time"] = closed_ts
                    pos_state["last_closed_candle_ts"] = closed_ts
                    pos_state["last_risk_check_time"] = int(time.time())
                    pos_state["risk_state"] = {
                        "t1": "triggered" if t1_hit else ("done" if pos_state.get("t1_done") else "normal"),
                        "t2": "triggered" if t2_hit else ("done" if pos_state.get("t2_done") else "normal"),
                        "t3": "triggered" if t3_hit else "normal",
                        "black_swan": "normal",
                        "atr_protection": "normal"
                    }
                    state[key] = pos_state
                    _save_state(state)

                    async with RUNTIME.action_lock(symbol, side):
                        fresh = await _position_fresh(exchange, symbol, side, conf, priority="HIGH")
                        if not fresh: continue
                        current_contracts = _position_size(fresh)

                        # T3 趋势破位 -> 紧急市价全平 (Taker)
                        if t3_hit:
                            msg = f"[{symbol}] 🚨 T3 {n3}MA 真破位，右侧趋势失效，全平仓位。"
                            logger.error(msg); send_alert(full_config, "风控警告: T3 趋势失效", msg, symbol=symbol)
                            success, _ = await _full_exit_with_revalidation(exchange, symbol, side, fresh, "T3 全平", conf, pos_state)
                            if success: state.pop(key, None); _save_state(state); return

                        # T2 破位 30% 减仓 -> 限价挂单 (Maker)
                        elif t2_hit:
                            qty = min(current_contracts, (_f(pos_state.get("base_contracts"), current_contracts)) * float(conf.get("tier2_ratio", 0.30)))
                            msg = f"[{symbol}] 📉 T2 ({n2}) MA 真破位，执行 30% 限价减仓: {qty}。"
                            logger.warning(msg); send_alert(full_config, "风控提示: T2 减仓", msg, symbol=symbol)
                            _, after = await _limit_reduce_with_maker_retry(
                                exchange, symbol, side, fresh, qty, "T2 限价减仓", conf,
                                int(pos_state.get("position_version", 0)), pos_state
                            )
                            pos_state["contracts"] = after; pos_state["t2_done"] = True
                            state[key] = pos_state; _save_state(state)

                        # T1 破位 20% 减仓 -> 限价挂单 (Maker)
                        elif t1_hit:
                            qty = min(current_contracts, (_f(pos_state.get("base_contracts"), current_contracts)) * float(conf.get("tier1_ratio", 0.20)))
                            msg = f"[{symbol}] 📉 T1 ({n1}) MA 真破位，执行 20% 限价减仓: {qty}。"
                            logger.warning(msg); send_alert(full_config, "风控提示: T1 减仓", msg, symbol=symbol)
                            _, after = await _limit_reduce_with_maker_retry(
                                exchange, symbol, side, fresh, qty, "T1 限价减仓", conf,
                                int(pos_state.get("position_version", 0)), pos_state
                            )
                            pos_state["contracts"] = after; pos_state["t1_done"] = True
                            state[key] = pos_state; _save_state(state)

            except Exception as exc:
                logger.error(f"[{symbol}] MA/破位计算逻辑异常: {exc}")

            await asyncio.sleep(5.0 + random.uniform(0.0, 1.0))

        except asyncio.CancelledError:
            raise
        except CircuitBreakerException as c_exc:
            # 明确感知限流状态并进行单行提示，不吞日志也不刷屏 Traceback
            if time.time() - RUNTIME.last_circuit_notice > 10.0:
                logger.warning(f"⏳ [限流冷却中] {c_exc}")
                RUNTIME.last_circuit_notice = time.time()
            await asyncio.sleep(5.0)
        except Exception as exc:
            logger.error(f"[{symbol}] 风控守护未知异常: {exc}")
            await asyncio.sleep(5.0)


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
    logger.info("🛡️ 风控中枢启动：集中式批量巡检 + 限流显式警报机制已就位")
    await cleanup_orphaned_state_and_orders(exchange, conf)

    tasks = {}
    last_cleanup = 0.0
    last_heartbeat_time = 0

    while True:
        try:
            full_config = _load_config()
            conf = full_config.get("position_protection", {})

            # 1. 集中批量拉取一次所有仓位
            positions = await _fetch_positions(exchange, conf, symbols=None, priority="NORMAL", force=True)

            # 2. 全局批量更新全市场 Tickers（一次请求解决所有币种 Ticker 需求）
            try:
                tickers = await _rest(exchange, "fetch_tickers", priority="LOW", conf=conf)
                now_ts = time.monotonic()
                for sym, tick_data in tickers.items():
                    RUNTIME.ticker_cache[sym] = (now_ts, tick_data)
            except Exception as e:
                logger.warning(f"批量获取 Tickers 异常 (降级为单币拉取): {e}")

            # 3. 更新全局仓位映射
            pos_map = {}
            active = set()
            for pos in positions:
                if _position_size(pos) <= 0 or _position_side(pos) not in ("long", "short"):
                    continue
                symbol = pos.get("symbol")
                if not symbol:
                    continue
                pos_map[symbol] = pos
                active.add(symbol)

            RUNTIME.global_positions_map = pos_map

            # 4. 动态调度后台守护协程
            for symbol in active:
                task = tasks.get(symbol)
                if task is None or task.done():
                    tasks[symbol] = asyncio.create_task(watch_symbol_position(exchange, symbol), name=f"position-protect:{symbol}")

            if time.time() - last_heartbeat_time >= 60:
                if active:
                    logger.info(f"💓 [风控中枢心跳] 正在守护 {len(active)} 个活跃持仓: {list(active)}")
                else:
                    logger.debug("💓 [风控中枢心跳] 当前账户 0 持仓，等待入场信号...")
                last_heartbeat_time = time.time()

            # 5. 清理无效任务
            for symbol, task in list(tasks.items()):
                if symbol not in active and task.done():
                    tasks.pop(symbol, None)

            if time.time() - last_cleanup >= 3600:
                await cleanup_orphaned_state_and_orders(exchange, conf)
                last_cleanup = time.time()

            # 6. 自适应休眠（持仓多时自动平滑平摊请求）
            dynamic_sync_sec = max(5.0, len(active) * 0.15)
            await asyncio.sleep(dynamic_sync_sec)

        except asyncio.CancelledError:
            raise
        except CircuitBreakerException as c_exc:
            logger.warning(f"⏳ [风控主控] 系统处于 API 限流冷却状态，休眠 10 秒后重试: {c_exc}")
            await asyncio.sleep(10.0)
        except Exception as exc:
            logger.error(f"风控主控循环异常: {exc}", exc_info=True)
            await asyncio.sleep(10.0)