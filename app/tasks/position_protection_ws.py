"""Binance USDⓈ-M Futures ATR position protection with WebSocket real-time kline stream.

- Inner layer: Uses asyncio and ccxt.pro to watch 5-minute kline websocket stream in real-time.
- Candle close confirmation: Instantly triggers market close upon candle close (x: true) breakout.
- Outer layer: Maintains a wider exchange-side STOP_MARKET order as a disaster recovery backup.
- Maintenance: Cleans up obsolete state records and orphaned orders periodically.
"""

import asyncio
import json
import os
import uuid
from datetime import time
from pathlib import Path

import ccxt.pro as ccxtpro
import pandas as pd
from loguru import logger

from app.services.notification_service import send_alert

STATE_FILE = Path("position_protection_state.json")
STATE_VERSION = 3
CLIENT_ALGO_PREFIX = "PM_WS_SL_"


def _f(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_state():
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"⚠️ 无法读取仓位保护状态文件：{e}，将使用空状态。")
        return {}


def _save_state(state):
    try:
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        logger.error(f"❌ 保存仓位保护状态失败：{e}", exc_info=True)


def _position_side(position):
    side = str(position.get("side") or "").lower()
    if side in ("long", "short"):
        return side
    info = position.get("info") or {}
    ps = str(info.get("positionSide") or "").upper()
    if ps == "LONG":
        return "long"
    if ps == "SHORT":
        return "short"
    amt = _f(info.get("positionAmt"), 0)
    return "long" if amt > 0 else "short" if amt < 0 else None


def _position_size(position):
    contracts = _f(position.get("contracts"))
    if contracts is not None:
        return abs(contracts)
    info = position.get("info") or {}
    return abs(_f(info.get("positionAmt"), 0) or 0)


def _entry_price(position):
    for key in ("entryPrice", "average"):
        value = _f(position.get(key))
        if value and value > 0:
            return value
    info = position.get("info") or {}
    value = _f(info.get("entryPrice"))
    return value if value and value > 0 else None


def _raw_position_side(position):
    info = position.get("info") or {}
    ps = str(info.get("positionSide") or "BOTH").upper()
    return ps if ps in ("BOTH", "LONG", "SHORT") else "BOTH"


def _algo_methods(exchange):
    required = (
        "fapiPrivateGetOpenAlgoOrders",
        "fapiPrivatePostAlgoOrder",
        "fapiPrivateDeleteAlgoOrder",
    )
    missing = [x for x in required if not hasattr(exchange, x)]
    if missing:
        raise RuntimeError("当前 CCXT 异步版本缺少 Binance Algo Order 原始接口。")


async def _get_open_algo_orders(exchange, symbol):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    response = await exchange.fapiPrivateGetOpenAlgoOrders({"symbol": market_id})
    if isinstance(response, dict):
        return response.get("orders") or response.get("data") or []
    return response or []


def _algo_status(order):
    return str(order.get("algoStatus") or order.get("status") or "").upper()


def _algo_trigger(order):
    return _f(order.get("triggerPrice") or order.get("stopPrice"))


def _is_stop_loss_algo(order, side, position_side, current_price):
    if not isinstance(order, dict):
        return False
    if _algo_status(order) not in ("NEW", "TRIGGER_PENDING", ""):
        return False
    if str(order.get("orderType") or order.get("type") or "").upper() != "STOP_MARKET":
        return False
    expected_side = "SELL" if side == "long" else "BUY"
    if str(order.get("side") or "").upper() != expected_side:
        return False
    order_ps = str(order.get("positionSide") or "BOTH").upper()
    if position_side in ("LONG", "SHORT") and order_ps != position_side:
        return False
    trigger = _algo_trigger(order)
    if trigger is None or current_price is None:
        return True
    return trigger < current_price if side == "long" else trigger > current_price


async def _find_stop_orders(exchange, symbol, side, position, current_price=None):
    if current_price is None:
        ticker = await exchange.fetch_ticker(symbol)
        current_price = _f(ticker.get("mark") or ticker.get("last"))
    raw_ps = _raw_position_side(position)
    orders = await _get_open_algo_orders(exchange, symbol)
    return [o for o in orders if _is_stop_loss_algo(o, side, raw_ps, current_price)]


def _select_existing_stop(stops, side):
    valid = [(_algo_trigger(o), o) for o in stops if _algo_trigger(o) is not None]
    if not valid:
        return None, None
    if side == "long":
        return max(valid, key=lambda x: x[0])
    return min(valid, key=lambda x: x[0])


async def _cancel_algo(exchange, symbol, algo_id):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    return await exchange.fapiPrivateDeleteAlgoOrder({"symbol": market_id, "algoId": str(algo_id)})


async def _cancel_stop_orders(exchange, symbol, stops):
    for order in stops:
        algo_id = order.get("algoId") or order.get("id")
        if algo_id:
            try:
                await _cancel_algo(exchange, symbol, algo_id)
            except Exception as e:
                logger.error(f"❌ {symbol} 取消旧止损失败: {e}")


def _make_client_algo_id(symbol, side):
    compact = symbol.replace("/", "").replace(":", "")
    suffix = "L" if side == "long" else "S"
    return f"{CLIENT_ALGO_PREFIX}{compact}_{suffix}_{uuid.uuid4().hex[:8]}"[:36]


async def _create_stop(exchange, symbol, position, side, stop_price):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    raw_ps = _raw_position_side(position)
    qty = _position_size(position)
    params = {
        "algoType": "CONDITIONAL",
        "symbol": market_id,
        "side": "SELL" if side == "long" else "BUY",
        "type": "STOP_MARKET",
        "positionSide": raw_ps,
        "quantity": exchange.amount_to_precision(symbol, qty),
        "triggerPrice": exchange.price_to_precision(symbol, stop_price),
        "workingType": "MARK_PRICE",
        "newOrderRespType": "RESULT",
        "clientAlgoId": _make_client_algo_id(symbol, side),
    }
    if raw_ps == "BOTH":
        params["reduceOnly"] = "true"
    return await exchange.fapiPrivatePostAlgoOrder(params)


async def _calculate_atr_async(exchange, symbol, timeframe, period):
    limit = max(period + 50, 100)
    rows = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    if not rows or len(rows) < period + 2:
        raise RuntimeError(f"{symbol} K线不足")
    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    if len(df) > 1:
        df = df.iloc[:-1].copy()
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [df["high"] - df["low"], (df["high"] - prev_close).abs(), (df["low"] - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    value = _f(atr.iloc[-1])
    if value is None or value <= 0:
        raise RuntimeError(f"{symbol} ATR 计算错误")
    return value


async def cleanup_orphaned_state_and_orders(exchange):
    """【恢复清理逻辑】定期检查并清理无持仓币种残留的本地状态与交易所孤儿条件单"""
    logger.info("🧹 执行后台清理维护任务：检查孤儿状态与残留条件单...")
    try:
        # 确保市场列表已加载，否则 exchange.markets 会为空
        if not exchange.markets:
            await exchange.load_markets()

        positions = await exchange.fetch_positions()
        active_symbols = {p["symbol"] for p in positions if _position_size(p) > 0}

        # 1. 清理本地过期的 state 记录
        state = _load_state()
        state_modified = False
        for key in list(state.keys()):
            found_active = any(sym in key for sym in active_symbols)
            if not found_active and active_symbols:
                state.pop(key, None)
                state_modified = True
        if state_modified:
            _save_state(state)
            logger.info("🧹 已清理本地失效的仓位保护状态记录。")

        # 2. 清理没有持仓但挂有本策略前缀条件单的“孤儿订单”
        markets = exchange.markets
        for symbol, market in markets.items():
            if market.get("linear") and symbol not in active_symbols:
                try:
                    orders = await _get_open_algo_orders(exchange, symbol)
                    for order in orders:
                        client_id = str(order.get("clientAlgoId") or "")
                        if client_id.startswith(CLIENT_ALGO_PREFIX):
                            algo_id = order.get("algoId") or order.get("id")
                            if algo_id:
                                await _cancel_algo(exchange, symbol, algo_id)
                                logger.warning(f"🧹 发现无仓位残留的孤儿止损单，已自动清理: {symbol} (AlgoID: {algo_id})")
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"❌ 后台清理维护任务异常: {e}", exc_info=True)

async def watch_symbol_position(exchange, symbol, config):
    """单仓位的 WebSocket 实时监控守护协程"""
    conf = config.get("position_protection", {})
    atr_conf = conf.get("atr", {})
    timeframe = str(atr_conf.get("timeframe", "1h"))
    period = max(2, int(atr_conf.get("period", 14)))
    multiplier = float(atr_conf.get("multiplier", 1.5))
    exchange_buffer = float(atr_conf.get("exchange_buffer_multiplier", 0.5))
    min_percent = float(atr_conf.get("min_percent", 0.01))
    max_percent = float(atr_conf.get("max_percent", 0.06))

    logger.info(f"🔌 启动 WebSocket 实时监控：{symbol} (基于 5m K线闭环确认)")

    while True:
        try:
            positions = await exchange.fetch_positions([symbol])
            pos = next((p for p in positions if _position_size(p) > 0 and _position_side(p) in ("long", "short")), None)

            if not pos:
                logger.info(f"🧹 {symbol} 仓位已平，退出 WebSocket 监控。")
                state = _load_state()
                for k in list(state.keys()):
                    if symbol in k:
                        state.pop(k, None)
                _save_state(state)
                break

            side = _position_side(pos)
            entry = _entry_price(pos)
            contracts = _position_size(pos)
            raw_ps = _raw_position_side(pos)

            atr = await _calculate_atr_async(exchange, symbol, timeframe, period)

            raw_dist = atr * multiplier / entry
            distance_pct = max(min_percent, min(raw_dist, max_percent))
            smart_stop_price = float(exchange.price_to_precision(symbol, entry * (1 - distance_pct) if side == "long" else entry * (1 + distance_pct)))

            ex_dist = max(min_percent, min(atr * (multiplier + exchange_buffer) / entry, max_percent))
            exchange_target_stop = float(exchange.price_to_precision(symbol, entry * (1 - ex_dist) if side == "long" else entry * (1 + ex_dist)))

            stops = await _find_stop_orders(exchange, symbol, side, pos)
            existing_stop, existing_order = _select_existing_stop(stops, side)

            if existing_stop is None:
                await _create_stop(exchange, symbol, pos, side, exchange_target_stop)
                logger.success(f"🛡️ {symbol} 建立交易所兜底止损：{exchange_target_stop}")

            # 实时监听 5分钟 K 线流
            ohlcvs = await exchange.watch_ohlcv(symbol, '5m')
            if not ohlcvs:
                continue

            latest_candle = ohlcvs[-1]
            close_price = _f(latest_candle[4])

            is_touched = (side == "long" and close_price <= smart_stop_price) or \
                         (side == "short" and close_price >= smart_stop_price)

            if is_touched:
                logger.warning(f"🚨 {symbol} 实时 WebSocket 检测到价格触及内层线！触发收盘确认。收盘价: {close_price}, 止损线: {smart_stop_price}")

                await exchange.create_market_order(
                    symbol,
                    'sell' if side == "long" else 'buy',
                    contracts,
                    params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True}
                )
                if stops:
                    await _cancel_stop_orders(exchange, symbol, stops)
                break

        except ccxtpro.NetworkError as ne:
            logger.warning(f"⚠️ {symbol} WebSocket 网络抖动，正在自动重连: {ne}")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"❌ {symbol} WebSocket 监控异常: {e}", exc_info=True)
            await asyncio.sleep(5)


async def protect_positions_main(exchange, config):
    """主任务：动态扫描活跃持仓启动 WebSocket 协程，并定时执行历史/孤儿订单清理"""
    conf = config.get("position_protection", {})
    if not conf.get("enabled", True):
        return

    _algo_methods(exchange)

    # 启动时先执行一次清理
    await cleanup_orphaned_state_and_orders(exchange)
    last_cleanup_time = time.time()

    while True:
        try:
            # 每隔 1 小时执行一次后台清理维护
            if time.time() - last_cleanup_time > 3600:
                await cleanup_orphaned_state_and_orders(exchange)
                last_cleanup_time = time.time()

            positions = await exchange.fetch_positions()
            active_symbols = set()
            tasks = []

            for pos in positions:
                if _position_size(pos) > 0 and _position_side(pos) in ("long", "short"):
                    symbol = pos.get("symbol")
                    if symbol and symbol not in active_symbols:
                        active_symbols.add(symbol)
                        tasks.append(asyncio.create_task(watch_symbol_position(exchange, symbol, config)))

            if tasks:
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"❌ WebSocket 主控循环异常: {e}", exc_info=True)
            await asyncio.sleep(10)