"""Binance USDⓈ-M Futures Multi-Tier & ATR Position Protection (JSON Config Driven).

- Config Source: Reads directly from local `config.json`.
- Features: 3-Tier Swing High/Low protection, ATR Stop, Trailing Stop, and Exchange Hard Stop-Loss.
"""

import asyncio
import json
import os
import time
import uuid
from pathlib import Path

import ccxt.pro as ccxtpro
import pandas as pd
from loguru import logger

from app.services.notification_service import send_alert

STATE_FILE = Path("position_protection_state.json")
CONFIG_FILE = Path("config.json")
CLIENT_ALGO_PREFIX = "PM_WS_SL_"

DEFAULT_CONFIG = {
    "position_protection": {
        "enabled": True,
        "interval_minutes": 5,
        "stop_mode": "swing_levels",
        "timeframe": "1h",
        "n1_bars": 7,
        "n2_bars": 26,
        "n3_bars": 83,
        "tier1_ratio": 0.2,
        "tier2_ratio": 0.3,
        "exchange_buffer_pct": 0.01,
        "atr": {
            "period": 14,
            "multiplier": 1.5,
            "exchange_buffer_multiplier": 1,
            "min_percent": 0.015,
            "max_percent": 0.08
        },
        "trailing": {
            "enabled": True,
            "activation_atr": 1.5
        }
    }
}


def _load_config():
    """从本地 config.json 读取配置，若不存在则自动初始化默认配置"""
    if not CONFIG_FILE.exists():
        try:
            with CONFIG_FILE.open("w", encoding="utf-8") as f:
                json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
            logger.info("📄 已自动生成默认 config.json 配置文件。")
        except Exception as e:
            logger.error(f"❌ 无法创建默认 config.json: {e}")
        return DEFAULT_CONFIG

    try:
        with CONFIG_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "position_protection" in data:
            return data
    except Exception as e:
        logger.warning(f"⚠️ 读取 config.json 失败，使用默认配置: {e}")

    return DEFAULT_CONFIG


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
        logger.warning(f"⚠️ 无法读取状态文件：{e}")
        return {}


def _save_state(state):
    try:
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        logger.error(f"❌ 保存状态失败：{e}", exc_info=True)


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
    status = _algo_status(order)
    if status and status not in ("NEW", "TRIGGER_PENDING", "UNTRIGGERED", ""):
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
                logger.error(f"❌ [{symbol}] 取消旧止损失败: {e}")


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

    logger.info(f"🛡️ [{symbol}] 提交外部硬止损兜底单 | 方向: {params['side']} | 数量: {qty} | 触发价: {stop_price}")
    response = await exchange.fapiPrivatePostAlgoOrder(params)
    logger.success(f"✅ [{symbol}] 外部硬止损兜底单挂单成功")
    return response


async def _calculate_swing_levels_async(exchange, symbol, side, timeframe, n1, n2, n3):
    """根据可配置周期与 N1/N2/N3 计算高低点：做多取低点(Low)，做空取高点(High)"""
    limit = max(n3 + 10, 100)
    rows = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    if not rows or len(rows) < n3 + 2:
        raise RuntimeError(f"{symbol} {timeframe} K线数据不足 (需要至少 {n3 + 2} 根)")

    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    if len(df) > 1:
        df = df.iloc[:-1].copy()  # 排除正在运行的当前根 K 线

    if side == "long":
        t1 = float(df["low"].iloc[-n1:].min())
        t2 = float(df["low"].iloc[-n2:].min())
        t3 = float(df["low"].iloc[-n3:].min())
    else:
        t1 = float(df["high"].iloc[-n1:].max())
        t2 = float(df["high"].iloc[-n2:].max())
        t3 = float(df["high"].iloc[-n3:].max())

    return t1, t2, t3


async def cleanup_orphaned_state_and_orders(exchange):
    try:
        if not exchange.markets:
            await exchange.load_markets()
        positions = await exchange.fetch_positions()
        active_symbols = {p["symbol"] for p in positions if _position_size(p) > 0}

        state = _load_state()
        state_modified = False
        for key in list(state.keys()):
            found_active = any(sym in key for sym in active_symbols)
            if not found_active and active_symbols:
                state.pop(key, None)
                state_modified = True
        if state_modified:
            _save_state(state)

        for symbol, market in exchange.markets.items():
            if market.get("linear") and symbol not in active_symbols:
                try:
                    orders = await _get_open_algo_orders(exchange, symbol)
                    for order in orders:
                        if str(order.get("clientAlgoId") or "").startswith(CLIENT_ALGO_PREFIX):
                            algo_id = order.get("algoId") or order.get("id")
                            if algo_id:
                                await _cancel_algo(exchange, symbol, algo_id)
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"❌ [清理任务] 异常: {e}", exc_info=True)


async def watch_symbol_position(exchange, symbol):
    """从 config.json 读取配置并执行风控守护协程"""
    while True:
        try:
            full_config = _load_config()
            conf = full_config.get("position_protection", {})
            timeframe = str(conf.get("timeframe", "1h"))
            n1 = int(conf.get("n1_bars", 7))
            n2 = int(conf.get("n2_bars", 26))
            n3 = int(conf.get("n3_bars", 83))

            tier1_ratio = float(conf.get("tier1_ratio", 0.2))
            tier2_ratio = float(conf.get("tier2_ratio", 0.3))
            buffer_pct = float(conf.get("exchange_buffer_pct", 0.01))

            await asyncio.sleep(5)

            positions = await exchange.fetch_positions([symbol])
            pos = next((p for p in positions if _position_size(p) > 0 and _position_side(p) in ("long", "short")), None)

            state = _load_state()
            state_key = f"{symbol}_{_position_side(pos) if pos else 'none'}"

            if not pos:
                logger.info(f"🧹 [{symbol}] 持仓已平，退出监控并清理状态。")
                if state_key in state:
                    state.pop(state_key, None)
                    _save_state(state)
                break

            side = _position_side(pos)
            contracts = _position_size(pos)
            raw_ps = _raw_position_side(pos)

            t1_val, t2_val, t3_val = await _calculate_swing_levels_async(exchange, symbol, side, timeframe, n1, n2, n3)

            if side == "long":
                exchange_target_stop = float(exchange.price_to_precision(symbol, t3_val * (1 - buffer_pct)))
            else:
                exchange_target_stop = float(exchange.price_to_precision(symbol, t3_val * (1 + buffer_pct)))

            stops = await _find_stop_orders(exchange, symbol, side, pos)
            existing_stop, _ = _select_existing_stop(stops, side)

            if existing_stop is None:
                logger.warning(f"⚠️ [{symbol}] 交易所缺少硬止损兜底单，正在重新补挂...")
                await _create_stop(exchange, symbol, pos, side, exchange_target_stop)

            try:
                ohlcvs = await asyncio.wait_for(exchange.watch_ohlcv(symbol, timeframe), timeout=30)
            except asyncio.TimeoutError:
                continue

            if not ohlcvs:
                continue

            latest_candle = ohlcvs[-1]
            close_price = _f(latest_candle[4])

            logger.debug(f"📊 [{symbol}] ({side.upper()}) 巡检 | 收盘价: {close_price} | T1({n1}k): {t1_val} | T2({n2}k): {t2_val} | T3({n3}k): {t3_val}")

            pos_state = state.get(state_key, {})
            t1_done = pos_state.get("t1_done", False)
            t2_done = pos_state.get("t2_done", False)

            if side == "long":
                if close_price <= t3_val:
                    msg = f"🚨 [{symbol}] 做多长期极值破位（第三层清算）！\n收盘价: {close_price} 跌破 {n3} 根 K线低点: {t3_val}，执行全仓清算！"
                    logger.warning(msg)
                    await send_alert(msg)
                    await exchange.create_market_order(symbol, 'sell', contracts, params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    if stops:
                        await _cancel_stop_orders(exchange, symbol, stops)
                    break
                elif close_price <= t2_val and not t2_done:
                    reduce_qty = contracts * tier2_ratio
                    msg = f"⚠️ [{symbol}] 做多中期极值破位（第二层减仓）！\n收盘价: {close_price} 跌破 {n2} 根 K线低点: {t2_val}，减仓 {tier2_ratio*100}%"
                    logger.warning(msg)
                    await send_alert(msg)
                    await exchange.create_market_order(symbol, 'sell', exchange.amount_to_precision(symbol, reduce_qty), params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    pos_state["t2_done"] = True
                    state[state_key] = pos_state
                    _save_state(state)
                elif close_price <= t1_val and not t1_done:
                    reduce_qty = contracts * tier1_ratio
                    msg = f"💡 [{symbol}] 做多短期极值破位（第一层减仓）！\n收盘价: {close_price} 跌破 {n1} 根 K线低点: {t1_val}，减仓 {tier1_ratio*100}%"
                    logger.warning(msg)
                    await send_alert(msg)
                    await exchange.create_market_order(symbol, 'sell', exchange.amount_to_precision(symbol, reduce_qty), params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    pos_state["t1_done"] = True
                    state[state_key] = pos_state
                    _save_state(state)

            elif side == "short":
                if close_price >= t3_val:
                    msg = f"🚨 [{symbol}] 做空长期极值破位（第三层清算）！\n收盘价: {close_price} 突破 {n3} 根 K线高点: {t3_val}，执行全仓清算！"
                    logger.warning(msg)
                    await send_alert(msg)
                    await exchange.create_market_order(symbol, 'buy', contracts, params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    if stops:
                        await _cancel_stop_orders(exchange, symbol, stops)
                    break
                elif close_price >= t2_val and not t2_done:
                    reduce_qty = contracts * tier2_ratio
                    msg = f"⚠️ [{symbol}] 做空中期极值破位（第二层减仓）！\n收盘价: {close_price} 突破 {n2} 根 K线高点: {t2_val}，减仓 {tier2_ratio*100}%"
                    logger.warning(msg)
                    await send_alert(msg)
                    await exchange.create_market_order(symbol, 'buy', exchange.amount_to_precision(symbol, reduce_qty), params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    pos_state["t2_done"] = True
                    state[state_key] = pos_state
                    _save_state(state)
                elif close_price >= t1_val and not t1_done:
                    reduce_qty = contracts * tier1_ratio
                    msg = f"💡 [{symbol}] 做空短期极值破位（第一层减仓）！\n收盘价: {close_price} 突破 {n1} 根 K线高点: {t1_val}，减仓 {tier1_ratio*100}%"
                    logger.warning(msg)
                    await send_alert(msg)
                    await exchange.create_market_order(symbol, 'buy', exchange.amount_to_precision(symbol, reduce_qty), params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    pos_state["t1_done"] = True
                    state[state_key] = pos_state
                    _save_state(state)

        except ccxtpro.NetworkError as ne:
            logger.warning(f"⚠️ [{symbol}] 网络抖动: {ne}")
            await asyncio.sleep(5)
        except Exception as e:
            err_msg = str(e)
            if "429" in err_msg or "-1003" in err_msg:
                logger.warning(f"⚠️ [{symbol}] 触发 429 限制，冷却 15 秒...")
                await asyncio.sleep(15)
            else:
                logger.error(f"❌ [{symbol}] 监控异常: {e}", exc_info=True)
                await asyncio.sleep(5)


async def protect_positions_main(exchange, config=None):
    full_config = _load_config()
    conf = full_config.get("position_protection", {})
    if not conf.get("enabled", True):
        logger.info("🛡️ 仓位保护功能已禁用。")
        return

    _algo_methods(exchange)
    logger.info("🛡️ 启动多层极值风控主控循环 (JSON 配置驱动)...")
    await cleanup_orphaned_state_and_orders(exchange)
    last_cleanup_time = time.time()

    while True:
        try:
            current_time = time.time()
            if current_time - last_cleanup_time > 3600:
                await cleanup_orphaned_state_and_orders(exchange)
                last_cleanup_time = current_time

            positions = await exchange.fetch_positions()
            active_symbols = set()
            tasks = []

            for pos in positions:
                if _position_size(pos) > 0 and _position_side(pos) in ("long", "short"):
                    symbol = pos.get("symbol")
                    if symbol and symbol not in active_symbols:
                        active_symbols.add(symbol)
                        tasks.append(asyncio.create_task(watch_symbol_position(exchange, symbol)))

            if tasks:
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"❌ 主控循环异常: {e}", exc_info=True)
            await asyncio.sleep(10)