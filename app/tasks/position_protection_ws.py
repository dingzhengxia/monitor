"""Binance USDⓈ-M Futures Multi-Tier & Trailing Position Protection.

- Tier 1/2/3 Internal Stop/Take-Profit Logic
- Exchange Hard Stop: closePosition=True (Auto covers size changes)
- Trailing Stop: Automatically updates exchange conditional order to lock in profit.
- Precise ClientAlgoId Tracking & Robust Cancellation on Position Add / Trailing.
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
        "auto_reset_state_on_add": True,
        "sync_exchange_stop_on_reduce": True,
        "trailing_update_threshold_pct": 0.005,
        "take_profit": {
            "enabled": False,
            "mode": "rr_ratio",
            "risk_reward_ratio": 2.0
        }
    }
}


def _load_config():
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


def _is_script_algo_order(order):
    """判断某个订单是否是由本程序挂出的风控兜底单"""
    if not isinstance(order, dict):
        return False
    status = _algo_status(order)
    if status and status not in ("NEW", "WORKING", "TRIGGER_PENDING", "UNTRIGGERED", "OPEN", ""):
        return False

    client_id = str(order.get("clientAlgoId") or "")
    if client_id.startswith(CLIENT_ALGO_PREFIX):
        return True

    # 兜底特征：如果是 closePosition=true 且是 STOP_MARKET
    is_stop = str(order.get("orderType") or order.get("type") or "").upper() == "STOP_MARKET"
    is_close_all = str(order.get("closePosition") or "").lower() == "true"
    return is_stop and is_close_all


async def _get_script_stop_orders(exchange, symbol):
    """获取交易所上当前属于本程序的所有活跃兜底止损单"""
    orders = await _get_open_algo_orders(exchange, symbol)
    return [o for o in orders if _is_script_algo_order(o)]


async def _cancel_script_stop_orders(exchange, symbol):
    """撤销交易所上所有属于本程序的旧兜底单"""
    script_stops = await _get_script_stop_orders(exchange, symbol)
    if not script_stops:
        return 0

    market_id = exchange.market(symbol)["id"]
    cancelled_count = 0

    for order in script_stops:
        algo_id = order.get("algoId") or order.get("id")
        client_algo_id = order.get("clientAlgoId")

        params = {"symbol": market_id}
        if algo_id:
            try:
                params["algoId"] = int(algo_id)
            except ValueError:
                params["algoId"] = str(algo_id)
        elif client_algo_id:
            params["clientAlgoId"] = str(client_algo_id)

        try:
            await exchange.fapiPrivateDeleteAlgoOrder(params)
            cancelled_count += 1
            logger.info(f"🗑️ [{symbol}] 已成功撤销旧兜底单 (algoId: {algo_id})")
        except Exception as e:
            logger.error(f"❌ [{symbol}] 撤销旧兜底单失败 (algoId: {algo_id}): {e}")

    return cancelled_count


def _make_client_algo_id(symbol, side):
    compact = symbol.replace("/", "").replace(":", "")
    suffix = "L" if side == "long" else "S"
    return f"{CLIENT_ALGO_PREFIX}{compact}_{suffix}_{uuid.uuid4().hex[:8]}"[:36]


async def _create_full_close_stop(exchange, symbol, position, side, stop_price):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    raw_ps = _raw_position_side(position)

    params = {
        "algoType": "CONDITIONAL",
        "symbol": market_id,
        "side": "SELL" if side == "long" else "BUY",
        "type": "STOP_MARKET",
        "positionSide": raw_ps,
        "triggerPrice": exchange.price_to_precision(symbol, stop_price),
        "workingType": "MARK_PRICE",
        "closePosition": "true",
        "clientAlgoId": _make_client_algo_id(symbol, side),
    }

    logger.info(f"🛡️ [{symbol}] 挂设交易所【全仓全平】兜底单 | 方向: {params['side']} | 触发价: {stop_price}")
    response = await exchange.fapiPrivatePostAlgoOrder(params)
    logger.success(f"✅ [{symbol}] 全仓全平兜底单挂单成功")
    return response


async def _calculate_swing_levels_async(exchange, symbol, side, timeframe, n1, n2, n3):
    limit = max(n3 + 10, 100)
    rows = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    if not rows or len(rows) < n3 + 2:
        raise RuntimeError(f"{symbol} {timeframe} K线数据不足 (需要至少 {n3 + 2} 根)")

    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    if len(df) > 1:
        df = df.iloc[:-1].copy()

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
                    await _cancel_script_stop_orders(exchange, symbol)
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"❌ [清理任务] 异常: {e}", exc_info=True)


async def watch_symbol_position(exchange, symbol):
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
            auto_reset_on_add = bool(conf.get("auto_reset_state_on_add", True))
            trailing_threshold = float(conf.get("trailing_update_threshold_pct", 0.005))

            await asyncio.sleep(5)

            positions = await exchange.fetch_positions([symbol])
            pos = next((p for p in positions if _position_size(p) > 0 and _position_side(p) in ("long", "short")), None)

            state = _load_state()
            state_key = f"{symbol}_{_position_side(pos) if pos else 'none'}"

            if not pos:
                logger.info(f"🧹 [{symbol}] 检测到无持仓，撤销交易所孤儿单并注销风控。")
                await _cancel_script_stop_orders(exchange, symbol)
                if state_key in state:
                    state.pop(state_key, None)
                    _save_state(state)
                break

            side = _position_side(pos)
            contracts = _position_size(pos)
            raw_ps = _raw_position_side(pos)

            pos_state = state.get(state_key, {})
            last_contracts = pos_state.get("contracts", contracts)

            t1_val, t2_val, t3_val = await _calculate_swing_levels_async(exchange, symbol, side, timeframe, n1, n2, n3)

            if side == "long":
                exchange_target_stop = float(exchange.price_to_precision(symbol, t3_val * (1 - buffer_pct)))
            else:
                exchange_target_stop = float(exchange.price_to_precision(symbol, t3_val * (1 + buffer_pct)))

            # 获取当前挂在交易所的本脚本止损单
            stops = await _get_script_stop_orders(exchange, symbol)

            current_trigger = None
            if stops:
                valid_triggers = [_algo_trigger(o) for o in stops if _algo_trigger(o) is not None]
                if valid_triggers:
                    current_trigger = max(valid_triggers) if side == "long" else min(valid_triggers)

            is_added = contracts > (last_contracts + 1e-8)

            needs_trailing = False
            if current_trigger is not None:
                if side == "long" and exchange_target_stop > current_trigger * (1 + trailing_threshold):
                    needs_trailing = True
                elif side == "short" and exchange_target_stop < current_trigger * (1 - trailing_threshold):
                    needs_trailing = True

            # 触发挂/更新止损单的条件：无单、加仓、或触发移动止盈
            if not stops or is_added or needs_trailing:
                if needs_trailing and not is_added and stops:
                    logger.success(f"🚀 [{symbol}] 触发移动止盈！(旧兜底价: {current_trigger} -> 新兜底价: {exchange_target_stop})，自动上移锁润单！")

                if is_added:
                    logger.info(f"📈 [{symbol}] 检测到加仓 (旧仓: {last_contracts} -> 新仓: {contracts})")
                    if auto_reset_on_add:
                        logger.info(f"🔄 [{symbol}] 重置 2/3/5 分层风控状态，按新总仓位保护。")
                        pos_state["t1_done"] = False
                        pos_state["t2_done"] = False

                # 无论出于什么原因挂新单，首先彻底撤销交易所上的旧兜底单
                await _cancel_script_stop_orders(exchange, symbol)

                # 挂设新的全仓全平兜底单
                await _create_full_close_stop(exchange, symbol, pos, side, exchange_target_stop)

                pos_state["contracts"] = contracts
                state[state_key] = pos_state
                _save_state(state)

            try:
                ohlcvs = await asyncio.wait_for(exchange.watch_ohlcv(symbol, timeframe), timeout=30)
            except asyncio.TimeoutError:
                continue

            if not ohlcvs or not ohlcvs[-1]:
                continue

            latest_candle = ohlcvs[-1]
            close_price = _f(latest_candle[4])

            t1_done = pos_state.get("t1_done", False)
            t2_done = pos_state.get("t2_done", False)

            if side == "long":
                if close_price <= t3_val:
                    msg = f"🚨 [{symbol}] 做多 T3 破位！收盘价: {close_price} <= {t3_val}，执行剩余 50% 全仓清算！"
                    logger.warning(msg)
                    send_alert(full_config, "风控警告: T3 全平清算", msg, symbol=symbol)
                    try:
                        await exchange.create_market_order(symbol, 'sell', contracts, params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    except Exception as e:
                        logger.error(f"清算失败: {e}")
                    await _cancel_script_stop_orders(exchange, symbol)
                    break
                elif close_price <= t2_val and not t2_done:
                    reduce_qty = contracts * tier2_ratio
                    msg = f"⚠️ [{symbol}] 做多 T2 破位！收盘价: {close_price} <= {t2_val}，执行 30% 减仓（数量: {reduce_qty}）"
                    logger.warning(msg)
                    send_alert(full_config, "风控提示: T2 阶段减仓", msg, symbol=symbol)
                    try:
                        await exchange.create_market_order(symbol, 'sell', exchange.amount_to_precision(symbol, reduce_qty), params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    except Exception as e:
                        logger.error(f"减仓失败: {e}")
                    pos_state["t2_done"] = True
                    state[state_key] = pos_state
                    _save_state(state)
                elif close_price <= t1_val and not t1_done:
                    reduce_qty = contracts * tier1_ratio
                    msg = f"💡 [{symbol}] 做多 T1 破位！收盘价: {close_price} <= {t1_val}，执行 20% 减仓（数量: {reduce_qty}）"
                    logger.warning(msg)
                    send_alert(full_config, "风控提示: T1 阶段减仓", msg, symbol=symbol)
                    try:
                        await exchange.create_market_order(symbol, 'sell', exchange.amount_to_precision(symbol, reduce_qty), params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    except Exception as e:
                        logger.error(f"减仓失败: {e}")
                    pos_state["t1_done"] = True
                    state[state_key] = pos_state
                    _save_state(state)

            elif side == "short":
                if close_price >= t3_val:
                    msg = f"🚨 [{symbol}] 做空 T3 破位！收盘价: {close_price} >= {t3_val}，执行剩余 50% 全仓清算！"
                    logger.warning(msg)
                    send_alert(full_config, "风控警告: T3 全平清算", msg, symbol=symbol)
                    try:
                        await exchange.create_market_order(symbol, 'buy', contracts, params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    except Exception as e:
                        logger.error(f"清算失败: {e}")
                    await _cancel_script_stop_orders(exchange, symbol)
                    break
                elif close_price >= t2_val and not t2_done:
                    reduce_qty = contracts * tier2_ratio
                    msg = f"⚠️ [{symbol}] 做空 T2 破位！收盘价: {close_price} >= {t2_val}，执行 30% 减仓（数量: {reduce_qty}）"
                    logger.warning(msg)
                    send_alert(full_config, "风控提示: T2 阶段减仓", msg, symbol=symbol)
                    try:
                        await exchange.create_market_order(symbol, 'buy', exchange.amount_to_precision(symbol, reduce_qty), params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    except Exception as e:
                        logger.error(f"减仓失败: {e}")
                    pos_state["t2_done"] = True
                    state[state_key] = pos_state
                    _save_state(state)
                elif close_price >= t1_val and not t1_done:
                    reduce_qty = contracts * tier1_ratio
                    msg = f"💡 [{symbol}] 做空 T1 破位！收盘价: {close_price} >= {t1_val}，执行 20% 减仓（数量: {reduce_qty}）"
                    logger.warning(msg)
                    send_alert(full_config, "风控提示: T1 阶段减仓", msg, symbol=symbol)
                    try:
                        await exchange.create_market_order(symbol, 'buy', exchange.amount_to_precision(symbol, reduce_qty), params={"reduceOnly": True, "positionSide": raw_ps} if raw_ps != "BOTH" else {"reduceOnly": True})
                    except Exception as e:
                        logger.error(f"减仓失败: {e}")
                    pos_state["t1_done"] = True
                    state[state_key] = pos_state
                    _save_state(state)

        except ccxtpro.NetworkError as ne:
            logger.warning(f"⚠️ [{symbol}] 网络抖动: {ne}")
            await asyncio.sleep(5)
        except Exception as e:
            err_msg = str(e)
            if "429" in err_msg or "-1003" in err_msg:
                logger.warning(f"⚠️ [{symbol}] 触发 API 限频，等待 15 秒...")
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
    logger.info("🛡️ 启动多层极值风控主控循环 (已彻底修复加仓/移动止盈旧单撤销机制)...")
    await cleanup_orphaned_state_and_orders(exchange)
    last_cleanup_time = time.time()

    active_tasks = {}

    while True:
        try:
            current_time = time.time()
            if current_time - last_cleanup_time > 3600:
                await cleanup_orphaned_state_and_orders(exchange)
                last_cleanup_time = current_time

            positions = await exchange.fetch_positions()
            current_active_symbols = set()

            for pos in positions:
                if _position_size(pos) > 0 and _position_side(pos) in ("long", "short"):
                    symbol = pos.get("symbol")
                    if symbol:
                        current_active_symbols.add(symbol)
                        if symbol not in active_tasks or active_tasks[symbol].done():
                            logger.info(f"🆕 [{symbol}] 启动独立风控守护协程")
                            active_tasks[symbol] = asyncio.create_task(watch_symbol_position(exchange, symbol))

            for symbol in list(active_tasks.keys()):
                if symbol not in current_active_symbols and active_tasks[symbol].done():
                    del active_tasks[symbol]

            await asyncio.sleep(10)

        except Exception as e:
            logger.error(f"❌ 主控循环异常: {e}", exc_info=True)
            await asyncio.sleep(10)