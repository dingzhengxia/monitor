# --- START OF FILE app/tasks/position_protection.py ---
"""Binance USDⓈ-M Futures position protection.

Rules implemented for this project:
- Check positions every 5 minutes (configured in config.json).
- Initial stop: based on NEW average entry price and 5m ATR(14),
  distance = clamp(ATR * multiplier, min_percent, max_percent).
- Trailing starts only after favorable move >= activation_atr * ATR.
- Once trailing starts, stop can only move in the profitable direction.
- When adding to an existing position, the protection cycle is RESET:
  the new average entry price becomes the new baseline and the previous
  trailing stop is intentionally ignored.
- Reducing a position does not reset the stop.

Binance USDⓈ-M Futures moved conditional orders to the Algo Order API;
this module uses CCXT's raw fapiPrivate* Algo bindings instead of the old
create_order(STOP_MARKET) path.
"""

import json
import os
import time
from pathlib import Path

import pandas as pd
from loguru import logger

STATE_FILE = "position_protection_state.json"


def _f(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"⚠️ 无法读取仓位保护状态文件：{e}，将使用空状态。")
        return {}


def _save_state(state):
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)
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
    if amt > 0:
        return "long"
    if amt < 0:
        return "short"
    return None


def _position_size(position):
    contracts = _f(position.get("contracts"))
    if contracts is not None:
        return abs(contracts)
    info = position.get("info") or {}
    return abs(_f(info.get("positionAmt"), 0) or 0)


def _entry_price(position):
    # CCXT unified field first; Binance raw entryPrice as fallback.
    for key in ("entryPrice", "average"):
        value = _f(position.get(key))
        if value and value > 0:
            return value
    info = position.get("info") or {}
    value = _f(info.get("entryPrice"))
    return value if value and value > 0 else None


def _mark_or_last(position, exchange, symbol):
    for key in ("markPrice", "lastPrice"):
        value = _f(position.get(key))
        if value and value > 0:
            return value

    ticker = exchange.fetch_ticker(symbol)
    for key in ("last", "mark", "close"):
        value = _f(ticker.get(key))
        if value and value > 0:
            return value
    return None


def _atr(exchange, symbol, timeframe, period):
    """Wilder-style ATR using OHLCV. Uses completed candles only."""
    limit = max(period + 50, 100)
    rows = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    if not rows or len(rows) < period + 2:
        raise RuntimeError(f"{symbol} {timeframe} K线不足，无法计算 ATR({period})")

    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    # Drop the currently-forming candle to avoid ATR changing every few seconds.
    if len(df) > 1:
        df = df.iloc[:-1].copy()

    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Wilder RMA = alpha 1/period, equivalent to ATR convention used by most charts.
    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    value = _f(atr.iloc[-1])
    if value is None or value <= 0:
        raise RuntimeError(f"{symbol} ATR({period}) 无有效结果")
    return value


def _distance_percent(atr, reference_price, multiplier, min_percent, max_percent):
    raw = atr * multiplier / reference_price
    return max(min_percent, min(raw, max_percent))


def _price_to_precision(exchange, symbol, price):
    return float(exchange.price_to_precision(symbol, price))


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
        raise RuntimeError(
            "当前 CCXT Binance 版本缺少 Binance Algo Order 原始接口："
            + ", ".join(missing)
        )


def _get_open_algo_orders(exchange, symbol):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    response = exchange.fapiPrivateGetOpenAlgoOrders({"symbol": market_id})
    if isinstance(response, dict):
        return response.get("orders") or response.get("data") or []
    return response or []


def _is_stop_loss_algo(order, side, position_side, current_price):
    if not isinstance(order, dict):
        return False

    status = str(order.get("algoStatus") or order.get("status") or "").upper()
    if status not in ("NEW", "TRIGGER_PENDING", ""):
        return False

    order_type = str(order.get("orderType") or order.get("type") or "").upper()
    if order_type != "STOP_MARKET":
        return False

    order_side = str(order.get("side") or "").upper()
    expected_side = "SELL" if side == "long" else "BUY"
    if order_side != expected_side:
        return False

    order_ps = str(order.get("positionSide") or "BOTH").upper()
    if position_side in ("LONG", "SHORT") and order_ps != position_side:
        return False

    trigger = _f(order.get("triggerPrice") or order.get("stopPrice"))
    if trigger is None or current_price is None:
        return True

    if side == "long":
        return trigger < current_price
    return trigger > current_price


def _find_stop_orders(exchange, symbol, side, position):
    current_price = _mark_or_last(position, exchange, symbol)
    raw_ps = _raw_position_side(position)
    orders = _get_open_algo_orders(exchange, symbol)
    result = []
    for order in orders:
        if _is_stop_loss_algo(order, side, raw_ps, current_price):
            result.append(order)
    return result


def _cancel_algo(exchange, symbol, algo_id):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    return exchange.fapiPrivateDeleteAlgoOrder(
        {"symbol": market_id, "algoId": str(algo_id)}
    )


def _cancel_stop_orders(exchange, symbol, stops):
    for order in stops:
        algo_id = order.get("algoId") or order.get("id")
        if not algo_id:
            logger.warning(f"⚠️ {symbol} 发现止损单但没有 algoId，无法取消：{order}")
            continue
        _cancel_algo(exchange, symbol, algo_id)
        logger.info(f"🧹 {symbol} 已取消旧止损 Algo {algo_id}")


def _create_stop(exchange, symbol, position, side, stop_price):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    raw_ps = _raw_position_side(position)
    qty = _position_size(position)
    if qty <= 0:
        raise RuntimeError("仓位数量为 0，不能创建止损")

    # Binance Algo API requires triggerPrice and supports quantity + reduceOnly.
    # We deliberately use quantity rather than closePosition because the current
    # USDⓈ-M Algo API is quantity/reduceOnly oriented and the bot must protect the
    # exact current position size.
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
    }

    # Binance Hedge Mode does not accept reduceOnly; positionSide + opposite
    # side already makes this a closing order. In one-way mode, reduceOnly is
    # appropriate to prevent the stop from accidentally opening a reverse position.
    if raw_ps == "BOTH":
        params["reduceOnly"] = "true"
    response = exchange.fapiPrivatePostAlgoOrder(params)
    return response


def _state_key(symbol, raw_position_side, side):
    return f"{symbol}|{raw_position_side}|{side}"


def _changed_for_addition(previous, contracts, entry):
    if not previous:
        return False

    old_contracts = _f(previous.get("contracts"), 0)
    old_entry = _f(previous.get("entry_price"))

    # A larger position is an unambiguous add.
    if contracts > old_contracts + max(old_contracts, 1.0) * 1e-8:
        return True

    # Average entry moved while size stayed roughly the same: this can also be
    # an add followed by a partial reduction between two checks.
    if old_entry and entry and abs(entry - old_entry) / old_entry > 1e-8 and contracts >= old_contracts * 0.99:
        return True

    return False


def _initial_stop(entry, side, distance_percent):
    return entry * (1 - distance_percent) if side == "long" else entry * (1 + distance_percent)


def _trailing_stop(current_price, side, distance_percent):
    return current_price * (1 - distance_percent) if side == "long" else current_price * (1 + distance_percent)


def protect_positions(exchange, config):
    conf = config.get("position_protection", {})
    if not conf.get("enabled", True):
        return

    timeframe = conf.get("atr", {}).get("timeframe", "5m")
    period = int(conf.get("atr", {}).get("period", 14))
    multiplier = float(conf.get("atr", {}).get("multiplier", 1.5))
    min_percent = float(conf.get("atr", {}).get("min_percent", 0.01))
    max_percent = float(conf.get("atr", {}).get("max_percent", 0.04))
    activation_atr = float(conf.get("trailing", {}).get("activation_atr", 1.0))

    state = _load_state()

    try:
        positions = exchange.fetch_positions()
    except Exception as e:
        logger.error(f"❌ 获取仓位失败，本次不执行止损：{e}", exc_info=True)
        return

    active = []
    active_keys = set()

    for position in positions:
        contracts = _position_size(position)
        if contracts <= 0:
            continue

        symbol = position.get("symbol")
        side = _position_side(position)
        entry = _entry_price(position)
        if not symbol or side not in ("long", "short") or not entry:
            continue

        raw_ps = _raw_position_side(position)
        key = _state_key(symbol, raw_ps, side)
        active_keys.add(key)
        active.append((position, symbol, side, raw_ps, contracts, entry, key))

    # Remove stale state for positions that no longer exist.
    for key in list(state.keys()):
        if key not in active_keys:
            del state[key]

    if not active:
        _save_state(state)
        logger.info("🛡️ 当前没有未平仓仓位。")
        return

    logger.info(f"🛡️ ATR仓位保护检查：发现 {len(active)} 个仓位。")

    for position, symbol, side, raw_ps, contracts, entry, key in active:
        try:
            current_price = _mark_or_last(position, exchange, symbol)
            if not current_price or current_price <= 0:
                logger.warning(f"⚠️ {symbol} 无法获取当前价格，跳过。")
                continue

            atr = _atr(exchange, symbol, timeframe, period)
            distance_pct = _distance_percent(
                atr, entry, multiplier, min_percent, max_percent
            )
            stops = _find_stop_orders(exchange, symbol, side, position)
            previous = state.get(key)
            is_add = _changed_for_addition(previous, contracts, entry)

            if is_add:
                # USER RULE: after adding, ignore the previous trailing stop and
                # recalculate from the NEW average entry price.
                logger.warning(
                    f"➕ {symbol} {side} 检测到加仓：旧仓位={previous.get('contracts') if previous else None}, "
                    f"新仓位={contracts}, 新均价={entry}. 重置止损周期。"
                )
                if stops:
                    _cancel_stop_orders(exchange, symbol, stops)
                    stops = []

                stop_price = _price_to_precision(
                    exchange, symbol, _initial_stop(entry, side, distance_pct)
                )
                order = _create_stop(exchange, symbol, position, side, stop_price)
                state[key] = {
                    "contracts": contracts,
                    "entry_price": entry,
                    "stop_price": stop_price,
                    "trailing_active": False,
                    "last_reset_ts": int(time.time()),
                }
                logger.success(
                    f"✅ {symbol} {side} 加仓后已重置止损："
                    f"entry={entry}, ATR={atr:.8g}, distance={distance_pct:.2%}, stop={stop_price}, "
                    f"algoId={order.get('algoId')}"
                )
                continue

            # No previous state: if a stop already exists, respect it and only
            # initialize tracking. If there is no stop, create the initial one.
            if previous is None:
                if stops:
                    existing_trigger = _f(stops[0].get("triggerPrice") or stops[0].get("stopPrice"))
                    state[key] = {
                        "contracts": contracts,
                        "entry_price": entry,
                        "stop_price": existing_trigger,
                        "trailing_active": False,
                        "last_reset_ts": int(time.time()),
                    }
                    logger.info(
                        f"🛡️ {symbol} {side} 已有止损 {existing_trigger}，首次接管但不修改。"
                    )
                    continue

                stop_price = _price_to_precision(
                    exchange, symbol, _initial_stop(entry, side, distance_pct)
                )
                order = _create_stop(exchange, symbol, position, side, stop_price)
                state[key] = {
                    "contracts": contracts,
                    "entry_price": entry,
                    "stop_price": stop_price,
                    "trailing_active": False,
                    "last_reset_ts": int(time.time()),
                }
                logger.success(
                    f"🛡️ {symbol} {side} 首次添加止损：entry={entry}, ATR={atr:.8g}, "
                    f"distance={distance_pct:.2%}, stop={stop_price}, algoId={order.get('algoId')}"
                )
                continue

            # If no stop is found but we had state, recreate it immediately.
            if not stops:
                remembered_stop = _f(previous.get("stop_price"))
                logger.warning(
                    f"⚠️ {symbol} {side} 状态显示已有止损，但交易所当前未发现止损，立即重建。"
                )
                stop_price = _price_to_precision(
                    exchange,
                    symbol,
                    remembered_stop if remembered_stop else _initial_stop(entry, side, distance_pct),
                )
                order = _create_stop(exchange, symbol, position, side, stop_price)
                previous["stop_price"] = stop_price
                logger.success(
                    f"✅ {symbol} {side} 止损已重建：{stop_price}, algoId={order.get('algoId')}"
                )
                # Continue so a missing stop is not immediately trailed in the same pass.
                state[key] = previous
                continue

            # Keep the current state synchronized after reductions.
            previous["contracts"] = contracts
            previous["entry_price"] = entry

            favorable_move = (
                (current_price - entry) / entry
                if side == "long"
                else (entry - current_price) / entry
            )
            activation_distance = activation_atr * atr / entry

            if not conf.get("trailing", {}).get("enabled", True):
                state[key] = previous
                continue

            if not previous.get("trailing_active", False):
                if favorable_move < activation_distance:
                    state[key] = previous
                    logger.debug(
                        f"⏳ {symbol} {side} 尚未启动移动止损："
                        f"盈利={favorable_move:.2%}, 需要={activation_distance:.2%}"
                    )
                    continue
                previous["trailing_active"] = True
                logger.info(
                    f"🚀 {symbol} {side} 已达到 {activation_atr:.2f} ATR，启动移动止损。"
                )

            # Trailing distance uses current ATR but is capped by configured
            # min/max percent. It can tighten as volatility contracts, but the
            # stop itself is never moved in the losing direction.
            trail_pct = _distance_percent(
                atr, current_price, multiplier, min_percent, max_percent
            )
            candidate = _price_to_precision(
                exchange,
                symbol,
                _trailing_stop(current_price, side, trail_pct),
            )

            existing_stop = min(
                [_f(o.get("triggerPrice") or o.get("stopPrice")) for o in stops if _f(o.get("triggerPrice") or o.get("stopPrice"))],
                default=None,
            ) if side == "short" else max(
                [_f(o.get("triggerPrice") or o.get("stopPrice")) for o in stops if _f(o.get("triggerPrice") or o.get("stopPrice"))],
                default=None,
            )
            if existing_stop is None:
                existing_stop = _f(previous.get("stop_price"))

            should_move = (
                candidate > existing_stop + 1e-12
                if side == "long"
                else candidate < existing_stop - 1e-12
            )

            if not should_move:
                previous["stop_price"] = existing_stop
                state[key] = previous
                continue

            logger.info(
                f"📈 {symbol} {side} 移动止损：{existing_stop} -> {candidate} | "
                f"价格={current_price} ATR={atr:.8g} 距离={trail_pct:.2%}"
            )

            # Binance Algo conditional orders do not support modifying an
            # untriggered order, so cancel old algo(s) then create the new one.
            _cancel_stop_orders(exchange, symbol, stops)
            order = _create_stop(exchange, symbol, position, side, candidate)
            previous["stop_price"] = candidate
            state[key] = previous
            logger.success(
                f"✅ {symbol} {side} 止损已向盈利方向移动：{candidate} | algoId={order.get('algoId')}"
            )

        except Exception as e:
            logger.error(
                f"❌ {symbol} {side} 仓位保护处理失败：{e}",
                exc_info=True,
            )

    _save_state(state)
    logger.info("🛡️ ATR仓位保护检查完成。")


# --- END OF FILE app/tasks/position_protection.py ---
