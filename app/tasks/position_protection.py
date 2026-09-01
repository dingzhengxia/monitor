"""Binance USDⓈ-M Futures ATR position protection.

Protection rules for this project:
- Check positions every 5 minutes (configurable).
- ATR uses a higher timeframe (default: 1h) because positions are normally held
  for days, while the protection task itself still runs every 5 minutes.
- Initial stop is calculated from the CURRENT average entry price.
- Stop distance = ATR * multiplier, clamped to min/max percentage.
- Trailing starts only after favorable price movement reaches activation_atr * ATR.
- Once trailing starts, the stop can only move in the profitable direction.
- Long trailing candidate = highest observed price - ATR * multiplier.
- Short trailing candidate = lowest observed price + ATR * multiplier.
- Adding to a position fully resets the protection cycle: new average entry,
  new ATR baseline, new initial stop, and trailing becomes inactive again.
- Reducing a position does NOT reset the trailing cycle, but the stop quantity is
  synchronized to the new position size.
- Existing stop orders are respected when the bot first takes over a position.
- Binance USDⓈ-M Futures conditional orders use the Algo Order API.

The state file is deliberately written directly instead of atomically replacing
it, because it is bind-mounted as a single file by Docker; replacing a bind
mount point with os.replace() can fail on Linux.
"""

import json
import os
import time
import uuid
from pathlib import Path

import pandas as pd
from loguru import logger

STATE_FILE = Path("position_protection_state.json")
STATE_VERSION = 2
CLIENT_ALGO_PREFIX = "PM_SL_"


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
    """Save directly to the bind-mounted file."""
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
    for key in ("entryPrice", "average"):
        value = _f(position.get(key))
        if value and value > 0:
            return value
    info = position.get("info") or {}
    value = _f(info.get("entryPrice"))
    return value if value and value > 0 else None


def _mark_or_last(position, exchange, symbol):
    info = position.get("info") or {}
    for key in ("markPrice", "markPrice", "lastPrice"):
        value = _f(position.get(key))
        if value and value > 0:
            return value
        value = _f(info.get(key))
        if value and value > 0:
            return value

    ticker = exchange.fetch_ticker(symbol)
    for key in ("mark", "last", "close"):
        value = _f(ticker.get(key))
        if value and value > 0:
            return value
    return None


def _atr(exchange, symbol, timeframe, period):
    """Wilder-style ATR using completed candles only."""
    limit = max(period + 50, 100)
    rows = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    if not rows or len(rows) < period + 2:
        raise RuntimeError(f"{symbol} {timeframe} K线不足，无法计算 ATR({period})")

    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
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


def _algo_status(order):
    return str(order.get("algoStatus") or order.get("status") or "").upper()


def _algo_trigger(order):
    return _f(order.get("triggerPrice") or order.get("stopPrice"))


def _algo_quantity(order):
    return _f(order.get("quantity") or order.get("origQty"))


def _is_stop_loss_algo(order, side, position_side, current_price):
    if not isinstance(order, dict):
        return False

    if _algo_status(order) not in ("NEW", "TRIGGER_PENDING", ""):
        return False

    order_type = str(order.get("orderType") or order.get("type") or "").upper()
    if order_type != "STOP_MARKET":
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

    # A stop-loss for a long must be below current price; for a short it must be above.
    return trigger < current_price if side == "long" else trigger > current_price


def _find_stop_orders(exchange, symbol, side, position, current_price=None):
    if current_price is None:
        current_price = _mark_or_last(position, exchange, symbol)
    raw_ps = _raw_position_side(position)
    orders = _get_open_algo_orders(exchange, symbol)
    result = []
    for order in orders:
        if _is_stop_loss_algo(order, side, raw_ps, current_price):
            result.append(order)
    return result


def _select_existing_stop(stops, side):
    valid = [(_algo_trigger(o), o) for o in stops if _algo_trigger(o) is not None]
    if not valid:
        return None, None
    # Tightest currently active stop is the most protective one.
    if side == "long":
        price, order = max(valid, key=lambda item: item[0])
    else:
        price, order = min(valid, key=lambda item: item[0])
    return price, order


def _cancel_algo(exchange, symbol, algo_id):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    return exchange.fapiPrivateDeleteAlgoOrder(
        {"symbol": market_id, "algoId": str(algo_id)}
    )


def _cancel_stop_orders(exchange, symbol, stops):
    errors = []
    for order in stops:
        algo_id = order.get("algoId") or order.get("id")
        if not algo_id:
            logger.warning(f"⚠️ {symbol} 发现止损单但没有 algoId，无法取消：{order}")
            continue
        try:
            _cancel_algo(exchange, symbol, algo_id)
            logger.info(f"🧹 {symbol} 已取消旧止损 Algo {algo_id}")
        except Exception as e:
            errors.append((algo_id, e))
            logger.error(f"❌ {symbol} 取消止损 Algo {algo_id} 失败：{e}", exc_info=True)
    if errors:
        raise RuntimeError(f"取消 {len(errors)} 个旧止损失败，拒绝继续替换止损。")


def _make_client_algo_id(symbol, side):
    # Binance allows [A-Za-z0-9_.:/-], max 36 chars. UUID avoids collisions
    # when multiple replacement orders are created in the same millisecond.
    compact = symbol.replace("/", "").replace(":", "")
    suffix = "L" if side == "long" else "S"
    value = f"{CLIENT_ALGO_PREFIX}{compact}_{suffix}_{uuid.uuid4().hex[:8]}"
    return value[:36]


def _create_stop(exchange, symbol, position, side, stop_price):
    _algo_methods(exchange)
    market_id = exchange.market(symbol)["id"]
    raw_ps = _raw_position_side(position)
    qty = _position_size(position)
    if qty <= 0:
        raise RuntimeError("仓位数量为 0，不能创建止损")

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

    # Binance explicitly forbids reduceOnly in Hedge Mode; in One-way Mode it is valid.
    if raw_ps == "BOTH":
        params["reduceOnly"] = "true"

    response = exchange.fapiPrivatePostAlgoOrder(params)
    return response


def _replace_stop(exchange, symbol, position, side, stops, new_stop):
    """Create the replacement first, then remove old stops.

    Creating first avoids a naked position if the replacement request succeeds
    but cancellation fails. Both orders are reduce-only/position-closing stops,
    so a brief overlap is safer than a gap in protection.
    """
    order = _create_stop(exchange, symbol, position, side, new_stop)
    try:
        _cancel_stop_orders(exchange, symbol, stops)
    except Exception:
        # Replacement exists, so protection is still present. Leave the new stop
        # in place and surface the error for visibility.
        logger.error(
            f"⚠️ {symbol} 新止损已创建，但旧止损未全部取消；请检查 Binance Algo Orders。",
            exc_info=True,
        )
    return order


def _state_key(symbol, raw_position_side, side):
    return f"{symbol}|{raw_position_side}|{side}"


def _changed_for_addition(previous, contracts, entry):
    if not previous:
        return False

    old_contracts = _f(previous.get("contracts"), 0)
    old_entry = _f(previous.get("entry_price"))

    if contracts > old_contracts + max(abs(old_contracts), 1.0) * 1e-8:
        return True

    # If average entry changes while position size does not materially decrease,
    # this is consistent with an add/rebalance between checks.
    if old_entry and entry and abs(entry - old_entry) / old_entry > 1e-8:
        return contracts >= old_contracts * 0.99

    return False


def _initial_stop(entry, side, distance_percent):
    return entry * (1 - distance_percent) if side == "long" else entry * (1 + distance_percent)


def _trailing_stop_from_extreme(extreme_price, side, atr, multiplier):
    distance = atr * multiplier
    return extreme_price - distance if side == "long" else extreme_price + distance


def _normalize_config(config):
    conf = config.get("position_protection", {})
    atr_conf = conf.get("atr", {})
    trailing_conf = conf.get("trailing", {})

    timeframe = str(atr_conf.get("timeframe", "1h"))
    period = max(2, int(atr_conf.get("period", 14)))
    multiplier = float(atr_conf.get("multiplier", 2.0))
    min_percent = float(atr_conf.get("min_percent", 0.015))
    max_percent = float(atr_conf.get("max_percent", 0.08))
    activation_atr = float(trailing_conf.get("activation_atr", 1.5))

    if multiplier <= 0 or activation_atr <= 0:
        raise ValueError("position_protection ATR multiplier / activation_atr 必须 > 0")
    if not (0 < min_percent <= max_percent):
        raise ValueError("position_protection min_percent / max_percent 配置无效")

    return conf, timeframe, period, multiplier, min_percent, max_percent, activation_atr


def protect_positions(exchange, config):
    try:
        conf, timeframe, period, multiplier, min_percent, max_percent, activation_atr = _normalize_config(config)
    except Exception as e:
        logger.error(f"❌ 仓位保护配置无效：{e}")
        return

    if not conf.get("enabled", True):
        return

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

    for key in list(state.keys()):
        if key not in active_keys:
            del state[key]

    if not active:
        _save_state(state)
        logger.info("🛡️ 当前没有未平仓仓位。")
        return

    logger.info(
        f"🛡️ ATR仓位保护检查：{len(active)} 个仓位 | ATR={timeframe}({period}) | "
        f"倍数={multiplier:.2f} | 启动={activation_atr:.2f} ATR | "
        f"距离限制={min_percent:.2%}~{max_percent:.2%}"
    )

    for position, symbol, side, raw_ps, contracts, entry, key in active:
        try:
            current_price = _mark_or_last(position, exchange, symbol)
            if not current_price or current_price <= 0:
                logger.warning(f"⚠️ {symbol} 无法获取当前价格，跳过。")
                continue

            atr = _atr(exchange, symbol, timeframe, period)
            distance_pct = _distance_percent(atr, entry, multiplier, min_percent, max_percent)
            stops = _find_stop_orders(exchange, symbol, side, position, current_price)
            previous = state.get(key)
            is_add = _changed_for_addition(previous, contracts, entry)

            if is_add:
                logger.warning(
                    f"➕ {symbol} {side} 检测到加仓：旧仓位={previous.get('contracts') if previous else None}, "
                    f"新仓位={contracts}, 新均价={entry}。完全重置止损周期。"
                )

                stop_price = _price_to_precision(exchange, symbol, _initial_stop(entry, side, distance_pct))
                order = _replace_stop(exchange, symbol, position, side, stops, stop_price) if stops else _create_stop(
                    exchange, symbol, position, side, stop_price
                )
                state[key] = {
                    "version": STATE_VERSION,
                    "contracts": contracts,
                    "entry_price": entry,
                    "stop_price": stop_price,
                    "trailing_active": False,
                    "extreme_price": current_price,
                    "last_reset_ts": int(time.time()),
                    "last_check_ts": int(time.time()),
                    "stop_algo_id": order.get("algoId"),
                    "stop_client_algo_id": order.get("clientAlgoId"),
                }
                logger.success(
                    f"✅ {symbol} {side} 加仓后已重置：entry={entry}, ATR={atr:.8g}, "
                    f"distance={distance_pct:.2%}, stop={stop_price}, algoId={order.get('algoId')}"
                )
                continue

            # First time the bot sees this position: respect an existing stop.
            if previous is None:
                existing_stop, existing_order = _select_existing_stop(stops, side)
                state[key] = {
                    "version": STATE_VERSION,
                    "contracts": contracts,
                    "entry_price": entry,
                    "stop_price": existing_stop,
                    "trailing_active": False,
                    "extreme_price": current_price,
                    "last_reset_ts": int(time.time()),
                    "last_check_ts": int(time.time()),
                    "stop_algo_id": existing_order.get("algoId") if existing_order else None,
                    "stop_client_algo_id": existing_order.get("clientAlgoId") if existing_order else None,
                }

                if existing_stop is not None:
                    logger.info(
                        f"🛡️ {symbol} {side} 已存在止损 {existing_stop}，首次接管但不修改。"
                    )
                    continue

                stop_price = _price_to_precision(exchange, symbol, _initial_stop(entry, side, distance_pct))
                order = _create_stop(exchange, symbol, position, side, stop_price)
                state[key]["stop_price"] = stop_price
                state[key]["stop_algo_id"] = order.get("algoId")
                state[key]["stop_client_algo_id"] = order.get("clientAlgoId")
                logger.success(
                    f"🛡️ {symbol} {side} 首次添加止损：entry={entry}, ATR={atr:.8g}, "
                    f"distance={distance_pct:.2%}, stop={stop_price}, algoId={order.get('algoId')}"
                )
                continue

            # Keep state synchronized with current position size and average entry.
            old_contracts = _f(previous.get("contracts"), contracts)
            previous["contracts"] = contracts
            previous["entry_price"] = entry
            previous["last_check_ts"] = int(time.time())

            # Track the most favorable observed price for Chandelier-style trailing.
            extreme = _f(previous.get("extreme_price"), current_price)
            if side == "long":
                extreme = max(extreme, current_price)
            else:
                extreme = min(extreme, current_price)
            previous["extreme_price"] = extreme

            existing_stop, existing_order = _select_existing_stop(stops, side)
            remembered_stop = _f(previous.get("stop_price"))
            if existing_stop is None:
                # Stop disappeared: recreate immediately. Prefer remembered stop;
                # if state has none, fall back to a fresh initial stop.
                replacement = remembered_stop or _initial_stop(entry, side, distance_pct)
                replacement = _price_to_precision(exchange, symbol, replacement)
                order = _create_stop(exchange, symbol, position, side, replacement)
                previous["stop_price"] = replacement
                previous["stop_algo_id"] = order.get("algoId")
                previous["stop_client_algo_id"] = order.get("clientAlgoId")
                state[key] = previous
                logger.warning(
                    f"⚠️ {symbol} {side} 状态显示已有止损，但交易所未发现止损，已立即重建：{replacement}"
                )
                continue

            # If the position was reduced, synchronize stop quantity while keeping
            # the existing stop price and trailing state unchanged.
            quantity_changed = old_contracts > contracts * 1.00000001
            existing_qty = _algo_quantity(existing_order) if existing_order else None
            qty_changed_on_exchange = existing_qty is not None and abs(existing_qty - contracts) > max(1e-12, contracts * 1e-8)
            if quantity_changed or qty_changed_on_exchange:
                replacement = _price_to_precision(exchange, symbol, existing_stop)
                order = _replace_stop(exchange, symbol, position, side, stops, replacement)
                previous["stop_price"] = replacement
                previous["stop_algo_id"] = order.get("algoId")
                previous["stop_client_algo_id"] = order.get("clientAlgoId")
                state[key] = previous
                logger.info(
                    f"📉 {symbol} {side} 仓位减少/止损数量不一致：{old_contracts} -> {contracts}，"
                    f"保持止损价格 {replacement}，同步保护数量。"
                )
                continue

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
                        f"⏳ {symbol} {side} 尚未启动移动止损：盈利={favorable_move:.2%}, "
                        f"需要={activation_distance:.2%}"
                    )
                    continue
                previous["trailing_active"] = True
                logger.info(
                    f"🚀 {symbol} {side} 已达到 {activation_atr:.2f} ATR，启动移动止损。"
                )

            candidate = _trailing_stop_from_extreme(extreme, side, atr, multiplier)
            candidate = _price_to_precision(exchange, symbol, candidate)

            # Enforce the configured absolute distance bounds for the trailing stop.
            # For long: candidate cannot be farther than max_percent from the extreme,
            # and cannot be tighter than min_percent. For short the direction reverses.
            trail_pct = _distance_percent(atr, extreme, multiplier, min_percent, max_percent)
            bounded_candidate = (
                extreme * (1 - trail_pct) if side == "long" else extreme * (1 + trail_pct)
            )
            candidate = _price_to_precision(exchange, symbol, bounded_candidate)

            # Never move a stop in the losing direction.
            should_move = (
                candidate > existing_stop + 1e-12
                if side == "long"
                else candidate < existing_stop - 1e-12
            )

            if not should_move:
                previous["stop_price"] = existing_stop
                previous["stop_algo_id"] = existing_order.get("algoId") if existing_order else previous.get("stop_algo_id")
                previous["stop_client_algo_id"] = existing_order.get("clientAlgoId") if existing_order else previous.get("stop_client_algo_id")
                state[key] = previous
                continue

            logger.info(
                f"📈 {symbol} {side} 移动止损：{existing_stop} -> {candidate} | "
                f"当前价={current_price} 极值={extreme} ATR={atr:.8g} 距离={trail_pct:.2%}"
            )

            order = _replace_stop(exchange, symbol, position, side, stops, candidate)
            previous["stop_price"] = candidate
            previous["stop_algo_id"] = order.get("algoId")
            previous["stop_client_algo_id"] = order.get("clientAlgoId")
            state[key] = previous
            logger.success(
                f"✅ {symbol} {side} 止损已向盈利方向移动：{candidate} | algoId={order.get('algoId')}"
            )

        except Exception as e:
            logger.error(f"❌ {symbol} {side} 仓位保护处理失败：{e}", exc_info=True)

    _save_state(state)
    logger.info("🛡️ ATR仓位保护检查完成。")
