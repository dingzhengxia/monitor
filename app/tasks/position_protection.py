# --- START OF FILE app/tasks/position_protection.py ---

from loguru import logger


# ============================================================
# 仓位止损保护
#
# 规则：
#   1. 每次检查当前所有未平仓仓位
#   2. 没有仓位 -> 什么都不做
#   3. 已经有止损 -> 什么都不做
#   4. 没有止损 -> 按当前价格 ±2% 挂止损
#
# 注意：
#   2% 是“标的价格”的波动幅度，不乘杠杆。
#
#   10x：
#       BTC 价格反向 2%
#       保证金理论亏损约 20%
#
#   20x：
#       BTC 价格反向 2%
#       保证金理论亏损约 40%
# ============================================================


def _safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_position_side(position):
    """
    优先使用 CCXT 统一字段 side。
    如果没有，再从 Binance positionSide 判断。
    """
    side = position.get("side")

    if side in ("long", "short"):
        return side

    info = position.get("info") or {}
    position_side = str(info.get("positionSide", "")).upper()

    if position_side == "LONG":
        return "long"

    if position_side == "SHORT":
        return "short"

    # 单向持仓模式下，根据 contracts 判断
    contracts = _safe_float(position.get("contracts"), 0)

    if contracts > 0:
        return "long"

    if contracts < 0:
        return "short"

    return None


def _get_position_size(position):
    """
    CCXT 通常把 contracts 返回为正数。
    Binance 原始 positionAmt 在 info 中可能带正负。
    """
    contracts = _safe_float(position.get("contracts"))

    if contracts is not None:
        return abs(contracts)

    info = position.get("info") or {}
    position_amt = _safe_float(info.get("positionAmt"))

    if position_amt is not None:
        return abs(position_amt)

    return 0.0


def _is_stop_loss_order(order, position_side, current_price):
    """
    判断一个未成交条件单是不是保护当前仓位的止损。

    不把止盈当止损。

    多仓：
        SELL + 止损价 < 当前价 => 止损

    空仓：
        BUY + 止损价 > 当前价 => 止损
    """

    if not order:
        return False

    status = str(order.get("status", "")).lower()

    if status not in ("open", "new", ""):
        return False

    order_side = str(order.get("side", "")).lower()

    if position_side == "long" and order_side != "sell":
        return False

    if position_side == "short" and order_side != "buy":
        return False

    info = order.get("info") or {}

    order_type = str(
        order.get("type")
        or info.get("type")
        or info.get("origType")
        or ""
    ).lower()

    # 明确的止损类型
    if "stop_loss" in order_type or order_type in (
        "stop_market",
        "stop",
    ):
        return True

    # 如果是 Binance Algo Order 返回的数据，也检查原始类型
    algo_type = str(
        info.get("algoType")
        or info.get("orderType")
        or info.get("origType")
        or ""
    ).lower()

    if "stop" in algo_type and "take_profit" not in algo_type:
        return True

    # 如果 CCXT 没有明确告诉我们类型，
    # 用 stopPrice + 方向判断。
    stop_price = _safe_float(
        order.get("stopPrice")
        or order.get("triggerPrice")
        or info.get("stopPrice")
        or info.get("triggerPrice")
    )

    if stop_price is None or current_price is None:
        return False

    if position_side == "long":
        return stop_price < current_price

    if position_side == "short":
        return stop_price > current_price

    return False


def _has_stop_loss(exchange, symbol, position_side, current_price):
    """
    检查指定仓位是否已经存在止损。

    注意：
    只要发现一个有效止损，就直接认为已经保护。
    不修改已有止损。
    """

    # --------------------------------------------------------
    # 第一层：直接看 CCXT position 中是否已经有 stopLossPrice
    # --------------------------------------------------------
    try:
        positions = exchange.fetch_positions([symbol])

        for p in positions:
            if p.get("symbol") != symbol:
                continue

            p_side = _get_position_side(p)

            if p_side != position_side:
                continue

            stop_loss_price = _safe_float(p.get("stopLossPrice"))

            if stop_loss_price is not None and stop_loss_price > 0:
                logger.debug(
                    f"🛡️ {symbol} {position_side} 已存在止损 "
                    f"{stop_loss_price}，跳过。"
                )
                return True

    except Exception as e:
        logger.debug(
            f"读取 {symbol} position stopLossPrice 失败：{e}"
        )

    # --------------------------------------------------------
    # 第二层：检查未成交条件单
    # --------------------------------------------------------
    try:
        orders = exchange.fetch_open_orders(symbol)
    except Exception as e:
        logger.warning(
            f"⚠️ 无法读取 {symbol} 未成交订单，"
            f"为了安全起见本次不新增止损：{e}"
        )
        return None

    for order in orders:
        if _is_stop_loss_order(
            order,
            position_side,
            current_price,
        ):
            logger.debug(
                f"🛡️ {symbol} {position_side} "
                f"发现已有止损订单 {order.get('id')}，跳过。"
            )
            return True

    return False


def _get_current_price(exchange, symbol, position):
    """
    优先使用 position 的 markPrice。
    如果没有，再 ticker。
    """

    mark_price = _safe_float(position.get("markPrice"))

    if mark_price is not None and mark_price > 0:
        return mark_price

    last_price = _safe_float(position.get("lastPrice"))

    if last_price is not None and last_price > 0:
        return last_price

    ticker = exchange.fetch_ticker(symbol)

    return _safe_float(
        ticker.get("last")
        or ticker.get("mark")
        or ticker.get("close")
    )


def _calculate_stop_price(current_price, position_side, stop_percent):
    """
    固定按照价格反向波动 stop_percent。

    多仓：
        当前价 * (1 - 2%)

    空仓：
        当前价 * (1 + 2%)
    """

    if position_side == "long":
        return current_price * (1.0 - stop_percent)

    if position_side == "short":
        return current_price * (1.0 + stop_percent)

    return None


def _create_stop_loss(
    exchange,
    symbol,
    position,
    position_side,
    stop_price,
):
    """
    创建 Binance 永续 STOP_MARKET 止损。

    使用 closePosition=True：
    触发后直接平掉当前对应方向的全部仓位。
    """

    market = exchange.market(symbol)

    info = position.get("info") or {}

    # Binance 单向模式 BOTH
    # Hedge Mode 则 LONG / SHORT
    position_side_raw = str(
        info.get("positionSide")
        or position.get("positionSide")
        or "BOTH"
    ).upper()

    if position_side == "long":
        order_side = "sell"
    else:
        order_side = "buy"

    # 按交易所价格精度处理
    stop_price = float(
        exchange.price_to_precision(symbol, stop_price)
    )

    params = {
        "stopPrice": stop_price,

        # 触发后关闭当前方向全部仓位
        "closePosition": True,

        # 使用标记价格触发，减少瞬时插针影响
        "workingType": "MARK_PRICE",

        # Binance 双向持仓需要 positionSide
        "positionSide": position_side_raw,
    }

    # closePosition=true 时不要再传 quantity/reduceOnly。
    #
    # Binance 官方接口明确要求 closePosition
    # 与 quantity / reduceOnly 互斥。
    #
    # CCXT 会根据 closePosition 参数处理条件单。
    order = exchange.create_order(
        symbol,
        "STOP_MARKET",
        order_side,
        None,
        None,
        params,
    )

    return order


def protect_positions(exchange, config):
    """
    主入口：

    每次执行：
        fetch_positions()
            ↓
        找所有实际持仓
            ↓
        判断有没有止损
            ↓
        没有 -> 当前价格 ±2%
            ↓
        挂 STOP_MARKET
    """

    protection_conf = config.get(
        "position_protection",
        {}
    )

    if not protection_conf.get("enabled", True):
        logger.debug("🛡️ 仓位止损保护未启用。")
        return

    # 固定默认 2%
    stop_percent = _safe_float(
        protection_conf.get("stop_loss_percent"),
        2.0,
    )

    if stop_percent <= 0 or stop_percent >= 100:
        logger.error(
            f"❌ position_protection.stop_loss_percent "
            f"配置错误：{stop_percent}"
        )
        return

    stop_percent = stop_percent / 100.0

    logger.info("🛡️ 开始检查当前仓位止损...")

    try:
        positions = exchange.fetch_positions()
    except Exception as e:
        logger.error(
            f"❌ 获取当前仓位失败，本次不执行止损挂单：{e}",
            exc_info=True,
        )
        return

    if not positions:
        logger.info("🛡️ 当前没有仓位。")
        return

    active_positions = []

    for position in positions:
        contracts = _get_position_size(position)

        if contracts <= 0:
            continue

        symbol = position.get("symbol")

        if not symbol:
            continue

        side = _get_position_side(position)

        if side not in ("long", "short"):
            logger.warning(
                f"⚠️ 无法判断 {symbol} 仓位方向，跳过。"
            )
            continue

        active_positions.append(
            (position, symbol, side)
        )

    if not active_positions:
        logger.info("🛡️ 当前没有实际未平仓仓位。")
        return

    logger.info(
        f"🛡️ 当前发现 {len(active_positions)} 个未平仓仓位。"
    )

    for position, symbol, side in active_positions:

        try:
            current_price = _get_current_price(
                exchange,
                symbol,
                position,
            )

            if current_price is None or current_price <= 0:
                logger.warning(
                    f"⚠️ {symbol} 无法获取当前价格，跳过。"
                )
                continue

            # ------------------------------------------------
            # 已经有止损：绝对不修改
            # ------------------------------------------------
            has_stop = _has_stop_loss(
                exchange,
                symbol,
                side,
                current_price,
            )

            # None = 查询订单失败
            # 为了安全，查询失败绝不盲目挂第二个止损。
            if has_stop is None:
                logger.warning(
                    f"⚠️ {symbol} {side} 无法确认已有止损，"
                    f"本次不新增止损。"
                )
                continue

            if has_stop:
                continue

            # ------------------------------------------------
            # 没有止损 -> 当前价格 ±2%
            # ------------------------------------------------
            stop_price = _calculate_stop_price(
                current_price,
                side,
                stop_percent,
            )

            stop_price = float(
                exchange.price_to_precision(
                    symbol,
                    stop_price,
                )
            )

            logger.warning(
                f"🚨 {symbol} {side} 没有止损！"
                f"当前价={current_price}, "
                f"止损价={stop_price}, "
                f"价格距离={stop_percent * 100:.2f}%"
            )

            order = _create_stop_loss(
                exchange,
                symbol,
                position,
                side,
                stop_price,
            )

            logger.success(
                f"✅ {symbol} {side} 止损已添加："
                f"{stop_price} | "
                f"订单ID={order.get('id')}"
            )

        except Exception as e:
            logger.error(
                f"❌ {symbol} {side} 添加止损失败：{e}",
                exc_info=True,
            )

    logger.info("🛡️ 仓位止损检查完成。")


# --- END OF FILE app/tasks/position_protection.py ---