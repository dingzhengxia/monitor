# --- START OF FILE app/analysis/strategies.py ---
from datetime import datetime, timezone
from loguru import logger
import pandas as pd
import pandas_ta as pta

from app.analysis.order_blocks import find_latest_order_blocks
from app.state import alerted_states, save_alert_states
from app.services.notification_service import send_alert
from app.services.data_fetcher import fetch_funding_rate
from app.analysis.trend import get_current_trend, timeframe_to_minutes
# 引入最新的波段寻找函数
from app.analysis.levels import find_price_interest_zones, calculate_pivot_points, find_market_structure_swings
from app.analysis.channels import detect_regression_channel
from app.analysis.indicators import (
    get_dynamic_volume_multiplier, get_dynamic_atr_multiplier, is_realtime_volume_over,
    get_dynamic_consecutive_candles
)
from app.utils import calculate_cooldown_time


def _get_params_for_timeframe(base_params: dict, timeframe: str) -> dict:
    final_params = base_params.copy()
    overrides = base_params.get("overrides_by_timeframe", {})
    if timeframe in overrides:
        final_params.update(overrides[timeframe])
    return final_params


def _prepare_and_send_notification(config, symbol, timeframe, df, signal_info):
    now_utc = datetime.now(timezone.utc)
    tf_minutes = timeframe_to_minutes(timeframe)
    params = config['strategy_params']
    market_settings = config.get('market_settings', {})

    alert_key = signal_info['alert_key']
    if alerted_states.get(alert_key) and now_utc < alerted_states[alert_key]:
        return

    vol_text = ""
    volume_label = ""
    trend_status, trend_emoji = "趋势未知", "📊"

    if df is not None:
        static_bases = market_settings.get('static_symbols', [])
        symbol_base = symbol.split('/')[0].split(':')[0]
        is_static_symbol = symbol_base in static_bases

        exemption_enabled_for_this_strategy = signal_info.get('exempt_static_on_volume', False)
        original_volume_confirm = signal_info.get('volume_must_confirm', False)
        final_volume_confirm = False if (
                exemption_enabled_for_this_strategy and is_static_symbol) else original_volume_confirm

        raw_lb_params = params.get('level_breakout', {})
        breakout_params = raw_lb_params[0] if isinstance(raw_lb_params, list) else raw_lb_params

        dynamic_multiplier = get_dynamic_volume_multiplier(symbol, config, signal_info.get('fallback_multiplier', 1.5))
        is_vol_over, v_text, actual_vol_ratio = is_realtime_volume_over(
            df, tf_minutes, breakout_params.get('volume_ma_period', 20), dynamic_multiplier
        )

        if final_volume_confirm and not is_vol_over:
            logger.debug(f"[{symbol}|{timeframe}] 信号 '{signal_info.get('log_name', 'N/A')}' 因成交量不足被过滤。")
            return

        volume_label = f"放量({actual_vol_ratio:.1f}x) " if is_vol_over else f"缩量({actual_vol_ratio:.1f}x) "
        if v_text and signal_info.get('always_show_volume', True):
            vol_text = f"\n---\n{v_text}"

        trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, params)

    title = signal_info['title_template'].format(vol_label=volume_label).replace("  ", " ").strip()

    message_data = signal_info.get('template_data', {})
    message_data['trend_message'] = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"
    message_data['vol_text'] = vol_text

    message = signal_info['message_template'].format(**message_data)

    send_alert(config, title, message, symbol)

    if signal_info.get('cooldown_logic') == 'align_to_period_end':
        alerted_states[alert_key] = calculate_cooldown_time(tf_minutes, align_to_period_end=True)
    else:
        cooldown_minutes = tf_minutes * signal_info.get('cooldown_mult', 1)
        alerted_states[alert_key] = calculate_cooldown_time(cooldown_minutes)

    save_alert_states()


def check_ma_breakout(exchange, symbol, timeframe, config, df, ma_params, config_index=0):
    try:
        ma_periods = ma_params.get('ma_periods', [7, 25, 99])
        ma_type = ma_params.get('ma_type', 'sma').lower()

        for period in ma_periods:
            col_name = f"{ma_type}_{period}"
            if ma_type == 'ema':
                df[col_name] = pta.ema(df['close'], length=period)
            else:
                df[col_name] = pta.sma(df['close'], length=period)

        df_cleaned = df.dropna().reset_index(drop=True)
        if len(df_cleaned) < 2: return

        current = df_cleaned.iloc[-1]
        prev = df_cleaned.iloc[-2]

        # 输出 MA 数值到日志
        ma_log_list = []
        for period in ma_periods:
            col_name = f"{ma_type}_{period}"
            if col_name in current:
                ma_log_list.append(f"{ma_type.upper()}{period}: {current[col_name]:.4f}")
        if ma_log_list:
            logger.debug(
                f"[{symbol}|{timeframe}] 📈 均线计算完毕 -> 当前价: {current['close']:.4f} | 均线: {', '.join(ma_log_list)}")

        for period in ma_periods:
            col_name = f"{ma_type}_{period}"
            if col_name not in current: continue

            ma_val = current[col_name]
            prev_ma_val = prev[col_name]

            bullish = prev['close'] < prev_ma_val and current['close'] > ma_val
            bearish = prev['close'] > prev_ma_val and current['close'] < ma_val

            if bullish or bearish:
                action = "突破" if bullish else "跌破"
                emoji = "🚀" if bullish else "📉"

                signal_info = {
                    'log_name': f'MA Breakout ({period})',
                    'alert_key': f"{symbol}_{timeframe}_MA_{action}_{period}_{config_index}",
                    'volume_must_confirm': ma_params.get('volume_confirm', True),
                    'fallback_multiplier': ma_params.get('volume_multiplier', 1.5),
                    'title_template': f"{emoji} {{vol_label}}{action} {ma_type.upper()}{period}: {symbol} ({timeframe})",
                    'message_template': (
                        "{trend_message}**信号**: 价格实时 **{action}** {ma_type.upper()}({period}) 均线。\n\n"
                        "> **当前价**: `{current_close:.4f}`\n"
                        "> **均线值**: `{ma_value:.4f}`\n\n"
                        "{vol_text}"
                    ),
                    'template_data': {"action": action, "period": period, "ma_type": ma_type.upper(),
                                      "current_close": current['close'], "ma_value": ma_val},
                    'cooldown_mult': 1
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (MA突破监控) 中出错: {e}", exc_info=True)


def check_level_breakout(exchange, symbol, timeframe, config, df, breakout_params, config_index=0):
    try:
        level_conf = breakout_params.get('level_detection', {})
        if not level_conf.get('method') == 'advanced': return
        df.ta.atr(length=breakout_params.get('atr_period', 14), append=True)
        df_cleaned = df.dropna().reset_index(drop=True)
        if len(df_cleaned) < 3: return
        current = df_cleaned.iloc[-1]
        prev = df_cleaned.iloc[-2]
        all_levels = []

        # 1. 引入实战级别的“近期前高/前低 (Swing Pivots)”
        if level_conf.get('swing_pivots', {}).get('enabled', True):
            left_bars = level_conf.get('swing_pivots', {}).get('left_bars', 7)
            right_bars = level_conf.get('swing_pivots', {}).get('right_bars', 7)
            swings = find_market_structure_swings(df.copy(), left_bars, right_bars)
            all_levels.extend(swings)

        # 2. 保留聚类算法(如果开启)
        if level_conf.get('clustering', {}).get('enabled', False):
            cluster_conf = level_conf['clustering']
            atr_group_mult = cluster_conf.get('atr_grouping_multiplier', 0.5)
            min_size = cluster_conf.get('min_cluster_size', 2)
            min_sep = cluster_conf.get('min_separation_atr_mult', 0.6)
            price_zones = find_price_interest_zones(df.copy(), atr_group_mult, min_size, min_sep)
            all_levels.extend(price_zones)

        # 3. 保留近期震荡箱体边界
        if level_conf.get('rolling_pivots', {}).get('enabled', False):
            period = breakout_params.get('breakout_period', 120)
            if len(df_cleaned) > period:
                lookback_df = df_cleaned.iloc[-period - 2:-2]
                if not lookback_df.empty:
                    window_high = lookback_df['high'].max()
                    window_low = lookback_df['low'].min()
                    all_levels.append({'level': window_high, 'type': f'箱体顶部(近{period}根K线)'})
                    all_levels.append({'level': window_low, 'type': f'箱体底部(近{period}根K线)'})

        if not all_levels: return
        prev_price = prev['close']
        resistances = sorted([lvl for lvl in all_levels if lvl['level'] > prev_price], key=lambda x: x['level'])
        supports = sorted([lvl for lvl in all_levels if lvl['level'] < prev_price], key=lambda x: x['level'],
                          reverse=True)

        if resistances or supports:
            res_str = ", ".join([f"{r['level']:.2f}({r.get('type', 'N/A')})" for r in resistances[:2]])
            sup_str = ", ".join([f"{s['level']:.2f}({s.get('type', 'N/A')})" for s in supports[:2]])
            logger.debug(
                f"[{symbol}|{timeframe}] 🎯 实战支撑阻力 -> 当前价: {current['close']:.2f} | 阻力: [{res_str}] | 支撑: [{sup_str}]")

        atr_val = current.get(f"ATRr_{breakout_params.get('atr_period', 14)}", 0.0)
        if atr_val == 0: return
        atr_break_multiplier = breakout_params.get('atr_multiplier_breakout', 0.1)
        atr_break_buffer = atr_val * atr_break_multiplier

        # --- 阻力位逻辑 (突破上方结构) ---
        if resistances:
            closest_res = resistances[0]
            cond_below_res = prev['close'] < closest_res['level']
            is_breakout = cond_below_res and current['close'] > closest_res['level'] + atr_break_buffer
            is_testing_res = cond_below_res and current['high'] >= closest_res['level'] and not is_breakout

            level_type_str = closest_res.get('type', '阻力位')

            if is_breakout:
                signal_info = {
                    'log_name': 'Level Breakout BOS',
                    'alert_key': f"{symbol}_{timeframe}_BOS_UP_{config_index}_{current['timestamp']}",
                    'volume_must_confirm': breakout_params.get('volume_confirm', True),
                    'fallback_multiplier': breakout_params.get('volume_multiplier', 1.5),
                    'title_template': f"🚨 {{vol_label}}多头结构破坏(BOS): {symbol} ({timeframe})",
                    'message_template': ("{trend_message}**信号**: **强势突破关键压力！**\n\n"
                                         f"**形态学**: 价格突破了 `{level_type_str}`，构成多头市场结构破坏 (Bullish BOS)。\n"
                                         f"> **阻力价位**: `{closest_res['level']:.4f}`\n"
                                         f"> **突破价格**: `{current['close']:.4f}`\n\n"
                                         "这是典型的右侧追多/右侧入场信号。\n\n{vol_text}"),
                    'template_data': {}, 'cooldown_mult': 1
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
            elif is_testing_res:
                signal_info = {
                    'log_name': 'Level Testing Res',
                    'alert_key': f"{symbol}_{timeframe}_testing_res_{config_index}_{current['timestamp']}",
                    'volume_must_confirm': False,
                    'title_template': f"⚠️ 测试上方阻力: {symbol} ({timeframe})",
                    'message_template': ("{trend_message}**信号**: **价格正在摸顶/插针试探上方阻力**。\n\n"
                                         f"**形态学**: 价格最高点触及了 `{level_type_str}`。\n"
                                         f"> **阻力价位**: `{closest_res['level']:.4f}`\n"
                                         f"> **当前最高价**: `{current['high']:.4f}`\n"
                                         "请留意是否形成受阻回落，或蓄力完成突破(BOS)。\n\n{vol_text}"),
                    'template_data': {}, 'cooldown_mult': 1, 'always_show_volume': True
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

        # --- 支撑位逻辑 (跌破下方结构) ---
        if supports:
            closest_sup = supports[0]
            cond_above_sup = prev['close'] > closest_sup['level']
            is_breakdown = cond_above_sup and current['close'] < closest_sup['level'] - atr_break_buffer
            is_testing_sup = cond_above_sup and current['low'] <= closest_sup['level'] and not is_breakdown

            level_type_str = closest_sup.get('type', '支撑位')

            if is_breakdown:
                signal_info = {
                    'log_name': 'Level Breakdown BOS',
                    'alert_key': f"{symbol}_{timeframe}_BOS_DOWN_{config_index}_{current['timestamp']}",
                    'volume_must_confirm': breakout_params.get('volume_confirm', True),
                    'fallback_multiplier': breakout_params.get('volume_multiplier', 1.5),
                    'title_template': f"📉 {{vol_label}}空头结构破坏(BOS): {symbol} ({timeframe})",
                    'message_template': ("{trend_message}**信号**: **有效跌破关键支撑！**\n\n"
                                         f"**形态学**: 价格跌破了 `{level_type_str}`，构成空头市场结构破坏 (Bearish BOS)。\n"
                                         f"> **支撑价位**: `{closest_sup['level']:.4f}`\n"
                                         f"> **跌破价格**: `{current['close']:.4f}`\n\n"
                                         "这是典型的右侧做空/破位离场信号。\n\n{vol_text}"),
                    'template_data': {}, 'cooldown_mult': 1
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
            elif is_testing_sup:
                signal_info = {
                    'log_name': 'Level Testing Sup',
                    'alert_key': f"{symbol}_{timeframe}_testing_sup_{config_index}_{current['timestamp']}",
                    'volume_must_confirm': False,
                    'title_template': f"💡 测试下方支撑: {symbol} ({timeframe})",
                    'message_template': ("{trend_message}**信号**: **价格插针/试探关键支撑**。\n\n"
                                         f"**形态学**: 价格最低点触及了 `{level_type_str}`。\n"
                                         f"> **支撑价位**: `{closest_sup['level']:.4f}`\n"
                                         f"> **当前最低价**: `{current['low']:.4f}`\n"
                                         "请留意是否企稳反弹，或无力防守破位下行。\n\n{vol_text}"),
                    'template_data': {}, 'cooldown_mult': 1, 'always_show_volume': True
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (关键位突破) 中出错: {e}", exc_info=True)


def check_order_block_interaction(exchange, symbol, timeframe, config, df, ob_params, config_index=0):
    try:
        swing_length = ob_params.get('swing_length', 10)
        atr_multiplier = ob_params.get('atr_multiplier', 0.1)
        bull_ob, bear_ob = find_latest_order_blocks(df.copy(), swing_length, atr_multiplier)

        # 收集所有存在的OB，不再区分名字，只看绝对价格区间 (解决极性转换倒挂问题)
        all_obs = []
        if bear_ob: all_obs.append(bear_ob)
        if bull_ob: all_obs.append(bull_ob)

        if not all_obs:
            return

        current = df.iloc[-1]
        prev = df.iloc[-2]

        # 输出订单块到日志
        ob_logs = []
        for ob in all_obs:
            t = "熊市OB" if ob['type'] == 'bearish' else "牛市OB"
            ob_logs.append(f"{t}: [{ob['bottom']:.4f}-{ob['top']:.4f}]")
        logger.debug(f"[{symbol}|{timeframe}] 🧱 订单块计算完毕 -> {', '.join(ob_logs)}")

        for ob in all_obs:
            top, bottom = ob['top'], ob['bottom']
            ob_type_name = "熊市OB(原阻力)" if ob['type'] == 'bearish' else "牛市OB(原支撑)"

            # 场景A：价格从下方接近OB -> 此时OB充当【阻力】
            if prev['close'] < bottom:
                if ob_params.get('alert_on_rejection', True) and bottom <= current['close'] <= top:
                    signal_info = {
                        'log_name': 'OrderBlock Testing Resistance',
                        'alert_key': f"{symbol}_{timeframe}_OB_TEST_RES_{ob['timestamp']}",
                        'volume_must_confirm': False,
                        'title_template': f"⚠️ {symbol} ({timeframe}) 测试关键阻力区",
                        'message_template': ("{trend_message}**信号**: 价格**向上进入**由前期 {ob_name} 形成的**阻力区**。\n\n"
                                             "> **阻力区间**: `{bottom:.4f} - {top:.4f}`\n"
                                             "> **当前价格**: `{current_close:.4f}`\n\n"
                                             "请关注此处是否受阻回落，或蓄力突破。\n\n"
                                             "{vol_text}"),
                        'template_data': {"bottom": bottom, "top": top, "current_close": current['close'],
                                          "ob_name": ob_type_name},
                        'cooldown_mult': 2, 'always_show_volume': True
                    }
                    _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

                elif ob_params.get('alert_on_breakout', False) and current['close'] > top:
                    signal_info = {
                        'log_name': 'OrderBlock Breakout Up',
                        'alert_key': f"{symbol}_{timeframe}_OB_BREAK_UP_{ob['timestamp']}",
                        'volume_must_confirm': True, 'fallback_multiplier': 1.8,
                        'title_template': f"🚀 {{vol_label}}强势突破阻力区: {symbol} ({timeframe})",
                        'message_template': ("{trend_message}**信号**: 价格**已向上突破**由 {ob_name} 形成的阻力区！\n\n"
                                             "> **原阻力区间**: `{bottom:.4f} - {top:.4f}`\n"
                                             "> **突破价格**: `{current_close:.4f}`\n\n"
                                             "阻力现已转化为支撑。\n\n"
                                             "{vol_text}"),
                        'template_data': {"bottom": bottom, "top": top, "current_close": current['close'],
                                          "ob_name": ob_type_name},
                        'cooldown_mult': 4
                    }
                    _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

            # 场景B：价格从上方接近OB -> 此时OB充当【支撑】
            elif prev['close'] > top:
                if ob_params.get('alert_on_rejection', True) and bottom <= current['close'] <= top:
                    signal_info = {
                        'log_name': 'OrderBlock Testing Support',
                        'alert_key': f"{symbol}_{timeframe}_OB_TEST_SUP_{ob['timestamp']}",
                        'volume_must_confirm': False,
                        'title_template': f"💡 {symbol} ({timeframe}) 测试关键支撑区",
                        'message_template': ("{trend_message}**信号**: 价格**向下进入**由前期 {ob_name} 形成的**支撑区**。\n\n"
                                             "> **支撑区间**: `{bottom:.4f} - {top:.4f}`\n"
                                             "> **当前价格**: `{current_close:.4f}`\n\n"
                                             "请关注此处是否获得支撑企稳，或无力跌破。\n\n"
                                             "{vol_text}"),
                        'template_data': {"bottom": bottom, "top": top, "current_close": current['close'],
                                          "ob_name": ob_type_name},
                        'cooldown_mult': 2, 'always_show_volume': True
                    }
                    _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

                elif ob_params.get('alert_on_breakout', False) and current['close'] < bottom:
                    signal_info = {
                        'log_name': 'OrderBlock Breakout Down',
                        'alert_key': f"{symbol}_{timeframe}_OB_BREAK_DOWN_{ob['timestamp']}",
                        'volume_must_confirm': True, 'fallback_multiplier': 1.8,
                        'title_template': f"📉 {{vol_label}}有效跌破支撑区: {symbol} ({timeframe})",
                        'message_template': ("{trend_message}**信号**: 价格**已向下砸穿**由 {ob_name} 形成的支撑区！\n\n"
                                             "> **原支撑区间**: `{bottom:.4f} - {top:.4f}`\n"
                                             "> **跌破价格**: `{current_close:.4f}`\n\n"
                                             "支撑现已转化为强阻力。\n\n"
                                             "{vol_text}"),
                        'template_data': {"bottom": bottom, "top": top, "current_close": current['close'],
                                          "ob_name": ob_type_name},
                        'cooldown_mult': 4
                    }
                    _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (订单块交互) 中出错: {e}", exc_info=True)


# 下面保留原有未修改的策略：ema_cross, kdj_cross, volatility_breakout, rsi_divergence, channel_breakout, consecutive_candles, funding_rate...
def check_ema_signals(exchange, symbol, timeframe, config, df, ema_params, config_index=0):
    try:
        atr_period = ema_params.get('atr_period', 14)
        atr_multiplier = ema_params.get('atr_multiplier', 0.3)
        df.ta.atr(length=atr_period, append=True)
        ema_period = ema_params.get('period', 120)
        indicator_result = df.ta.ema(length=ema_period, append=True)
        if indicator_result is None or indicator_result.empty: return
        ema_col = indicator_result.columns[0] if isinstance(indicator_result, pd.DataFrame) else indicator_result.name
        df_cleaned = df.dropna(subset=[ema_col]).reset_index(drop=True)
        if len(df_cleaned) < 2: return
        current, prev = df_cleaned.iloc[-1], df_cleaned.iloc[-2]
        atr_col = f"ATRr_{atr_period}"
        if pd.isna(current.get(atr_col)) or current.get(atr_col, 0) == 0: return
        atr_val = current[atr_col]
        atr_buffer = atr_val * atr_multiplier
        bullish = (current['close'] > current[ema_col] + atr_buffer) and (prev['close'] < prev[ema_col])
        bearish = (current['close'] < current[ema_col] - atr_buffer) and (prev['low'] > prev[ema_col])
        if bullish or bearish:
            action = "有效突破" if bullish else "有效跌破"
            breakout_distance = abs(current['close'] - current[ema_col])
            breakout_atr_ratio = (breakout_distance / atr_val) if atr_val > 0 else float('inf')
            signal_info = {
                'log_name': 'EMA Cross', 'alert_key': f"{symbol}_{timeframe}_EMACROSS_{config_index}",
                'volume_must_confirm': ema_params.get('volume_confirm', False),
                'fallback_multiplier': ema_params.get('volume_multiplier', 1.5),
                'title_template': f"🚀 EMA {{vol_label}}{action}: {symbol} ({timeframe})",
                'message_template': ("{trend_message}**信号**: 价格 **实时{action}** EMA({period})。\n\n"
                                     "> **当前价**: {current_close:.4f}\n> **EMA值**: {ema_value:.4f}\n> **突破力度**: **{breakout_atr_ratio:.1f} 倍 ATR**\n\n{vol_text}"),
                'template_data': {"action": action, "period": ema_period, "current_close": current['close'],
                                  "ema_value": current[ema_col], "breakout_atr_ratio": breakout_atr_ratio},
                'cooldown_mult': 1
            }
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ EMA信号错: {e}")


def check_kdj_cross(exchange, symbol, timeframe, config, df, kdj_params, config_index=0):
    try:
        indicator_result = df.ta.kdj(fast=kdj_params.get('fast_k', 9), slow=kdj_params.get('slow_k', 3),
                                     signal=kdj_params.get('slow_d', 3), append=True)
        if indicator_result is None or indicator_result.empty: return
        k_col, d_col = indicator_result.columns[0], indicator_result.columns[1]
        df_cleaned = df.dropna(subset=[k_col, d_col]).reset_index(drop=True)
        if len(df_cleaned) < 2: return
        current, prev = df_cleaned.iloc[-1], df_cleaned.iloc[-2]
        golden = current[k_col] > current[d_col] and prev[k_col] <= prev[d_col]
        death = current[k_col] < current[d_col] and prev[k_col] >= prev[d_col]
        if not (golden or death): return
        trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, config['strategy_params'])
        signal_type_desc = ""
        if "多头" in trend_status:
            signal_type_desc = "顺势看涨 (入场机会)" if golden else "回调警示 (减仓风险)"
        elif "空头" in trend_status:
            signal_type_desc = "顺势看跌 (入场机会)" if death else "反弹警示 (空单止盈)"
        else:
            signal_type_desc = "震荡金叉 (反弹机会)" if golden else "震荡死叉 (下跌机会)"
        if not signal_type_desc: return
        emoji = {"看涨": "📈", "看跌": "📉", "警示": "⚠️", "金叉": "📈", "死叉": "📉", "机会": "💡"}.get(
            signal_type_desc.split(' ')[0].replace("顺势", "").replace("震荡", ""), "⚙️")
        signal_info = {
            'log_name': 'KDJ Cross', 'alert_key': f"{symbol}_{timeframe}_KDJ_{config_index}",
            'volume_must_confirm': kdj_params.get('volume_confirm', True),
            'fallback_multiplier': kdj_params.get('volume_multiplier', 1.5),
            'title_template': f"{emoji} KDJ {{vol_label}}信号: {signal_type_desc} ({symbol} {timeframe})",
            'message_template': (
                "{trend_message}**信号解读**: {signal_type_desc}信号出现。\n\n**当前K/D值**: {k_val:.2f} / {d_val:.2f}\n**当前价**: {price:.4f}\n\n{vol_text}"),
            'template_data': {"signal_type_desc": signal_type_desc, "k_val": current[k_col], "d_val": current[d_col],
                              "price": current['close']},
            'cooldown_mult': 0.5
        }
        _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ KDJ信号错: {e}")


def check_volatility_breakout(exchange, symbol, timeframe, config, df, vol_params, config_index=0):
    try:
        atr_period = vol_params.get('atr_period', 14)
        dynamic_atr_multiplier = get_dynamic_atr_multiplier(symbol, config, vol_params.get('atr_multiplier', 2.5))
        df.ta.atr(length=atr_period, append=True)
        atr_col = f"ATRr_{atr_period}"
        if atr_col not in df.columns: return
        df_cleaned = df.dropna(subset=[atr_col]).reset_index(drop=True)
        if len(df_cleaned) < 2: return
        current, prev = df_cleaned.iloc[-1], df_cleaned.iloc[-2]
        if pd.isna(prev.get(atr_col)) or prev.get(atr_col, 0) == 0: return
        current_volatility = current['high'] - current['low']
        reference_atr = prev[atr_col]
        if current_volatility > reference_atr * dynamic_atr_multiplier:
            actual_atr_ratio = current_volatility / reference_atr
            signal_info = {
                'log_name': 'Volatility Breakout', 'alert_key': f"{symbol}_{timeframe}_VOLATILITY_{config_index}",
                'volume_must_confirm': vol_params.get('volume_confirm', True),
                'fallback_multiplier': vol_params.get('volume_multiplier', 2.0),
                'title_template': f"💥 {{vol_label}}盘中波动异常: {symbol} ({timeframe})",
                'message_template': (
                    "{trend_message}**波动分析**:\n> **当前波幅**: `{current_volatility:.4f}` **({actual_atr_ratio:.1f}倍)**\n> **参考ATR**: `{reference_atr:.4f}`\n\n{vol_text}"),
                'template_data': {"current_volatility": current_volatility, "actual_atr_ratio": actual_atr_ratio,
                                  "reference_atr": reference_atr},
                'cooldown_mult': 1
            }
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 波动率信号错: {e}")


def check_rsi_divergence(exchange, symbol, timeframe, config, df, rsi_params, config_index=0):
    try:
        indicator_result = pta.rsi(df['close'], length=rsi_params.get('rsi_period', 14))
        if indicator_result is None or indicator_result.empty: return
        df['rsi'] = indicator_result
        df_cleaned = df.dropna(subset=['rsi']).reset_index(drop=True)
        lookback = rsi_params.get('lookback_period', 60)
        if len(df_cleaned) < lookback + 1: return
        recent_df, current = df_cleaned.iloc[-lookback - 1:-1], df_cleaned.iloc[-1]
        if current['close'] > recent_df['close'].max() and current['rsi'] < recent_df['rsi'].max():
            signal_info = {'log_name': 'RSI Top Div', 'alert_key': f"{symbol}_{timeframe}_DIV_TOP_{config_index}",
                           'volume_must_confirm': False, 'title_template': f"🚩 RSI顶背离风险: {symbol} ({timeframe})",
                           'message_template': "{trend_message}**信号**: 价格创近期新高，但RSI指标衰弱。\n\n{vol_text}",
                           'template_data': {}, 'cooldown_mult': 2, 'always_show_volume': True}
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
        if current['close'] < recent_df['close'].min() and current['rsi'] > recent_df['rsi'].min():
            signal_info = {'log_name': 'RSI Bottom Div', 'alert_key': f"{symbol}_{timeframe}_DIV_BOT_{config_index}",
                           'volume_must_confirm': False, 'title_template': f"⛳️ RSI底背离机会: {symbol} ({timeframe})",
                           'message_template': "{trend_message}**信号**: 价格创近期新低，但RSI指标企稳。\n\n{vol_text}",
                           'template_data': {}, 'cooldown_mult': 2, 'always_show_volume': True}
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ RSI背离错: {e}")


def check_trend_channel_breakout(exchange, symbol, timeframe, config, df, channel_params, config_index=0):
    try:
        if 'lookback_period' not in channel_params: return
        df.ta.atr(length=14, append=True)
        atr_col = "ATRr_14"
        if atr_col not in df.columns: return

        df_for_channel = df.copy()
        df_for_channel['symbol'] = symbol
        df_for_channel['timeframe'] = timeframe

        channel_info = detect_regression_channel(
            df_for_channel,
            lookback_period=channel_params.get('lookback_period'),
            min_trend_length=channel_params.get('min_trend_length', 20),
            std_dev_multiplier=channel_params.get('std_dev_multiplier', 2.0)
        )

        # 如果算法认为当前是震荡市，或者单边趋势太短，就会返回 None
        if not channel_info or len(df) < 3: return

        current, prev = df.iloc[-1], df.iloc[-2]
        current_upper_band = channel_info['upper_band'].iloc[-1]
        prev_upper_band = channel_info['upper_band'].iloc[-2]
        current_lower_band = channel_info['lower_band'].iloc[-1]
        prev_lower_band = channel_info['lower_band'].iloc[-2]
        confirmation_buffer = current.get(atr_col, 0) * channel_params.get('breakout_confirmation_atr', 0.0)

        # V--- 新增：输出回归通道计算结果到日志 (Debug级别) ---V
        trend_dir = "↘️下降趋势" if channel_info['slope'] < 0 else "↗️上升趋势"
        logger.debug(
            f"[{symbol}|{timeframe}] 🛤️ 通道计算完毕 -> {trend_dir} (已持续 {channel_info['trend_length']} 根K线) | 当前价: {current['close']:.2f} | 通道上轨: {current_upper_band:.2f} | 通道下轨: {current_lower_band:.2f}")
        # ^-------------------------------------------------^

        # 信号1：突破下降趋势的回归通道 (看涨)
        if channel_info['slope'] < 0:
            if prev['close'] < prev_upper_band and current['close'] > current_upper_band + confirmation_buffer:
                signal_info = {
                    'log_name': f"Channel Up", 'alert_key': f"{symbol}_{timeframe}_CHAN_UP_{config_index}",
                    'volume_must_confirm': channel_params.get('volume_confirm', True),
                    'fallback_multiplier': channel_params.get('volume_multiplier', 1.8),
                    'title_template': f"📈 {{vol_label}}突破回归通道: {symbol} ({timeframe})",
                    'message_template': (
                        "{trend_message}**信号**: **确认突破下降回归通道**。\n\n> **突破价格**: `{current_close:.4f}`\n\n{vol_text}"),
                    'template_data': {"current_close": current['close']}, 'cooldown_mult': 4
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

        # 信号2：跌破上升趋势的回归通道 (看跌)
        elif channel_info['slope'] > 0:
            if prev['close'] > prev_lower_band and current['close'] < current_lower_band - confirmation_buffer:
                signal_info = {
                    'log_name': f"Channel Down", 'alert_key': f"{symbol}_{timeframe}_CHAN_DOWN_{config_index}",
                    'volume_must_confirm': channel_params.get('volume_confirm', True),
                    'fallback_multiplier': channel_params.get('volume_multiplier', 1.8),
                    'title_template': f"📉 {{vol_label}}跌破回归通道: {symbol} ({timeframe})",
                    'message_template': (
                        "{trend_message}**信号**: **确认跌破上升回归通道**。\n\n> **跌破价格**: `{current_close:.4f}`\n\n{vol_text}"),
                    'template_data': {"current_close": current['close']}, 'cooldown_mult': 4
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 通道突破错: {e}", exc_info=True)


def check_consecutive_candles(exchange, symbol, timeframe, config, df, consecutive_params, config_index=0):
    try:
        min_n_to_alert = get_dynamic_consecutive_candles(symbol, config,
                                                         consecutive_params.get('min_consecutive_candles', 4))
        if len(df) < min_n_to_alert + 2: return

        def count_backwards(start_index, direction):
            count = 0
            for i in range(start_index, -1, -1):
                candle = df.iloc[i]
                current_direction = 'up' if candle['close'] > candle['open'] else (
                    'down' if candle['close'] < candle['open'] else 'none')
                if current_direction == direction:
                    count += 1
                else:
                    break
            return count

        last_candle, prev_candle = df.iloc[-2], df.iloc[-3]
        is_last_up, is_last_down = last_candle['close'] > last_candle['open'], last_candle['close'] < last_candle[
            'open']
        is_prev_up, is_prev_down = prev_candle['close'] > prev_candle['open'], prev_candle['close'] < prev_candle[
            'open']

        if is_last_up and is_prev_down:
            if (c := count_backwards(len(df) - 3, 'down')) >= min_n_to_alert:
                signal_info = {'alert_key': f"{symbol}_{timeframe}_REV_UP_{config_index}_{last_candle['timestamp']}",
                               'title_template': f"🔄 趋势反转: {symbol} ({timeframe})", 'message_template': (
                        "{trend_message}**下跌趋势终结**! 连跌 **{c}** 根后首现上涨K线。\n> **当前价**: {p:.4f}\n\n{vol_text}"),
                               'template_data': {'c': c, 'p': last_candle['close']},
                               'cooldown_logic': 'align_to_period_end', 'always_show_volume': True}
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
        elif is_last_down and is_prev_up:
            if (c := count_backwards(len(df) - 3, 'up')) >= min_n_to_alert:
                signal_info = {'alert_key': f"{symbol}_{timeframe}_REV_DOWN_{config_index}_{last_candle['timestamp']}",
                               'title_template': f"🔄 趋势反转: {symbol} ({timeframe})", 'message_template': (
                        "{trend_message}**上涨趋势终结**! 连涨 **{c}** 根后首现下跌K线。\n> **当前价**: {p:.4f}\n\n{vol_text}"),
                               'template_data': {'c': c, 'p': last_candle['close']},
                               'cooldown_logic': 'align_to_period_end', 'always_show_volume': True}
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

        current_trend_count = count_backwards(len(df) - 2, 'up' if is_last_up else 'down')
        if current_trend_count >= min_n_to_alert:
            d_text, emoji = ("上涨", "📈") if is_last_up else ("下跌", "📉")
            signal_info = {
                'alert_key': f"{symbol}_{timeframe}_CONT_{'UP' if is_last_up else 'DOWN'}_{config_index}_{last_candle['timestamp']}",
                'title_template': f"{emoji} 趋势持续: {{vol_label}}{symbol} ({timeframe})", 'message_template': (
                    "{trend_message}价格连续 **{c}** 周期{d_text}。\n> **当前价**: {p:.4f}\n\n{vol_text}"),
                'template_data': {'c': current_trend_count, 'd_text': d_text, 'p': last_candle['close']},
                'cooldown_logic': 'align_to_period_end', 'always_show_volume': True,
                'fallback_multiplier': consecutive_params.get('volume_multiplier', 1.5),
                'volume_must_confirm': consecutive_params.get('volume_confirm', False)}
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 连K错: {e}")


def check_high_funding_rate(exchange, symbol, timeframe, config, df, fund_params, config_index=0):
    try:
        funding_data = fetch_funding_rate(exchange, symbol)
        if not funding_data or funding_data.get('fundingRate') is None: return
        current_rate = funding_data['fundingRate']
        interval_hours = int(funding_data.get('info', {}).get('fundingIntervalHours', 8))
        dynamic_threshold = fund_params.get('threshold', 0.01) * (interval_hours / 4)
        if abs(current_rate) >= dynamic_threshold:
            direction_str = "多头支付空头" if current_rate > 0 else "空头支付多头"
            sentiment = "🔥 过热" if current_rate > 0 else "🥶 逼空"
            color_emoji = "🔴" if current_rate > 0 else "🟢"
            signal_info = {
                'log_name': 'High Funding', 'alert_key': f"{symbol}_FUNDING_{config_index}",
                'volume_must_confirm': False,
                'title_template': f"{color_emoji} 资金费率告警: {symbol} 达 {current_rate * 100:.3f}%",
                'message_template': (
                    "{trend_message}**资金费率异常**\n> **费率**: `{rate:.4f}%`\n> **周期**: {hours}h\n> **状态**: {sentiment} ({direction})\n\n"),
                'template_data': {"rate": current_rate * 100, "hours": interval_hours, "sentiment": sentiment,
                                  "direction": direction_str},
                'cooldown_mult': fund_params.get('cooldown_mult', 4), 'always_show_volume': False
            }
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 费率错: {e}")
# --- END OF FILE app/analysis/strategies.py ---