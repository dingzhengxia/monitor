# --- START OF FILE app/analysis/strategies.py ---
from datetime import datetime, timezone
from loguru import logger
import pandas as pd
import pandas_ta as pta

from app.analysis.order_blocks import find_latest_order_blocks
from app.state import alerted_states, save_alert_states
from app.services.notification_service import send_alert
from app.services.data_fetcher import fetch_funding_rate  # <--- 新增导入
from app.analysis.trend import get_current_trend, timeframe_to_minutes
from app.analysis.levels import find_price_interest_zones, calculate_pivot_points
from app.analysis.channels import detect_regression_channel
from app.analysis.indicators import (
    get_dynamic_volume_multiplier, get_dynamic_atr_multiplier, is_realtime_volume_over,
    get_dynamic_consecutive_candles
)
from app.utils import calculate_cooldown_time


def _get_params_for_timeframe(base_params: dict, timeframe: str) -> dict:
    """
    一个辅助函数，用于获取特定时间周期的策略参数。
    它会加载基础参数，然后用该时间周期的特定覆盖参数来更新它们。
    """
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

    static_bases = market_settings.get('static_symbols', [])
    symbol_base = symbol.split('/')[0].split(':')[0]
    is_static_symbol = symbol_base in static_bases

    exemption_enabled_for_this_strategy = signal_info.get('exempt_static_on_volume', False)
    original_volume_confirm = signal_info.get('volume_must_confirm', False)
    final_volume_confirm = False if (
            exemption_enabled_for_this_strategy and is_static_symbol) else original_volume_confirm

    if exemption_enabled_for_this_strategy and is_static_symbol and original_volume_confirm:
        logger.trace(
            f"[{symbol}] 是白名单币种，且策略 '{signal_info.get('log_name', 'N/A')}' 配置了豁免，已豁免成交量确认。")

    raw_lb_params = params.get('level_breakout', {})
    breakout_params = raw_lb_params[0] if isinstance(raw_lb_params, list) else raw_lb_params

    dynamic_multiplier = get_dynamic_volume_multiplier(symbol, config, signal_info.get('fallback_multiplier', 1.5))
    is_vol_over, vol_text, actual_vol_ratio = is_realtime_volume_over(
        df, tf_minutes, breakout_params.get('volume_ma_period', 20), dynamic_multiplier
    )

    if final_volume_confirm and not is_vol_over:
        logger.debug(f"[{symbol}|{timeframe}] 信号 '{signal_info.get('log_name', 'N/A')}' 因成交量不足被过滤。")
        return

    volume_label = f"放量({actual_vol_ratio:.1f}x) " if is_vol_over else f"缩量({actual_vol_ratio:.1f}x) "
    title = signal_info['title_template'].format(vol_label=volume_label).replace("  ", " ").strip()

    message_data = signal_info.get('template_data', {})
    trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, params)
    message_data['trend_message'] = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"

    if vol_text and signal_info.get('always_show_volume', True):
        message_data['vol_text'] = f"\n---\n{vol_text}"
    else:
        message_data['vol_text'] = ""

    message = signal_info['message_template'].format(**message_data)

    send_alert(config, title, message, symbol)

    if signal_info.get('cooldown_logic') == 'align_to_period_end':
        alerted_states[alert_key] = calculate_cooldown_time(tf_minutes, align_to_period_end=True)
    else:
        cooldown_minutes = tf_minutes * signal_info.get('cooldown_mult', 1)
        alerted_states[alert_key] = calculate_cooldown_time(cooldown_minutes)

    save_alert_states()


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
                'log_name': 'EMA Cross',
                'alert_key': f"{symbol}_{timeframe}_EMACROSS_{config_index}",
                'volume_must_confirm': ema_params.get('volume_confirm', False),
                'fallback_multiplier': ema_params.get('volume_multiplier', 1.5),
                'title_template': f"🚀 EMA {{vol_label}}{action}: {symbol} ({timeframe})",
                'message_template': ("{trend_message}**信号**: 价格 **实时{action}** EMA({period})。\n\n"
                                     "**突破详情**:\n"
                                     "> **当前价**: {current_close:.4f}\n"
                                     "> **EMA值**: {ema_value:.4f}\n"
                                     "> **突破力度**: **{breakout_atr_ratio:.1f} 倍 ATR**\n"
                                     "> (突破阈值要求 > {atr_multiplier} 倍 ATR)\n\n"
                                     "{vol_text}"),
                'template_data': {"action": action, "period": ema_period, "current_close": current['close'],
                                  "ema_value": current[ema_col], "breakout_atr_ratio": breakout_atr_ratio,
                                  "atr_multiplier": atr_multiplier},
                'cooldown_mult': 1
            }
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (EMA信号) 中出错: {e}", exc_info=True)


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
        if "多头趋势" in trend_status:
            if golden:
                signal_type_desc = "顺势看涨 (入场机会)"
            elif death:
                signal_type_desc = "回调警示 (减仓风险)"
        elif "空头趋势" in trend_status:
            if death:
                signal_type_desc = "顺势看跌 (入场机会)"
            elif golden:
                signal_type_desc = "反弹警示 (空单止盈/反弹风险)"
        else:
            if golden:
                signal_type_desc = "震荡金叉 (反弹机会)"
            elif death:
                signal_type_desc = "震荡死叉 (下跌机会)"
        if not signal_type_desc: return
        emoji_map = {"看涨": "📈", "看跌": "📉", "警示": "⚠️", "金叉": "📈", "死叉": "📉", "机会": "💡"}
        emoji = emoji_map.get(signal_type_desc.split(' ')[0].replace("顺势", "").replace("震荡", ""), "⚙️")
        signal_info = {
            'log_name': 'KDJ Cross',
            'alert_key': f"{symbol}_{timeframe}_KDJ_{config_index}",
            'volume_must_confirm': kdj_params.get('volume_confirm', True),
            'fallback_multiplier': kdj_params.get('volume_multiplier', 1.5),
            'title_template': f"{emoji} KDJ {{vol_label}}信号: {signal_type_desc} ({symbol} {timeframe})",
            'message_template': ("{trend_message}**信号解读**: {signal_type_desc}信号出现。\n\n"
                                 "**当前K/D值**: {k_val:.2f} / {d_val:.2f}\n"
                                 "**当前价**: {price:.4f}\n\n"
                                 "{vol_text}"),
            'template_data': {"signal_type_desc": signal_type_desc, "k_val": current[k_col], "d_val": current[d_col],
                              "price": current['close']},
            'cooldown_mult': 0.5
        }
        _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (KDJ信号) 中出错: {e}", exc_info=True)


def check_volatility_breakout(exchange, symbol, timeframe, config, df, vol_params, config_index=0):
    try:
        atr_period = vol_params.get('atr_period', 14)
        dynamic_atr_multiplier = get_dynamic_atr_multiplier(symbol, config, vol_params.get('atr_multiplier', 2.5))
        atr_col = f"ATRr_{atr_period}"
        df.ta.atr(length=atr_period, append=True)
        if atr_col not in df.columns: return
        df_cleaned = df.dropna(subset=[atr_col]).reset_index(drop=True)
        if len(df_cleaned) < 2: return
        current, prev = df_cleaned.iloc[-1], df_cleaned.iloc[-2]
        if pd.isna(prev.get(atr_col)) or prev.get(atr_col, 0) == 0: return
        current_volatility = current['high'] - current['low']
        reference_atr = prev[atr_col]
        is_volatility_breakout = current_volatility > reference_atr * dynamic_atr_multiplier
        if is_volatility_breakout:
            actual_atr_ratio = (current_volatility / reference_atr) if reference_atr > 0 else float('inf')
            signal_info = {
                'log_name': 'Volatility Breakout',
                'alert_key': f"{symbol}_{timeframe}_VOLATILITY_{config_index}",
                'volume_must_confirm': vol_params.get('volume_confirm', True),
                'fallback_multiplier': vol_params.get('volume_multiplier', 2.0),
                'title_template': f"💥 {{vol_label}}盘中波动异常: {symbol} ({timeframe})",
                'message_template': ("{trend_message}**波动分析**:\n"
                                     "> **当前波幅**: `{current_volatility:.4f}` **(为参考ATR的 {actual_atr_ratio:.1f} 倍)**\n"
                                     "> **动态基准 (参考ATR)**: `{reference_atr:.4f}`\n"
                                     "> **波动阈值({dynamic_atr_multiplier:.1f}x)**: `{atr_threshold:.4f}`\n\n"
                                     "{vol_text}"),
                'template_data': {"current_volatility": current_volatility, "actual_atr_ratio": actual_atr_ratio,
                                  "reference_atr": reference_atr, "dynamic_atr_multiplier": dynamic_atr_multiplier,
                                  "atr_threshold": reference_atr * dynamic_atr_multiplier},
                'cooldown_mult': 1
            }
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (波动率信号) 中出错: {e}", exc_info=True)


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
        if level_conf.get('clustering', {}).get('enabled', False):
            cluster_conf = level_conf['clustering']
            atr_group_mult = cluster_conf.get('atr_grouping_multiplier', 0.5)
            min_size = cluster_conf.get('min_cluster_size', 2)
            min_sep = cluster_conf.get('min_separation_atr_mult', 0.6)
            price_zones = find_price_interest_zones(df.copy(), atr_group_mult, min_size, min_sep)
            all_levels.extend(price_zones)
        if level_conf.get('static_pivots', {}).get('enabled', False):
            try:
                daily_ohlcv_list = exchange.fetch_ohlcv(symbol, '1d', limit=2)
                if len(daily_ohlcv_list) >= 2:
                    prev_day_data = daily_ohlcv_list[-2]
                    prev_day_ohlc = {'high': prev_day_data[2], 'low': prev_day_data[3], 'close': prev_day_data[4]}
                    pivot_resistances, pivot_supports = calculate_pivot_points(prev_day_ohlc)
                    for r in pivot_resistances: r['type'] = f"D-{r['type']}"
                    for s in pivot_supports: s['type'] = f"D-{s['type']}"
                    all_levels.extend(pivot_resistances)
                    all_levels.extend(pivot_supports)
            except Exception as e:
                logger.debug(f"[{symbol}|{timeframe}] 获取静态枢轴点数据失败: {e}")
        if level_conf.get('rolling_pivots', {}).get('enabled', False):
            period = breakout_params.get('breakout_period', 120)
            if len(df_cleaned) > period:
                lookback_df = df_cleaned.iloc[-period - 2:-2]
                if not lookback_df.empty:
                    window_high = lookback_df['high'].max()
                    window_low = lookback_df['low'].min()
                    window_close = lookback_df['close'].iloc[-1]
                    window_ohlc = {'high': window_high, 'low': window_low, 'close': window_close}
                    rolling_resistances, rolling_supports = calculate_pivot_points(window_ohlc)
                    prefix = f'P({period})'
                    for r in rolling_resistances: r['type'] = f"{prefix}-{r['type']}"
                    for s in rolling_supports: s['type'] = f"{prefix}-{s['type']}"
                    all_levels.extend(rolling_resistances)
                    all_levels.extend(rolling_supports)
        if not all_levels: return
        prev_price = prev['close']
        resistances = sorted([lvl for lvl in all_levels if lvl['level'] > prev_price], key=lambda x: x['level'])
        supports = sorted([lvl for lvl in all_levels if lvl['level'] < prev_price], key=lambda x: x['level'],
                          reverse=True)
        atr_val = current.get(f"ATRr_{breakout_params.get('atr_period', 14)}", 0.0)
        if atr_val == 0: return
        atr_break_multiplier = breakout_params.get('atr_multiplier_breakout', 0.1)
        atr_break_buffer = atr_val * atr_break_multiplier
        if resistances:
            closest_res = resistances[0]
            cond1 = prev['close'] < closest_res['level']
            cond2 = current['close'] > closest_res['level'] + atr_break_buffer
            is_breakout = cond1 and cond2
            if is_breakout:
                level_type_str = "+".join(sorted(list(set(closest_res.get('types', [closest_res.get('type')])))))
                is_confluence = len(closest_res.get('types', [])) > 1
                level_prefix = "🔥共振区域" if is_confluence else "水平位"
                signal_info = {
                    'log_name': 'Level Breakout',
                    'alert_key': f"{symbol}_{timeframe}_breakout_resistance_{config_index}_{current['timestamp']}",
                    'volume_must_confirm': breakout_params.get('volume_confirm', True),
                    'fallback_multiplier': breakout_params.get('volume_multiplier', 1.5),
                    'title_template': f"🚨 {{vol_label}}突破关键阻力: {symbol} ({timeframe})",
                    'message_template': (
                        "{trend_message}**信号**: **突破关键阻力**!\n\n" f"**价格行为**: {level_prefix} ({level_type_str})\n" f"> **关键价位**: {closest_res['level']:.4f}\n" f"> **突破价格**: {current['close']:.4f}\n\n" "{vol_text}"),
                    'template_data': {},
                    'cooldown_mult': 1
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
        if supports:
            closest_sup = supports[0]
            cond1 = prev['close'] > closest_sup['level']
            cond2 = current['close'] < closest_sup['level'] - atr_break_buffer
            is_breakdown = cond1 and cond2
            if is_breakdown:
                level_type_str = "+".join(sorted(list(set(closest_sup.get('types', [closest_sup.get('type')])))))
                is_confluence = len(closest_sup.get('types', [])) > 1
                level_prefix = "🔥共振区域" if is_confluence else "水平位"
                signal_info = {
                    'log_name': 'Level Breakdown',
                    'alert_key': f"{symbol}_{timeframe}_breakout_support_{config_index}_{current['timestamp']}",
                    'volume_must_confirm': breakout_params.get('volume_confirm', True),
                    'fallback_multiplier': breakout_params.get('volume_multiplier', 1.5),
                    'title_template': f"📉 {{vol_label}}跌破关键支撑: {symbol} ({timeframe})",
                    'message_template': (
                        "{trend_message}**信号**: **跌破关键支撑**!\n\n" f"**价格行为**: {level_prefix} ({level_type_str})\n" f"> **关键价位**: {closest_sup['level']:.4f}\n" f"> **跌破价格**: {current['close']:.4f}\n\n" "{vol_text}"),
                    'template_data': {},
                    'cooldown_mult': 1
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (关键位突破) 中出错: {e}", exc_info=True)


def check_rsi_divergence(exchange, symbol, timeframe, config, df, rsi_params, config_index=0):
    try:
        indicator_result = pta.rsi(df['close'], length=rsi_params.get('rsi_period', 14))
        if indicator_result is None or indicator_result.empty: return
        rsi_col = indicator_result.columns[0] if isinstance(indicator_result, pd.DataFrame) else indicator_result.name
        df['rsi'] = indicator_result
        df_cleaned = df.dropna(subset=['rsi']).reset_index(drop=True)
        lookback = rsi_params.get('lookback_period', 60)
        if len(df_cleaned) < lookback + 1: return
        recent_df, current = df_cleaned.iloc[-lookback - 1:-1], df_cleaned.iloc[-1]
        if current['close'] > recent_df['close'].max() and current['rsi'] < recent_df['rsi'].max():
            signal_info = {
                'log_name': 'RSI Top Divergence',
                'alert_key': f"{symbol}_{timeframe}_DIV_TOP_{config_index}",
                'volume_must_confirm': False,
                'title_template': f"🚩 RSI顶背离风险: {symbol} ({timeframe})",
                'message_template': "{trend_message}**信号**: 价格创近期新高，但RSI指标出现衰弱迹象（潜在反转/回调风险）。\n\n{vol_text}",
                'template_data': {},
                'cooldown_mult': 2,
                'always_show_volume': True
            }
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
        if current['close'] < recent_df['close'].min() and current['rsi'] > recent_df['rsi'].min():
            signal_info = {
                'log_name': 'RSI Bottom Divergence',
                'alert_key': f"{symbol}_{timeframe}_DIV_BOTTOM_{config_index}",
                'volume_must_confirm': False,
                'title_template': f"⛳️ RSI底背离机会: {symbol} ({timeframe})",
                'message_template': "{trend_message}**信号**: 价格创近期新低，但RSI指标出现企稳迹象（潜在反转/反弹机会）。\n\n{vol_text}",
                'template_data': {},
                'cooldown_mult': 2,
                'always_show_volume': True
            }
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (RSI背离) 中出错: {e}", exc_info=True)


def check_trend_channel_breakout(exchange, symbol, timeframe, config, df, channel_params, config_index=0):
    try:
        if 'lookback_period' not in channel_params:
            logger.warning(
                f"[{symbol}|{timeframe}] 趋势通道策略 '{channel_params.get('name', config_index)}' 缺少 'lookback_period' 参数，跳过。");
            return

        atr_period = 14
        df.ta.atr(length=atr_period, append=True)
        atr_col = f"ATRr_{atr_period}"
        if atr_col not in df.columns:
            return

        df_for_channel = df.copy();
        df_for_channel['symbol'] = symbol;
        df_for_channel['timeframe'] = timeframe
        channel_info = detect_regression_channel(df_for_channel, lookback_period=channel_params.get('lookback_period'),
                                                 min_trend_length=channel_params.get('min_trend_length', 20),
                                                 std_dev_multiplier=channel_params.get('std_dev_multiplier', 2.0))

        if not channel_info: return

        if len(df) < 3: return
        current, prev = df.iloc[-1], df.iloc[-2]

        if len(channel_info['upper_band']) < 2: return
        current_upper_band = channel_info['upper_band'].iloc[-1]
        prev_upper_band = channel_info['upper_band'].iloc[-2]
        current_lower_band = channel_info['lower_band'].iloc[-1]
        prev_lower_band = channel_info['lower_band'].iloc[-2]

        trend_length = channel_info['trend_length']

        confirmation_atr_mult = channel_params.get('breakout_confirmation_atr', 0.0)
        current_atr = current.get(atr_col, 0)
        if pd.isna(current_atr) or current_atr == 0:
            return

        confirmation_buffer = current_atr * confirmation_atr_mult

        # 信号1: 突破下降趋势的回归通道 (看涨)
        if channel_info['slope'] < 0:
            breakout_threshold = current_upper_band + confirmation_buffer

            is_confirmed_breakout = prev['close'] < prev_upper_band and current['close'] > breakout_threshold

            if is_confirmed_breakout:
                signal_info = {'log_name': f"Reg_Channel_Breakout ({channel_params.get('name')})",
                               'alert_key': f"{symbol}_{timeframe}_REG_CHAN_UP_{config_index}",
                               'volume_must_confirm': channel_params.get('volume_confirm', True),
                               'fallback_multiplier': channel_params.get('volume_multiplier', 1.8),
                               'title_template': f"📈 {{vol_label}}{channel_params.get('name')}确认突破: {symbol} ({timeframe})",
                               'message_template': ("{trend_message}**信号**: **价格确认突破下降回归通道**。\n\n"
                                                    "**趋势分析**:\n"
                                                    "> **趋势持续**: {trend_length} 根K线\n"
                                                    "> **突破价格**: {current_close:.4f}\n"
                                                    "> **通道上轨**: {upper_band:.4f} `(+{buffer:.4f} 确认区)`\n\n"
                                                    "价格有力偏离了下行趋势，可能是趋势反转的信号。\n\n"
                                                    "{vol_text}"),
                               'template_data': {"current_close": current['close'],
                                                 "upper_band": current_upper_band,
                                                 "buffer": confirmation_buffer,
                                                 "trend_length": trend_length},
                               'cooldown_mult': 4}
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

        # 信号2: 跌破上升趋势的回归通道 (看跌)
        elif channel_info['slope'] > 0:
            breakdown_threshold = current_lower_band - confirmation_buffer

            is_confirmed_breakdown = prev['close'] > prev_lower_band and current['close'] < breakdown_threshold

            if is_confirmed_breakdown:
                signal_info = {'log_name': f"Reg_Channel_Breakdown ({channel_params.get('name')})",
                               'alert_key': f"{symbol}_{timeframe}_REG_CHAN_DOWN_{config_index}",
                               'volume_must_confirm': channel_params.get('volume_confirm', True),
                               'fallback_multiplier': channel_params.get('volume_multiplier', 1.8),
                               'title_template': f"📉 {{vol_label}}{channel_params.get('name')}确认跌破: {symbol} ({timeframe})",
                               'message_template': ("{trend_message}**信号**: **价格确认跌破上升回归通道**。\n\n"
                                                    "**趋势分析**:\n"
                                                    "> **趋势持续**: {trend_length} 根K线\n"
                                                    "> **跌破价格**: {current_close:.4f}\n"
                                                    "> **通道下轨**: {lower_band:.4f} `(-{buffer:.4f} 确认区)`\n\n"
                                                    "价格有力偏离了上行趋势，可能是趋势反转的信号。\n\n"
                                                    "{vol_text}"),
                               'template_data': {"current_close": current['close'],
                                                 "lower_band": current_lower_band,
                                                 "buffer": confirmation_buffer,
                                                 "trend_length": trend_length},
                               'cooldown_mult': 4}
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (回归通道突破) 中出错: {e}", exc_info=True)


def check_consecutive_candles(exchange, symbol, timeframe, config, df, consecutive_params, config_index=0):
    try:
        fallback_n = consecutive_params.get('min_consecutive_candles', 4)
        min_n_to_alert = get_dynamic_consecutive_candles(symbol, config, fallback_n)

        if len(df) < min_n_to_alert + 2 or len(df) < 3:
            return

        def count_backwards(start_index, direction):
            count = 0
            for i in range(start_index, -1, -1):
                candle = df.iloc[i];
                is_up = candle['close'] > candle['open'];
                is_down = candle['close'] < candle['open'];
                current_direction = 'up' if is_up else ('down' if is_down else 'none')
                if current_direction == direction:
                    count += 1
                else:
                    break
            return count

        last_candle = df.iloc[-2];
        prev_candle = df.iloc[-3];
        is_last_up = last_candle['close'] > last_candle['open'];
        is_last_down = last_candle['close'] < last_candle['open'];
        is_prev_up = prev_candle['close'] > prev_candle['open'];
        is_prev_down = prev_candle['close'] < prev_candle['open']

        if is_last_up and is_prev_down:
            prev_down_trend_count = count_backwards(len(df) - 3, 'down')
            if prev_down_trend_count >= min_n_to_alert:
                alert_key = f"{symbol}_{timeframe}_REVERSAL_UP_{config_index}_{last_candle['timestamp']}"
                signal_info = {'alert_key': alert_key, 'title_template': f"🔄 趋势反转: {symbol} ({timeframe})",
                               'message_template': (
                                   "{trend_message}**信号**: **下跌趋势终结**!\n\n> 连续下跌 **{prev_down_trend_count}** 根K线后，出现首根上涨K线。\n> **当前价**: {current_price:.4f}\n\n{vol_text}"),
                               'template_data': {'prev_down_trend_count': prev_down_trend_count,
                                                 'current_price': last_candle['close']},
                               'cooldown_logic': 'align_to_period_end', 'always_show_volume': True}
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

        elif is_last_down and is_prev_up:
            prev_up_trend_count = count_backwards(len(df) - 3, 'up')
            if prev_up_trend_count >= min_n_to_alert:
                alert_key = f"{symbol}_{timeframe}_REVERSAL_DOWN_{config_index}_{last_candle['timestamp']}"
                signal_info = {'alert_key': alert_key, 'title_template': f"🔄 趋势反转: {symbol} ({timeframe})",
                               'message_template': (
                                   "{trend_message}**信号**: **上涨趋势终结**!\n\n> 连续上涨 **{prev_up_trend_count}** 根K线后，出现首根下跌K线。\n> **当前价**: {current_price:.4f}\n\n{vol_text}"),
                               'template_data': {'prev_up_trend_count': prev_up_trend_count,
                                                 'current_price': last_candle['close']},
                               'cooldown_logic': 'align_to_period_end', 'always_show_volume': True}
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

        current_trend_count = 0;
        current_direction = None

        if is_last_up:
            current_direction = 'up';
            current_trend_count = count_backwards(len(df) - 2, 'up')
        elif is_last_down:
            current_direction = 'down';
            current_trend_count = count_backwards(len(df) - 2, 'down')

        if current_trend_count >= min_n_to_alert:
            alert_key = f"{symbol}_{timeframe}_CONTINUOUS_{current_direction.upper()}_{config_index}_{last_candle['timestamp']}";
            direction_text = "上涨" if current_direction == 'up' else "下跌";
            emoji = "📈" if current_direction == 'up' else "📉"
            signal_info = {'alert_key': alert_key,
                           'title_template': f"{emoji} 趋势持续: {{vol_label}}{symbol} ({timeframe})",
                           'message_template': (
                               "{trend_message}**信号**: 价格已连续 **{current_trend_count}** 个周期{direction_text}。\n\n> **当前价**: {current_price:.4f}\n\n{vol_text}"),
                           'template_data': {'current_trend_count': current_trend_count,
                                             'direction_text': direction_text,
                                             'current_price': last_candle['close']},
                           'cooldown_logic': 'align_to_period_end',
                           'fallback_multiplier': consecutive_params.get('volume_multiplier', 1.5),
                           'volume_must_confirm': consecutive_params.get('volume_confirm', False),
                           'always_show_volume': True}
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (无状态连续K线信号) 中出错: {e}", exc_info=True)


def check_order_block_interaction(exchange, symbol, timeframe, config, df, ob_params, config_index=0):
    try:
        swing_length = ob_params.get('swing_length', 10)
        atr_multiplier = ob_params.get('atr_multiplier', 0.1)
        bull_ob, bear_ob = find_latest_order_blocks(df.copy(), swing_length, atr_multiplier)

        if not (bull_ob or bear_ob):
            return

        current = df.iloc[-1]
        prev = df.iloc[-2]

        if bear_ob:
            top, bottom = bear_ob['top'], bear_ob['bottom']
            if ob_params.get('alert_on_rejection', True) and \
                    prev['close'] < bottom and current['close'] >= bottom and current['close'] <= top:
                signal_info = {
                    'log_name': 'OrderBlock Rejection',
                    'alert_key': f"{symbol}_{timeframe}_OB_REJECT_BEAR_{bear_ob['timestamp']}",
                    'volume_must_confirm': False,
                    'title_template': f"⚠️ {symbol} ({timeframe}) 测试关键阻力区",
                    'message_template': ("{trend_message}**信号**: 价格已进入由前期市场结构形成的**熊市订单块(阻力区)**。\n\n"
                                         "> **阻力区间**: `{bottom:.4f} - {top:.4f}`\n"
                                         "> **当前价格**: `{current_close:.4f}`\n\n"
                                         "请关注此处是否出现价格拒绝或反转信号。\n\n"
                                         "{vol_text}"),
                    'template_data': {"bottom": bottom, "top": top, "current_close": current['close']},
                    'cooldown_mult': 2,
                    'always_show_volume': True
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

            if ob_params.get('alert_on_breakout', False) and prev['close'] < top and current['close'] > top:
                signal_info = {
                    'log_name': 'OrderBlock Breakout',
                    'alert_key': f"{symbol}_{timeframe}_OB_BREAK_BEAR_{bear_ob['timestamp']}",
                    'volume_must_confirm': True,
                    'fallback_multiplier': 1.8,
                    'title_template': f"🚀 {{vol_label}}突破关键阻力: {symbol} ({timeframe})",
                    'message_template': ("{trend_message}**信号**: 价格**已突破**前期关键的**熊市订单块(阻力区)**。\n\n"
                                         "> **原阻力区间**: `{bottom:.4f} - {top:.4f}`\n"
                                         "> **突破价格**: `{current_close:.4f}`\n\n"
                                         "市场结构可能发生转变，原阻力可能转为支撑。\n\n"
                                         "{vol_text}"),
                    'template_data': {"bottom": bottom, "top": top, "current_close": current['close']},
                    'cooldown_mult': 4,
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

        if bull_ob:
            top, bottom = bull_ob['top'], bull_ob['bottom']
            if ob_params.get('alert_on_rejection', True) and \
                    prev['close'] > top and current['close'] <= top and current['close'] >= bottom:
                signal_info = {
                    'log_name': 'OrderBlock Support',
                    'alert_key': f"{symbol}_{timeframe}_OB_SUPPORT_BULL_{bull_ob['timestamp']}",
                    'volume_must_confirm': False,
                    'title_template': f"💡 {symbol} ({timeframe}) 测试关键支撑区",
                    'message_template': ("{trend_message}**信号**: 价格已进入由前期市场结构形成的**牛市订单块(支撑区)**。\n\n"
                                         "> **支撑区间**: `{bottom:.4f} - {top:.4f}`\n"
                                         "> **当前价格**: `{current_close:.4f}`\n\n"
                                         "请关注此处是否获得支撑或出现反弹信号。\n\n"
                                         "{vol_text}"),
                    'template_data': {"bottom": bottom, "top": top, "current_close": current['close']},
                    'cooldown_mult': 2,
                    'always_show_volume': True
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

            if ob_params.get('alert_on_breakout', False) and prev['close'] > bottom and current['close'] < bottom:
                signal_info = {
                    'log_name': 'OrderBlock Breakdown',
                    'alert_key': f"{symbol}_{timeframe}_OB_BREAK_BULL_{bull_ob['timestamp']}",
                    'volume_must_confirm': True,
                    'fallback_multiplier': 1.8,
                    'title_template': f"📉 {{vol_label}}跌破关键支撑: {symbol} ({timeframe})",
                    'message_template': ("{trend_message}**信号**: 价格**已跌破**前期关键的**牛市订单块(支撑区)**。\n\n"
                                         "> **原支撑区间**: `{bottom:.4f} - {top:.4f}`\n"
                                         "> **跌破价格**: `{current_close:.4f}`\n\n"
                                         "市场结构可能发生转变，原支撑可能转为阻力。\n\n"
                                         "{vol_text}"),
                    'template_data': {"bottom": bottom, "top": top, "current_close": current['close']},
                    'cooldown_mult': 4
                }
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (订单块交互) 中出错: {e}", exc_info=True)


def check_high_funding_rate(exchange, symbol, timeframe, config, df, fund_params, config_index=0):
    """
    监控资金费率是否异常（超过阈值）。
    """
    try:
        threshold = fund_params.get('threshold', 0.01)

        funding_data = fetch_funding_rate(exchange, symbol)
        if not funding_data:
            return

        current_rate = funding_data.get('fundingRate')
        if current_rate is None:
            return

        if abs(current_rate) >= threshold:
            rate_percent = current_rate * 100

            if current_rate > 0:
                direction_str = "多头支付空头 (费率极高)"
                sentiment = "🔥 极度看涨/过热"
                color_emoji = "🔴"
            else:
                direction_str = "空头支付多头 (费率极低)"
                sentiment = "🥶 极度看跌/逼空风险"
                color_emoji = "🟢"

            next_fund_time_str = "N/A"
            if funding_data.get('nextFundingTimestamp'):
                next_fund_dt = datetime.fromtimestamp(funding_data['nextFundingTimestamp'] / 1000, tz=timezone.utc)
                next_fund_time_str = next_fund_dt.strftime('%H:%M UTC')

            signal_info = {
                'log_name': 'High Funding Rate',
                # 使用不带时间戳的 Alert Key 确保冷却生效
                'alert_key': f"{symbol}_FUNDING_RATE_{config_index}",
                'volume_must_confirm': False,
                'title_template': f"{color_emoji} 资金费率告警: {symbol} 达 {rate_percent:.3f}%",
                'message_template': (
                    "{trend_message}**信号**: **资金费率异常** ( > {threshold_pct}% )。\n\n"
                    "> **当前费率**: `{rate_percent:.4f}%`\n"
                    "> **市场状态**: {sentiment}\n"
                    "> **资金流向**: {direction_str}\n"
                    "> **结算时间**: {next_fund_time}\n\n"
                    "⚠️ 高额资金费率通常意味着市场极其拥挤，可能面临剧烈的**清洗或反转**。"
                ),
                'template_data': {
                    "threshold_pct": threshold * 100,
                    "rate_percent": rate_percent,
                    "sentiment": sentiment,
                    "direction_str": direction_str,
                    "next_fund_time": next_fund_time_str
                },
                # 从配置读取冷却倍数，默认4倍
                'cooldown_mult': fund_params.get('cooldown_mult', 4),
                'always_show_volume': False
            }

            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

    except Exception as e:
        logger.error(f"❌ 在 {symbol} (资金费率监控) 中出错: {e}", exc_info=True)
# --- END OF FILE app/analysis/strategies.py ---