# --- START OF FILE app/analysis/strategies.py (ULTIMATE FORMATTING FIX V53.1 - FULL CODE) ---
from datetime import datetime, timezone
from loguru import logger
import pandas as pd
import pandas_ta as pta

from app.state import alerted_states, save_alert_states
from app.services.notification_service import send_alert
from app.analysis.trend import get_current_trend, timeframe_to_minutes
from app.analysis.levels import find_price_interest_zones, calculate_pivot_points
from app.analysis.indicators import (
    get_dynamic_volume_multiplier, get_dynamic_atr_multiplier, is_realtime_volume_over,
    get_dynamic_consecutive_candles
)
from app.utils import calculate_cooldown_time


def _prepare_and_send_notification(config, symbol, timeframe, df, signal_info):
    now_utc = datetime.now(timezone.utc)
    tf_minutes = timeframe_to_minutes(timeframe)
    params = config['strategy_params']

    alert_key = signal_info['alert_key']
    if alerted_states.get(alert_key) and now_utc < alerted_states[alert_key]:
        return

    vol_br_params = params.get('volume_breakout', {})
    dynamic_multiplier = get_dynamic_volume_multiplier(symbol, config, signal_info.get('fallback_multiplier', 1.5))
    is_vol_over, vol_text, actual_vol_ratio = is_realtime_volume_over(
        df, tf_minutes, vol_br_params.get('volume_ma_period', 20), dynamic_multiplier
    )

    if signal_info.get('volume_must_confirm', False) and not is_vol_over:
        logger.debug(f"[{symbol}|{timeframe}] 信号 '{signal_info.get('log_name', 'N/A')}' 因成交量不足被过滤。")
        return

    volume_label = f"放量({actual_vol_ratio:.1f}x) " if is_vol_over else "缩量 "
    title = signal_info['title_template'].format(vol_label=volume_label)

    message_data = signal_info.get('template_data', {})
    message_data['vol_text'] = vol_text
    message = signal_info['message_template'].format(**message_data)

    send_alert(config, title, message, symbol)
    cooldown_minutes = tf_minutes * signal_info.get('cooldown_mult', 1)
    alerted_states[alert_key] = calculate_cooldown_time(cooldown_minutes)
    save_alert_states()


def check_ema_signals(exchange, symbol, timeframe, config, df):
    try:
        params = config['strategy_params'];
        ema_params = params.get('ema_cross', {})
        atr_period = ema_params.get('atr_period', 14);
        atr_multiplier = ema_params.get('atr_multiplier', 0.3)
        df.ta.atr(length=atr_period, append=True)
        ema_period = ema_params.get('period', 120)
        indicator_result = df.ta.ema(length=ema_period, append=True)
        if indicator_result is None or indicator_result.empty: return
        if isinstance(indicator_result, pd.DataFrame):
            ema_col = indicator_result.columns[0]
        else:
            ema_col = indicator_result.name
        df_cleaned = df.dropna().reset_index(drop=True)
        if len(df_cleaned) < 2: return
        current, prev = df_cleaned.iloc[-1], df_cleaned.iloc[-2]
        atr_col = f"ATRr_{atr_period}"
        if pd.isna(current.get(atr_col)) or current.get(atr_col, 0) == 0: return
        atr_val = current[atr_col];
        atr_buffer = atr_val * atr_multiplier
        bullish = (current['close'] > current[ema_col] + atr_buffer) and (prev['close'] < prev[ema_col])
        bearish = (current['close'] < current[ema_col] - atr_buffer) and (prev['low'] > prev[ema_col])
        if bullish or bearish:
            action = "有效突破" if bullish else "有效跌破"
            trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, params)
            trend_message = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"
            breakout_distance = abs(current['close'] - current[ema_col]);
            breakout_atr_ratio = (breakout_distance / atr_val) if atr_val > 0 else float('inf')
            signal_info = {
                'log_name': 'EMA Cross',
                'alert_key': f"{symbol}_{timeframe}_EMACROSS_VALID_{'UP' if bullish else 'DOWN'}_REALTIME",
                'volume_must_confirm': ema_params.get('volume_confirm', False),
                'fallback_multiplier': ema_params.get('volume_multiplier', 1.5),
                'title_template': f"🚀 EMA {{vol_label}}{action}: {symbol} ({timeframe})".replace("  ", " "),
                'message_template': ("{trend_message}**信号**: 价格 **实时{action}** EMA({period})。\n\n"
                                     "**突破详情**:\n"
                                     "> **当前价**: {current_close:.4f}\n"
                                     "> **EMA值**: {ema_value:.4f}\n"
                                     "> **突破力度**: **{breakout_atr_ratio:.1f} 倍 ATR**\n"
                                     "> (突破阈值要求 > {atr_multiplier} 倍 ATR)\n\n"
                                     "{vol_text}"),
                'template_data': {"trend_message": trend_message, "action": action, "period": ema_period,
                                  "current_close": current['close'], "ema_value": current[ema_col],
                                  "breakout_atr_ratio": breakout_atr_ratio, "atr_multiplier": atr_multiplier},
                'cooldown_mult': 1
            }
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (EMA信号) 中出错: {e}", exc_info=True)


def check_kdj_cross(exchange, symbol, timeframe, config, df):
    try:
        params = config['strategy_params'];
        kdj_params = params.get('kdj_cross', {})
        indicator_result = df.ta.kdj(fast=kdj_params.get('fast_k', 9), slow=kdj_params.get('slow_k', 3),
                                     signal=kdj_params.get('slow_d', 3), append=True)
        if indicator_result is None or indicator_result.empty: return
        k_col, d_col = indicator_result.columns[0], indicator_result.columns[1]
        df_cleaned = df.dropna().reset_index(drop=True)
        if len(df_cleaned) < 2: return
        current, prev = df_cleaned.iloc[-1], df_cleaned.iloc[-2]
        golden = current[k_col] > current[d_col] and prev[k_col] <= prev[d_col]
        death = current[k_col] < current[d_col] and prev[k_col] >= prev[d_col]
        if not (golden or death): return
        trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, params)
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
        trend_message = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"
        emoji_map = {"看涨": "📈", "看跌": "📉", "警示": "⚠️", "金叉": "📈", "死叉": "📉", "机会": "💡"};
        emoji = emoji_map.get(signal_type_desc.split(' ')[0].replace("顺势", "").replace("震荡", ""), "⚙️")
        signal_info = {
            'log_name': 'KDJ Cross',
            'alert_key': f"{symbol}_{timeframe}_KDJ_{signal_type_desc.split(' ')[0]}_REALTIME",
            'volume_must_confirm': kdj_params.get('volume_confirm', False),
            'fallback_multiplier': kdj_params.get('volume_multiplier', 1.5),
            'title_template': f"{emoji} KDJ {{vol_label}}信号: {signal_type_desc} ({symbol} {timeframe})".replace("  ",
                                                                                                                  " "),
            'message_template': ("{trend_message}**信号解读**: {signal_type_desc}信号出现。\n\n"
                                 "**当前K/D值**: {k_val:.2f} / {d_val:.2f}\n"
                                 "**当前价**: {price:.4f}\n\n"
                                 "{vol_text}"),
            'template_data': {"trend_message": trend_message, "signal_type_desc": signal_type_desc,
                              "k_val": current[k_col], "d_val": current[d_col], "price": current['close']},
            'cooldown_mult': 0.5
        }
        _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (KDJ信号) 中出错: {e}", exc_info=True)


def check_volatility_breakout(exchange, symbol, timeframe, config, df):
    try:
        params = config['strategy_params'];
        vol_params = params.get('volatility_breakout', {})
        atr_period = vol_params.get('atr_period', 14)
        dynamic_atr_multiplier = get_dynamic_atr_multiplier(symbol, config, vol_params.get('atr_multiplier', 2.5))
        atr_col = f"ATRr_{atr_period}";
        df.ta.atr(length=atr_period, append=True)
        if atr_col not in df.columns: return
        df_cleaned = df.dropna().reset_index(drop=True)
        if len(df_cleaned) < 2: return
        current, prev = df_cleaned.iloc[-1], df_cleaned.iloc[-2]
        if pd.isna(prev.get(atr_col)) or prev.get(atr_col, 0) == 0: return
        current_volatility = current['high'] - current['low']
        reference_atr = prev[atr_col]
        is_volatility_breakout = current_volatility > reference_atr * dynamic_atr_multiplier
        if is_volatility_breakout:
            trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, params)
            trend_message = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"
            actual_atr_ratio = (current_volatility / reference_atr) if reference_atr > 0 else float('inf')
            signal_info = {
                'log_name': 'Volatility Breakout',
                'alert_key': f"{symbol}_{timeframe}_VOLATILITY_REALTIME",
                'volume_must_confirm': vol_params.get('volume_confirm', False),
                'fallback_multiplier': vol_params.get('volume_multiplier', 2.0),
                'title_template': f"💥 {{vol_label}}盘中波动异常: {symbol} ({timeframe})".replace("  ", " "),
                'message_template': ("{trend_message}"
                                     "**波动分析**:\n"
                                     "> **当前波幅**: `{current_volatility:.4f}` **(为参考ATR的 {actual_atr_ratio:.1f} 倍)**\n"
                                     "> **动态基准 (参考ATR)**: `{reference_atr:.4f}`\n"
                                     "> **波动阈值({dynamic_atr_multiplier:.1f}x)**: `{atr_threshold:.4f}`\n\n"
                                     "{vol_text}"),
                'template_data': {"trend_message": trend_message, "current_volatility": current_volatility,
                                  "actual_atr_ratio": actual_atr_ratio, "reference_atr": reference_atr,
                                  "dynamic_atr_multiplier": dynamic_atr_multiplier,
                                  "atr_threshold": reference_atr * dynamic_atr_multiplier},
                'cooldown_mult': 1
            }
            _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (波动率信号) 中出错: {e}", exc_info=True)


def check_volume_breakout(exchange, symbol, timeframe, config, df):
    try:
        df.name = symbol
        params = config['strategy_params']
        vol_br_params = params.get('volume_breakout', {})
        level_conf = vol_br_params.get('level_detection', {})
        if not level_conf.get('method') == 'advanced': return
        current_price = df.iloc[-1]['close']
        all_levels = []
        if level_conf.get('clustering', {}).get('enabled', False):
            cluster_conf = level_conf['clustering']
            atr_group_mult = cluster_conf.get('atr_grouping_multiplier', 0.5)
            min_size = cluster_conf.get('min_cluster_size', 2)
            min_sep = cluster_conf.get('min_separation_atr_mult', 1.0)
            price_zones = find_price_interest_zones(df.copy(), atr_group_mult, min_size, min_sep)
            all_levels.extend(price_zones)
        if level_conf.get('pivots', {}).get('enabled', False):
            try:
                daily_ohlcv_list = exchange.fetch_ohlcv(symbol, '1d', limit=2)
                if len(daily_ohlcv_list) >= 2:
                    prev_day_data = daily_ohlcv_list[-2]
                    prev_day_ohlc = {'high': prev_day_data[2], 'low': prev_day_data[3], 'close': prev_day_data[4]}
                    pivot_resistances, pivot_supports = calculate_pivot_points(prev_day_ohlc)
                    all_levels.extend(pivot_resistances)
                    all_levels.extend(pivot_supports)
            except Exception as e:
                logger.debug(f"为 {symbol} 获取枢轴点数据失败: {e}")
        resistances = sorted([lvl for lvl in all_levels if lvl['level'] > current_price], key=lambda x: x['level'])
        supports = sorted([lvl for lvl in all_levels if lvl['level'] < current_price], key=lambda x: x['level'],
                          reverse=True)
        if not resistances and not supports: return
        df.ta.atr(length=vol_br_params.get('atr_period', 14), append=True)
        df_cleaned = df.dropna().reset_index(drop=True)
        if len(df_cleaned) < 2: return
        current, prev = df_cleaned.iloc[-1], df_cleaned.iloc[-2]
        atr_val = current.get(f"ATRr_{vol_br_params.get('atr_period', 14)}", 0.0)
        if atr_val == 0: return
        atr_break_multiplier = vol_br_params.get('atr_multiplier_breakout', 0.1)
        atr_break_buffer = atr_val * atr_break_multiplier
        trend_status, _ = get_current_trend(df.copy(), timeframe, params)
        if resistances:
            closest_res = resistances[0]
            is_breakout = current['close'] > closest_res['level'] + atr_break_buffer and prev['close'] < closest_res[
                'level']
            if is_breakout and ("多头" in trend_status or "震荡" in trend_status):
                level_type_str = "+".join(sorted(list(set(closest_res.get('types', [closest_res.get('type')])))))
                is_confluence = len(closest_res.get('types', [])) > 1
                level_prefix = "🔥共振区域" if is_confluence else "水平位"
                signal_info = {
                    'log_name': 'Volume Breakout',
                    'alert_key': f"{symbol}_{timeframe}_breakout_resistance_{closest_res['level']:.4f}",
                    'volume_must_confirm': True,
                    'fallback_multiplier': vol_br_params.get('volume_multiplier', 1.5),
                    'title_template': f"🚨 {{vol_label}}突破关键阻力: {symbol} ({timeframe})",
                    'message_template': (f"**信号**: **{{vol_label}}突破关键阻力**!\n\n"
                                         f"**价格行为**: {level_prefix} ({level_type_str})\n"
                                         f"> **关键价位**: {closest_res['level']:.4f}\n"
                                         f"> **当前价格**: {current_price:.4f}\n\n"
                                         "{{vol_text}}"),
                    'template_data': {}, 'cooldown_mult': 1}
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
        if supports:
            closest_sup = supports[0]
            is_breakdown = current['close'] < closest_sup['level'] - atr_break_buffer and prev['close'] > closest_sup[
                'level']
            if is_breakdown and ("空头" in trend_status or "震荡" in trend_status):
                level_type_str = "+".join(sorted(list(set(closest_sup.get('types', [closest_sup.get('type')])))))
                is_confluence = len(closest_sup.get('types', [])) > 1
                level_prefix = "🔥共振区域" if is_confluence else "水平位"
                signal_info = {
                    'log_name': 'Volume Breakdown',
                    'alert_key': f"{symbol}_{timeframe}_breakout_support_{closest_sup['level']:.4f}",
                    'volume_must_confirm': True,
                    'fallback_multiplier': vol_br_params.get('volume_multiplier', 1.5),
                    'title_template': f"📉 {{vol_label}}跌破关键支撑: {symbol} ({timeframe})",
                    'message_template': (f"**信号**: **{{vol_label}}跌破关键支撑**!\n\n"
                                         f"**价格行为**: {level_prefix} ({level_type_str})\n"
                                         f"> **关键价位**: {closest_sup['level']:.4f}\n"
                                         f"> **当前价格**: {current_price:.4f}\n\n"
                                         "{{vol_text}}"),
                    'template_data': {}, 'cooldown_mult': 1}
                _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (高级量价突破) 中出错: {e}", exc_info=True)


def check_rsi_divergence(exchange, symbol, timeframe, config, df):
    try:
        now_utc = datetime.now(timezone.utc);
        cooldown_minutes = timeframe_to_minutes(timeframe) * 2
        params = config['strategy_params'];
        rsi_params = params.get('rsi_divergence', {})
        trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, params);
        trend_message = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"
        indicator_result = pta.rsi(df['close'], length=rsi_params.get('rsi_period', 14))
        if indicator_result is None or indicator_result.empty: return
        if isinstance(indicator_result, pd.DataFrame):
            rsi_col = indicator_result.columns[0]
        else:
            rsi_col = indicator_result.name
        df['rsi'] = indicator_result
        df_cleaned = df.dropna().reset_index(drop=True)
        lookback = rsi_params.get('lookback_period', 60)
        if len(df_cleaned) < lookback + 1: return
        recent_df, current = df_cleaned.iloc[-lookback - 1:-1], df_cleaned.iloc[-1]
        if current['close'] > recent_df['close'].max() and current['rsi'] < recent_df['rsi'].max():
            alert_key = f"{symbol}_{timeframe}_DIV_TOP_REALTIME"
            if not (alerted_states.get(alert_key) and now_utc < alerted_states[alert_key]):
                title = f"🚩 实时RSI顶背离风险: {symbol} ({timeframe})";
                message = trend_message + "**信号**: 价格创近期新高，但RSI指标出现衰弱迹象（潜在反转/回调风险）。"
                send_alert(config, title, message, symbol);
                alerted_states[alert_key] = calculate_cooldown_time(cooldown_minutes);
                save_alert_states()
        if current['close'] < recent_df['close'].min() and current['rsi'] > recent_df['rsi'].min():
            alert_key = f"{symbol}_{timeframe}_DIV_BOTTOM_REALTIME"
            if not (alerted_states.get(alert_key) and now_utc < alerted_states[alert_key]):
                title = f"⛳️ 实时RSI底背离机会: {symbol} ({timeframe})";
                message = trend_message + "**信号**: 价格创近期新低，但RSI指标出现企稳迹象（潜在反转/反弹机会）。"
                send_alert(config, title, message, symbol);
                alerted_states[alert_key] = calculate_cooldown_time(cooldown_minutes);
                save_alert_states()
    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (RSI背离) 中出错: {e}", exc_info=True)


def check_consecutive_candles(exchange, symbol, timeframe, config, df):
    try:
        params = config['strategy_params']
        consecutive_params = params.get('consecutive_candles', {})

        # 【核心修改】: 使用动态函数获取K线数量 n
        fallback_n = consecutive_params.get('min_consecutive_candles', 4)
        n = get_dynamic_consecutive_candles(symbol, config, fallback_n)

        if len(df) < n + 1:
            return

        # 注意：这里我们检查的是从倒数第n+1根到倒数第2根 (即最近n个已完成的K线)
        # 这样信号更稳定，避免盘中K线颜色变化导致信号闪烁
        recent_candles = df.iloc[-n - 1:-1]

        if len(recent_candles) < n:
            return  # 确保切片后仍有足够的K线

        is_all_up = (recent_candles['close'] > recent_candles['open']).all()
        is_all_down = (recent_candles['close'] < recent_candles['open']).all()

        if not (is_all_up or is_all_down):
            return

        direction = "上涨" if is_all_up else "下跌"
        emoji = "📈" if is_all_up else "📉"
        trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, params)
        trend_message = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"

        # 使用动态获取的 n 值来生成唯一的 alert_key
        alert_key = f"{symbol}_{timeframe}_CONSECUTIVE_{'UP' if is_all_up else 'DOWN'}_{n}"

        # 信号逻辑现在只在第一次满足N根条件时触发，之后由冷却期控制
        # 直接准备并发送通知
        signal_info = {
            'log_name': 'Consecutive Candles',
            'alert_key': alert_key,  # 使用上面生成的 key
            'volume_must_confirm': consecutive_params.get('volume_confirm', False),
            'fallback_multiplier': consecutive_params.get('volume_multiplier', 1.5),
            'title_template': f"{emoji} 连续{direction}信号: {symbol} ({timeframe})".replace("  ", " "),
            'message_template': ("{trend_message}**信号**: 价格已连续 **{n} 个周期 {direction}**。\n\n"
                                 "**动态参数详情**:\n"
                                 "> **币种排名**: 触发时需要 `{n}` 根连续K线\n\n"
                                 "**详细信息**:\n"
                                 "> **起始价**: {start_price:.4f}\n"
                                 "> **当前价**: {end_price:.4f}\n\n"
                                 "{vol_text}"),
            'template_data': {"trend_message": trend_message,
                              "n": n,
                              "direction": direction,
                              "start_price": recent_candles.iloc[0]['open'],
                              "end_price": recent_candles.iloc[-1]['close']},
            # 冷却时间至少为N个周期，防止在第 N+1 根时再次报警
            'cooldown_mult': n
        }

        # 注意：这里我们直接调用 _prepare_and_send_notification
        # 它内部会处理冷却期检查
        _prepare_and_send_notification(config, symbol, timeframe, df, signal_info)

    except Exception as e:
        logger.error(f"❌ 在 {symbol} {timeframe} (动态连续K线信号) 中出错: {e}", exc_info=True)