# --- START OF FILE app/analysis/indicators.py ---
import math
import pandas as pd
from datetime import datetime, timezone

# 本地应用导入
from app.state import cached_top_symbols


def _calculate_dynamic_multiplier(symbol, config, conf_key, fallback_multiplier, min_default, max_default):
    dyn_conf = config['strategy_params'].get(conf_key, {})
    if not dyn_conf.get('enabled', False):
        return fallback_multiplier
    try:
        rank = cached_top_symbols.index(symbol) + 1
    except (ValueError, TypeError):
        return dyn_conf.get('max_multiplier', fallback_multiplier)

    method = dyn_conf.get('method')
    min_mult = dyn_conf.get('min_multiplier', min_default)
    max_mult = dyn_conf.get('max_multiplier', max_default)
    total_ranks = dyn_conf.get('apply_to_rank_n', 100)

    if method == 'linear':
        if total_ranks <= 1: return min_mult
        slope = (max_mult - min_mult) / (total_ranks - 1)
        multiplier = min_mult + (rank - 1) * slope
        return max(min_mult, min(multiplier, max_mult))
    elif method == 'stepped':
        step_size = dyn_conf.get('rank_step_size', 10)
        if step_size <= 0: return fallback_multiplier
        num_steps = math.ceil(total_ranks / step_size)
        if num_steps <= 1: return min_mult
        increment_per_step = (max_mult - min_mult) / (num_steps - 1)
        current_step_index = math.floor((rank - 1) / step_size)
        multiplier = min_mult + current_step_index * increment_per_step
        return max(min_mult, min(multiplier, max_mult))

    return fallback_multiplier


def get_dynamic_volume_multiplier(symbol, config, fallback_multiplier):
    return _calculate_dynamic_multiplier(symbol, config, 'dynamic_volume_multipliers', fallback_multiplier, 2.5, 10.0)


def get_dynamic_atr_multiplier(symbol, config, fallback_multiplier):
    return _calculate_dynamic_multiplier(symbol, config, 'dynamic_atr_multipliers', fallback_multiplier, 2.5, 5.0)


def is_realtime_volume_over(df, tf_minutes, volume_ma_period, multiplier):
    df_vol = df.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df_vol['timestamp'] = pd.to_datetime(df_vol['timestamp'], unit='ms', utc=True)
    else:
        df_vol['timestamp'] = df.index.tz_localize('UTC')

    if len(df_vol) < volume_ma_period + 1: return False, "", 0.0

    df_vol['volume_ma'] = df_vol['volume'].rolling(window=volume_ma_period).mean().shift(1)
    df_vol = df_vol.dropna()

    if df_vol.empty: return False, "", 0.0

    current = df_vol.iloc[-1]
    now_utc = datetime.now(timezone.utc)
    start_time = current['timestamp']

    minutes_elapsed = (now_utc - start_time).total_seconds() / 60
    MIN_TIME_RATIO = 0.05
    time_ratio = max(minutes_elapsed / tf_minutes, MIN_TIME_RATIO) if tf_minutes > 0 else 1.0
    time_ratio = min(time_ratio, 1.0)
    actual_time_progress = minutes_elapsed / tf_minutes if tf_minutes > 0 else 1.0

    dynamic_baseline = current['volume_ma'] * time_ratio
    is_over = current['volume'] > (dynamic_baseline * multiplier)
    actual_ratio = (current['volume'] / dynamic_baseline) if dynamic_baseline > 0 else float('inf')

    text = (
        f"**成交量分析** (周期进行{actual_time_progress:.0%}):\n"
        f"> **当前量**: {current['volume']:.0f} **(为动态基准的 {actual_ratio:.1f} 倍)**\n"
        f"> **动态基准**: {dynamic_baseline:.0f} (已按时间调整)\n"
        f"> **放量阈值({multiplier:.1f}x)**: {(dynamic_baseline * multiplier):.0f}"
    )
    return is_over, text, actual_ratio
# --- END OF FILE app/analysis/indicators.py ---