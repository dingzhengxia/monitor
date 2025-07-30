# --- START OF FILE app/analysis/indicators.py (UPGRADED WITH UNIFIED DYNAMIC ENGINE) ---
import math
import pandas as pd
from datetime import datetime, timezone

# 本地应用导入
from app.state import cached_top_symbols


def _calculate_dynamic_value(symbol, dyn_conf, fallback_value):
    """
    一个通用的动态值计算引擎，根据交易对排名计算参数。
    支持 'linear', 'stepped', 'linear_stepped' 方法。
    """
    if not dyn_conf or not dyn_conf.get('enabled', False):
        return fallback_value

    try:
        rank = cached_top_symbols.index(symbol) + 1
    except (ValueError, TypeError):
        return dyn_conf.get('default_multiplier') or dyn_conf.get('default_count') or fallback_value

    method = dyn_conf.get('method', 'linear')
    total_ranks_applied = dyn_conf.get('apply_to_rank_n', 100)

    # 统一处理 'multiplier' 和 'count'
    min_val_key = 'min_multiplier' if 'min_multiplier' in dyn_conf else 'min_count'
    max_val_key = 'max_multiplier' if 'max_multiplier' in dyn_conf else 'max_count'
    default_val_key = 'default_multiplier' if 'default_multiplier' in dyn_conf else 'default_count'

    min_val = dyn_conf.get(min_val_key, fallback_value)
    max_val = dyn_conf.get(max_val_key, min_val * 2)  # 提供一个合理的默认max值
    default_val = dyn_conf.get(default_val_key, max_val)

    if rank > total_ranks_applied:
        return default_val

    if method == 'linear':
        if total_ranks_applied <= 1: return min_val
        slope = (max_val - min_val) / (total_ranks_applied - 1)
        value = min_val + (rank - 1) * slope
        # K线数量需要是整数
        if 'count' in min_val_key:
            return int(round(max(min_val, min(value, max_val))))
        return max(min_val, min(value, max_val))

    elif method == 'linear_stepped':
        step_size = dyn_conf.get('rank_step_size', 10)
        if step_size <= 0: return min_val

        num_steps = math.ceil(total_ranks_applied / step_size)
        if num_steps <= 1: return min_val

        increment_per_step = (max_val - min_val) / (num_steps - 1)
        current_step_index = math.floor((rank - 1) / step_size)
        value = min_val + current_step_index * increment_per_step
        if 'count' in min_val_key:
            return int(round(max(min_val, min(value, max_val))))
        return max(min_val, min(value, max_val))

    elif method == 'stepped':
        tiers = sorted(dyn_conf.get('tiers', []), key=lambda x: x['up_to_rank'])
        tier_val_key = 'multiplier' if 'multiplier' in tiers[0] else 'count'
        for tier in tiers:
            if rank <= tier['up_to_rank']:
                return tier[tier_val_key]
        return default_val

    return fallback_value


def get_dynamic_volume_multiplier(symbol, config, fallback_multiplier):
    dyn_conf = config['strategy_params'].get('dynamic_volume_multipliers', {})
    return _calculate_dynamic_value(symbol, dyn_conf, fallback_multiplier)


def get_dynamic_atr_multiplier(symbol, config, fallback_multiplier):
    dyn_conf = config['strategy_params'].get('dynamic_atr_multipliers', {})
    return _calculate_dynamic_value(symbol, dyn_conf, fallback_multiplier)


def get_dynamic_consecutive_candles(symbol, config, fallback_count):
    dyn_conf = config['strategy_params'].get('consecutive_candles', {}).get('dynamic_count', {})
    return _calculate_dynamic_value(symbol, dyn_conf, fallback_count)


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