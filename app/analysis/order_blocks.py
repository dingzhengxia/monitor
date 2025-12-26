# --- START OF FILE app/analysis/order_blocks.py (WITH MINIMUM THICKNESS) ---
import pandas as pd
from scipy.signal import find_peaks


def find_latest_order_blocks(df, swing_length=10, atr_multiplier=0.1):
    """
    【优化版】查找最新的牛市和熊市订单块。
    新增特性：
    - 最小厚度保证：如果识别到的订单块K线太薄（小于0.5倍ATR），
      会自动基于ATR将其扩充，防止区域过窄导致误报。
    """
    if len(df) < swing_length * 2 + 2:
        return None, None

    df_copy = df.copy()

    # --- 1. 使用 SciPy find_peaks 识别摆动点 ---
    highs = df_copy['high'].to_numpy()
    lows = df_copy['low'].to_numpy()

    swing_high_indices, _ = find_peaks(highs, distance=swing_length)
    swing_low_indices, _ = find_peaks(-lows, distance=swing_length)

    # --- 2. 计算 ATR (用于结构破坏验证 + 最小厚度计算) ---
    atr_period = 14
    df_copy.ta.atr(length=atr_period, append=True)
    atr_col = f"ATRr_{atr_period}"

    # 如果无法计算ATR，就没法做后续判断，直接返回
    if atr_col not in df_copy.columns:
        return None, None

    latest_bullish_ob = None
    latest_bearish_ob = None

    # 设定最小厚度系数 (例如: 区域宽度至少要达到 0.5 倍 ATR)
    MIN_THICKNESS_ATR_MULT = 0.5

    # --- 辅助函数：确保区域有一定厚度 ---
    def expand_zone_if_needed(top, bottom, atr_value):
        height = top - bottom
        min_height = atr_value * MIN_THICKNESS_ATR_MULT

        if height < min_height:
            # 区域太窄，进行中心扩散
            center = (top + bottom) / 2
            half_min = min_height / 2
            return center + half_min, center - half_min
        return top, bottom

    # 1. 寻找最新的熊市订单块 (Bearish OB - 阻力)
    for low_idx in reversed(swing_low_indices):
        if low_idx >= len(df_copy) - 1: continue

        swing_low_price = df_copy.at[low_idx, 'low']
        atr_at_swing = df_copy.at[low_idx, atr_col]
        if pd.isna(atr_at_swing) or atr_at_swing == 0: continue

        break_threshold = swing_low_price - (atr_at_swing * atr_multiplier)

        break_df = df_copy.loc[low_idx + 1:]
        break_indices = break_df[break_df['close'] < break_threshold].index

        if not break_indices.empty:
            break_idx = break_indices[0]

            ob_candle_idx = -1
            search_range_df = df_copy.loc[low_idx:break_idx]
            # 熊市OB通常是下跌前的最后一根阳线
            up_candles = search_range_df[search_range_df['close'] > search_range_df['open']]

            if not up_candles.empty:
                ob_candle_idx = up_candles.index[-1]

            if ob_candle_idx != -1:
                raw_top = df_copy.at[ob_candle_idx, 'high']
                raw_bottom = df_copy.at[ob_candle_idx, 'low']

                # 【核心优化】应用最小厚度逻辑
                final_top, final_bottom = expand_zone_if_needed(raw_top, raw_bottom, atr_at_swing)

                latest_bearish_ob = {
                    'top': final_top,
                    'bottom': final_bottom,
                    'timestamp': df_copy.at[ob_candle_idx, 'timestamp'],
                    'type': 'bearish'
                }
                break

    # 2. 寻找最新的牛市订单块 (Bullish OB - 支撑)
    for high_idx in reversed(swing_high_indices):
        if high_idx >= len(df_copy) - 1: continue

        swing_high_price = df_copy.at[high_idx, 'high']
        atr_at_swing = df_copy.at[high_idx, atr_col]
        if pd.isna(atr_at_swing) or atr_at_swing == 0: continue

        break_threshold = swing_high_price + (atr_at_swing * atr_multiplier)

        break_df = df_copy.loc[high_idx + 1:]
        break_indices = break_df[break_df['close'] > break_threshold].index

        if not break_indices.empty:
            break_idx = break_indices[0]

            ob_candle_idx = -1
            search_range_df = df_copy.loc[high_idx:break_idx]
            # 牛市OB通常是上涨前的最后一根阴线
            down_candles = search_range_df[search_range_df['close'] < search_range_df['open']]

            if not down_candles.empty:
                ob_candle_idx = down_candles.index[-1]

            if ob_candle_idx != -1:
                raw_top = df_copy.at[ob_candle_idx, 'high']
                raw_bottom = df_copy.at[ob_candle_idx, 'low']

                # 【核心优化】应用最小厚度逻辑
                final_top, final_bottom = expand_zone_if_needed(raw_top, raw_bottom, atr_at_swing)

                latest_bullish_ob = {
                    'top': final_top,
                    'bottom': final_bottom,
                    'timestamp': df_copy.at[ob_candle_idx, 'timestamp'],
                    'type': 'bullish'
                }
                break

    return latest_bullish_ob, latest_bearish_ob
# --- END OF FILE app/analysis/order_blocks.py (WITH MINIMUM THICKNESS) ---