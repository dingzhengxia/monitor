# --- START OF FILE app/analysis/order_blocks.py (FINAL, NO PIVOTLOW VERSION) ---
import pandas as pd
import numpy as np
from loguru import logger
import pandas_ta as pta
from scipy.signal import find_peaks


def find_latest_order_blocks(df, swing_length=10, atr_multiplier=0.1):
    """
    【最终版】查找最新的牛市和熊市订单块。
    - 使用 SciPy find_peaks 实现无重绘的摆动点检测，可靠且无依赖问题。
    - 使用ATR确认结构破坏的有效性。
    - 精确定位订单块为冲击波前的最后一根反向K线。
    """
    if len(df) < swing_length * 2 + 2:
        return None, None

    df_copy = df.copy()

    # --- 核心替代方案: 使用 SciPy find_peaks 来识别摆动点 ---
    highs = df_copy['high'].to_numpy()
    lows = df_copy['low'].to_numpy()

    swing_high_indices, _ = find_peaks(highs, distance=swing_length)
    swing_low_indices, _ = find_peaks(-lows, distance=swing_length)

    # --- 引入ATR用于验证结构破坏的有效性 ---
    atr_period = 14
    df_copy.ta.atr(length=atr_period, append=True)
    atr_col = f"ATRr_{atr_period}"
    if atr_col not in df_copy.columns:
        return None, None

    latest_bullish_ob = None
    latest_bearish_ob = None

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
            up_candles = search_range_df[search_range_df['close'] > search_range_df['open']]

            if not up_candles.empty:
                ob_candle_idx = up_candles.index[-1]

            if ob_candle_idx != -1:
                latest_bearish_ob = {
                    'top': df_copy.at[ob_candle_idx, 'high'],
                    'bottom': df_copy.at[ob_candle_idx, 'low'],
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
            down_candles = search_range_df[search_range_df['close'] < search_range_df['open']]

            if not down_candles.empty:
                ob_candle_idx = down_candles.index[-1]

            if ob_candle_idx != -1:
                latest_bullish_ob = {
                    'top': df_copy.at[ob_candle_idx, 'high'],
                    'bottom': df_copy.at[ob_candle_idx, 'low'],
                    'timestamp': df_copy.at[ob_candle_idx, 'timestamp'],
                    'type': 'bullish'
                }
                break

    return latest_bullish_ob, latest_bearish_ob
# --- END OF FILE app/analysis/order_blocks.py (FINAL, NO PIVOTLOW VERSION) ---