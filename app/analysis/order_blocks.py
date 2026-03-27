# --- START OF FILE app/analysis/order_blocks.py ---
import pandas as pd


def find_lux_order_blocks(df, swing_length=5):
    """
    【流派1: LuxAlgo 爆量订单块】
    基于成交量峰值 (Volume Pivots) 和局部极值，捕捉极限拐点。
    """
    length = swing_length
    if len(df) < length * 2 + 1: return None, None

    bull_obs, bear_obs = [], []

    for i in range(length, len(df) - length):
        vol_center = df['volume'].iloc[i]
        vol_left_max = df['volume'].iloc[i - length:i].max()
        vol_right_max = df['volume'].iloc[i + 1:i + length + 1].max()

        if (vol_center > vol_left_max) and (vol_center > vol_right_max):
            high_center = df['high'].iloc[i]
            low_center = df['low'].iloc[i]
            local_highest = df['high'].iloc[i - length:i].max()
            local_lowest = df['low'].iloc[i - length:i].min()

            if high_center >= local_highest:
                bear_obs.append({
                    'top': high_center, 'bottom': (high_center + low_center) / 2,
                    'index': i, 'timestamp': df['timestamp'].iloc[i], 'type': 'bearish'
                })
            elif low_center <= local_lowest:
                bull_obs.append({
                    'top': (high_center + low_center) / 2, 'bottom': low_center,
                    'index': i, 'timestamp': df['timestamp'].iloc[i], 'type': 'bullish'
                })

    # Mitigation (剔除失效块：被实体突破/跌破的过滤)
    valid_bull_obs = [ob for ob in bull_obs if not (df['low'].iloc[ob['index'] + 1:] < ob['bottom']).any()]
    valid_bear_obs = [ob for ob in bear_obs if not (df['high'].iloc[ob['index'] + 1:] > ob['top']).any()]

    return (valid_bull_obs[-1] if valid_bull_obs else None), (valid_bear_obs[-1] if valid_bear_obs else None)


def find_flux_order_blocks(df, swing_length=10, atr_multiplier=3.5):
    """
    【流派2: FluxCharts 结构订单块】
    基于市场结构破坏 (BOS) 并向后溯源起涨/起跌点，捕捉机构成本区。
    """
    if len(df) < swing_length * 2 + 1: return None, None

    df_copy = df.copy()
    df_copy.ta.atr(length=10, append=True)
    atr_col = "ATRr_10"

    df_copy['is_swing_high'] = df_copy['high'] == df_copy['high'].rolling(window=swing_length * 2 + 1,
                                                                          center=True).max()
    df_copy['is_swing_low'] = df_copy['low'] == df_copy['low'].rolling(window=swing_length * 2 + 1, center=True).min()

    bull_obs, bear_obs = [], []
    last_swing_high_idx, last_swing_low_idx = None, None
    high_crossed, low_crossed = True, True

    for i in range(swing_length, len(df_copy)):
        check_idx = i - swing_length
        if df_copy['is_swing_high'].iloc[check_idx]:
            last_swing_high_idx = check_idx
            high_crossed = False
        if df_copy['is_swing_low'].iloc[check_idx]:
            last_swing_low_idx = check_idx
            low_crossed = False

        current_close = df_copy['close'].iloc[i]
        current_atr = df_copy[atr_col].iloc[i] if atr_col in df_copy.columns else 0

        # 牛市 OB
        if last_swing_high_idx is not None and not high_crossed:
            if current_close > df_copy['high'].iloc[last_swing_high_idx]:
                high_crossed = True
                search_df = df_copy.iloc[last_swing_high_idx:i]
                if not search_df.empty:
                    lowest_idx = search_df['low'].idxmin()
                    top, bottom = df_copy['high'].iloc[lowest_idx], df_copy['low'].iloc[lowest_idx]
                    if (top - bottom) <= current_atr * atr_multiplier or current_atr == 0:
                        bull_obs.append({'top': top, 'bottom': bottom, 'break_idx': i,
                                         'timestamp': df_copy['timestamp'].iloc[lowest_idx], 'type': 'bullish'})

        # 熊市 OB
        if last_swing_low_idx is not None and not low_crossed:
            if current_close < df_copy['low'].iloc[last_swing_low_idx]:
                low_crossed = True
                search_df = df_copy.iloc[last_swing_low_idx:i]
                if not search_df.empty:
                    highest_idx = search_df['high'].idxmax()
                    top, bottom = df_copy['high'].iloc[highest_idx], df_copy['low'].iloc[highest_idx]
                    if (top - bottom) <= current_atr * atr_multiplier or current_atr == 0:
                        bear_obs.append({'top': top, 'bottom': bottom, 'break_idx': i,
                                         'timestamp': df_copy['timestamp'].iloc[highest_idx], 'type': 'bearish'})

    # Mitigation (剔除失效块)
    valid_bull_obs = [ob for ob in bull_obs if not (df_copy['low'].iloc[ob['break_idx'] + 1:] < ob['bottom']).any()]
    valid_bear_obs = [ob for ob in bear_obs if not (df_copy['high'].iloc[ob['break_idx'] + 1:] > ob['top']).any()]

    return (valid_bull_obs[-1] if valid_bull_obs else None), (valid_bear_obs[-1] if valid_bear_obs else None)
# --- END OF FILE app/analysis/order_blocks.py ---
