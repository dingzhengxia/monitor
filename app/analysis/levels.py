# --- START OF FILE app/analysis/levels.py ---
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from loguru import logger


def calculate_pivot_points(daily_ohlc):
    """
    计算经典枢轴点 (Pivot Points)
    """
    if not isinstance(daily_ohlc, dict) or not all(k in daily_ohlc for k in ['high', 'low', 'close']):
        return [], []
    h, l, c = daily_ohlc['high'], daily_ohlc['low'], daily_ohlc['close']
    p = (h + l + c) / 3
    r1 = 2 * p - l
    s1 = 2 * p - h
    r2 = p + (h - l)
    s2 = p - (h - l)
    r3 = h + 2 * (p - l)
    s3 = l - 2 * (h - p)
    resistances = [{'level': r, 'type': f'R{i + 1}'} for i, r in enumerate([r1, r2, r3])]
    supports = [{'level': s, 'type': f'S{i + 1}'} for i, s in enumerate([s1, s2, s3])]
    return resistances, supports


def _merge_close_levels(zones, min_separation):
    """
    后处理函数：合并或剔除距离过近的层级。
    优先保留强度 (touch_count) 更大的层级。
    """
    if not zones:
        return []

    sorted_zones = sorted(zones, key=lambda x: x['strength'], reverse=True)
    final_zones = []

    for zone in sorted_zones:
        is_too_close = False
        for existing in final_zones:
            if abs(zone['level'] - existing['level']) < min_separation:
                is_too_close = True
                break

        if not is_too_close:
            final_zones.append(zone)

    return sorted(final_zones, key=lambda x: x['level'], reverse=True)


def find_price_interest_zones(df, atr_grouping_multiplier=1.0, min_cluster_size=3, min_separation_atr_mult=1.5):
    """
    识别价格兴趣区 (DBSCAN 聚类)
    """
    if len(df) < 50:
        return []

    atr_period = 14
    df.ta.atr(length=atr_period, append=True)
    atr_col = f"ATRr_{atr_period}"

    if atr_col not in df.columns:
        return []

    current_atr = df[atr_col].iloc[-1]
    avg_atr = df[atr_col].dropna().mean()

    if pd.isna(avg_atr) or avg_atr == 0:
        return []

    period = 2
    is_fractal_high = df['high'].rolling(window=2 * period + 1, center=True).max() == df['high']
    is_fractal_low = df['low'].rolling(window=2 * period + 1, center=True).min() == df['low']

    fractal_highs_prices = df[is_fractal_high]['high'].dropna().values
    fractal_lows_prices = df[is_fractal_low]['low'].dropna().values

    all_fractals = np.concatenate([fractal_highs_prices, fractal_lows_prices]).reshape(-1, 1)

    if all_fractals.shape[0] < min_cluster_size:
        return []

    eps = avg_atr * atr_grouping_multiplier

    try:
        db = DBSCAN(eps=eps, min_samples=min_cluster_size).fit(all_fractals)
    except Exception as e:
        logger.error(f"DBSCAN clustering error: {e}")
        return []

    labels = db.labels_
    unique_labels = set(labels)

    raw_zones = []

    for k in unique_labels:
        if k == -1: continue

        class_member_mask = (labels == k)
        cluster_points = all_fractals[class_member_mask]

        zone_level = np.mean(cluster_points)
        strength = len(cluster_points)

        raw_zones.append({
            'level': zone_level,
            'strength': strength,
            'type': f'Zone ({strength} touches)'
        })

    separation_dist = current_atr * min_separation_atr_mult
    final_zones = _merge_close_levels(raw_zones, separation_dist)

    return final_zones


def find_market_structure_swings(df, left_bars=7, right_bars=7):
    """
    【实战交易级】寻找近期的波段高点(Swing High)和波段低点(Swing Low)。
    这就是交易员常说的“前高”和“前低”。突破它们构成 BOS (Break of Structure)。
    """
    if len(df) < left_bars + right_bars + 1:
        return []

    df_copy = df.copy()

    # 寻找波段高点 (Swing Highs)
    df_copy['is_swing_high'] = df_copy['high'] == df_copy['high'].rolling(window=left_bars + right_bars + 1,
                                                                          center=True).max()

    # 寻找波段低点 (Swing Lows)
    df_copy['is_swing_low'] = df_copy['low'] == df_copy['low'].rolling(window=left_bars + right_bars + 1,
                                                                       center=True).min()

    swing_levels = []

    # 提取最近的波段高点
    swing_highs = df_copy[df_copy['is_swing_high']].tail(5)
    for idx, row in swing_highs.iterrows():
        swing_levels.append({
            'level': row['high'],
            'type': '近期前高(Swing High)',
            'timestamp': row['timestamp']
        })

    # 提取最近的波段低点
    swing_lows = df_copy[df_copy['is_swing_low']].tail(5)
    for idx, row in swing_lows.iterrows():
        swing_levels.append({
            'level': row['low'],
            'type': '近期前低(Swing Low)',
            'timestamp': row['timestamp']
        })

    return swing_levels
# --- END OF FILE app/analysis/levels.py ---