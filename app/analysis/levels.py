import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN


def calculate_pivot_points(daily_ohlc):
    if not isinstance(daily_ohlc, dict) or not all(k in daily_ohlc for k in ['high', 'low', 'close']):
        return [], []
    h, l, c = daily_ohlc['high'], daily_ohlc['low'], daily_ohlc['close']
    p = (h + l + c) / 3
    r1 = 2 * p - l;
    s1 = 2 * p - h
    r2 = p + (h - l);
    s2 = p - (h - l)
    r3 = h + 2 * (p - l);
    s3 = l - 2 * (h - p)
    resistances = [{'level': r, 'type': f'R{i + 1}'} for i, r in enumerate([r1, r2, r3])]
    supports = [{'level': s, 'type': f'S{i + 1}'} for i, s in enumerate([s1, s2, s3])]
    return resistances, supports


# 【最终优化版算法】
def find_price_interest_zones(df, atr_grouping_multiplier=0.5, min_cluster_size=2, min_separation_atr_mult=1.0):
    """
    通过对所有分形拐点进行DBSCAN聚类，识别出重要的价格兴趣区。
    这个算法是一体化的，能更好地处理区域的识别和分离。
    """
    zones = []
    period = 2

    is_fractal_high = df['high'].rolling(window=2 * period + 1, center=True).max() == df['high']
    is_fractal_low = df['low'].rolling(window=2 * period + 1, center=True).min() == df['low']

    fractal_highs_prices = df[is_fractal_high]['high'].dropna().values
    fractal_lows_prices = df[is_fractal_low]['low'].dropna().values

    all_fractals = np.concatenate([fractal_highs_prices, fractal_lows_prices]).reshape(-1, 1)
    if all_fractals.shape[0] < min_cluster_size:
        return []

    # 使用 pandas_ta 计算 ATR
    atr_period = 14
    df.ta.atr(length=atr_period, append=True)
    avg_atr = df[f"ATRr_{atr_period}"].dropna().mean()
    if pd.isna(avg_atr) or avg_atr == 0:
        return []

    # DBSCAN 的 eps (邻域半径) 由 ATR 动态决定
    # 这同时解决了“聚类”和“分离”的问题
    eps = avg_atr * atr_grouping_multiplier

    # min_samples 对应我们的 min_cluster_size
    db = DBSCAN(eps=eps, min_samples=min_cluster_size).fit(all_fractals)

    labels = db.labels_
    unique_labels = set(labels)

    for k in unique_labels:
        # -1 标签代表噪声点，我们忽略它
        if k == -1:
            continue

        class_member_mask = (labels == k)
        cluster_points = all_fractals[class_member_mask]

        # 计算区域的中心价位和强度
        zone_level = np.mean(cluster_points)
        strength = len(cluster_points)

        zones.append({
            'level': zone_level,
            'type': f'Zone ({strength} touches)'
        })

    # 按价格排序
    zones.sort(key=lambda x: x['level'], reverse=True)
    return zones