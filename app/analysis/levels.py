# --- START OF FILE app/analysis/levels.py (OPTIMIZED V2) ---
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
    【新增】后处理函数：合并或剔除距离过近的层级。
    优先保留强度 (touch_count) 更大的层级。
    """
    if not zones:
        return []

    # 按强度 (touch_count) 从大到小排序，优先保留强的
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

    # 最后按价格重新排序方便阅读
    return sorted(final_zones, key=lambda x: x['level'], reverse=True)


def find_price_interest_zones(df, atr_grouping_multiplier=1.0, min_cluster_size=3, min_separation_atr_mult=1.5):
    """
    【升级版】识别价格兴趣区
    1. 识别分形拐点
    2. DBSCAN 聚类
    3. 强行去重 (剔除过近的层级)
    """
    if len(df) < 50:
        return []

    # 1. 计算ATR
    atr_period = 14
    df.ta.atr(length=atr_period, append=True)
    atr_col = f"ATRr_{atr_period}"

    if atr_col not in df.columns:
        return []

    current_atr = df[atr_col].iloc[-1]
    avg_atr = df[atr_col].dropna().mean()

    if pd.isna(avg_atr) or avg_atr == 0:
        return []

    # 2. 识别分形 (Fractals) - 也就是顶底拐点
    # 使用 5 根 K 线判定一个拐点 (前2后2)
    period = 2
    is_fractal_high = df['high'].rolling(window=2 * period + 1, center=True).max() == df['high']
    is_fractal_low = df['low'].rolling(window=2 * period + 1, center=True).min() == df['low']

    fractal_highs_prices = df[is_fractal_high]['high'].dropna().values
    fractal_lows_prices = df[is_fractal_low]['low'].dropna().values

    all_fractals = np.concatenate([fractal_highs_prices, fractal_lows_prices]).reshape(-1, 1)

    if all_fractals.shape[0] < min_cluster_size:
        return []

    # 3. DBSCAN 聚类
    # eps: 两个点被视为“邻居”的最大距离。这里用 ATR 的倍数。
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
        # -1 代表噪声点，忽略
        if k == -1:
            continue

        class_member_mask = (labels == k)
        cluster_points = all_fractals[class_member_mask]

        # 区域的核心价格 = 聚类点的平均值
        zone_level = np.mean(cluster_points)
        # 区域的强度 = 包含的点数 (触碰次数)
        strength = len(cluster_points)

        raw_zones.append({
            'level': zone_level,
            'strength': strength,
            'type': f'Zone ({strength} touches)'
        })

    # 4. 后处理：强行合并太近的层级
    # 定义“太近”的标准：这里用 min_separation_atr_mult * 当前ATR
    separation_dist = current_atr * min_separation_atr_mult

    final_zones = _merge_close_levels(raw_zones, separation_dist)

    return final_zones
# --- END OF FILE app/analysis/levels.py (OPTIMIZED V2) ---