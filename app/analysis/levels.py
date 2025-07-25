# --- START OF FILE app/analysis/levels.py (ULTIMATE FIX V50.1 - FULL CODE) ---
import pandas as pd


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


# 【V50.1 最终修复版】: 集成最小间距过滤
def find_price_interest_zones(df, atr_grouping_multiplier=0.5, min_cluster_size=2, min_separation_atr_mult=1.0):
    """
    通过对所有分形拐点进行统一聚类，并应用最小间距过滤，识别出重要的价格兴趣区。
    """
    zones = []
    period = 2

    is_fractal_high = df['high'].rolling(window=2 * period + 1, center=True).max() == df['high']
    is_fractal_low = df['low'].rolling(window=2 * period + 1, center=True).min() == df['low']

    fractal_highs_df = df[is_fractal_high][['high']].rename(columns={'high': 'price'})
    fractal_lows_df = df[is_fractal_low][['low']].rename(columns={'low': 'price'})

    all_fractals = pd.concat([fractal_highs_df, fractal_lows_df]).sort_values(by='price', ascending=False)
    if all_fractals.empty: return []

    atr_period = 14
    df.ta.atr(length=atr_period, append=True)
    avg_atr = df[f"ATRr_{atr_period}"].dropna().mean()
    if pd.isna(avg_atr) or avg_atr == 0: return []

    cluster_radius = avg_atr * atr_grouping_multiplier
    min_separation = avg_atr * min_separation_atr_mult

    # 聚类
    clusters = []
    while not all_fractals.empty:
        seed_price = all_fractals.iloc[0]['price']
        cluster_points = all_fractals[abs(all_fractals['price'] - seed_price) <= cluster_radius]
        if len(cluster_points) >= min_cluster_size:
            cluster_level = cluster_points['price'].mean()
            cluster_strength = len(cluster_points)
            clusters.append({'level': cluster_level, 'strength': cluster_strength})
        all_fractals = all_fractals.drop(cluster_points.index)

    # 最小间距过滤
    if len(clusters) < 2:
        return [{'level': c['level'], 'type': f'Zone ({c["strength"]} touches)'} for c in clusters]

    clusters.sort(key=lambda x: x['level'])

    final_clusters = [clusters[0]]
    for i in range(1, len(clusters)):
        current_level = clusters[i]['level']
        last_final_level = final_clusters[-1]['level']

        if abs(current_level - last_final_level) < min_separation:
            if clusters[i]['strength'] > final_clusters[-1]['strength']:
                final_clusters[-1] = clusters[i]
        else:
            final_clusters.append(clusters[i])

    for cluster in final_clusters:
        zones.append({'level': cluster['level'], 'type': f'Zone ({cluster["strength"]} touches)'})

    return zones
# --- END OF FILE app/analysis/levels.py (ULTIMATE FIX V50.1 - FULL CODE) ---
