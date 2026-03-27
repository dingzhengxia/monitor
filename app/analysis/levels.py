# --- START OF FILE app/analysis/levels.py ---
import pandas as pd


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

    # 提取最近的几个波段高点
    swing_highs = df_copy[df_copy['is_swing_high']].tail(5)
    for idx, row in swing_highs.iterrows():
        swing_levels.append({
            'level': row['high'],
            'type': '近期前高(Swing High)',
            'timestamp': row['timestamp']
        })

    # 提取最近的几个波段低点
    swing_lows = df_copy[df_copy['is_swing_low']].tail(5)
    for idx, row in swing_lows.iterrows():
        swing_levels.append({
            'level': row['low'],
            'type': '近期前低(Swing Low)',
            'timestamp': row['timestamp']
        })

    return swing_levels
# --- END OF FILE app/analysis/levels.py ---