# --- START OF FILE app/analysis/channels.py (DYNAMIC TREND SEGMENT ALGORITHM) ---
import numpy as np
import pandas as pd
from loguru import logger


def detect_regression_channel(df: pd.DataFrame, lookback_period: int = 100, min_trend_length: int = 20,
                              std_dev_multiplier: float = 2.0):
    """
    【终极版算法】自动检测趋势段并计算回归通道，与人工画线逻辑高度一致。

    1. 在`lookback_period`内找到最近的最高点和最低点。
    2. 确定哪个点是当前趋势的起点。
    3. 仅对从起点到现在的这个动态区间应用回归通道计算。
    """
    if len(df) < lookback_period:
        return None

    # 1. 在回看窗口内寻找趋势的潜在起点
    search_window = df.iloc[-lookback_period:]

    # idxmax()/idxmin() 返回的是索引标签，我们需要转换为整数位置
    last_high_pos = search_window['high'].argmax()
    last_low_pos = search_window['low'].argmin()

    # 确定趋势起点
    # 如果最高点比最低点更近，说明当前可能处于一个从高点开始的下降趋势
    # 反之，则处于一个从低点开始的上升趋势
    if last_high_pos > last_low_pos:
        trend_start_pos = last_high_pos
        trend_type = 'down'
    else:
        trend_start_pos = last_low_pos
        trend_type = 'up'

    # 2. 截取动态的趋势区间
    trend_df = search_window.iloc[trend_start_pos:].copy()

    # 3. 验证趋势区间的有效性
    if len(trend_df) < min_trend_length:
        logger.trace(
            f"[{df.iloc[-1]['symbol']}|{df.iloc[-1]['timeframe']}] 识别到的趋势段过短 ({len(trend_df)} < {min_trend_length})，跳过。")
        return None

    # 4. 对这个动态区间应用回归通道计算
    x = np.arange(len(trend_df))
    y = trend_df['close'].values

    slope, intercept = np.polyfit(x, y, 1)

    # 验证趋势方向是否与预期一致
    if (trend_type == 'up' and slope < 0) or (trend_type == 'down' and slope > 0):
        logger.trace(
            f"[{df.iloc[-1]['symbol']}|{df.iloc[-1]['timeframe']}] 趋势识别与斜率计算结果不符，可能是震荡市，跳过。")
        return None

    # 过滤横盘
    min_slope_threshold = trend_df['close'].mean() * 0.0005
    if abs(slope) < min_slope_threshold:
        logger.trace(f"[{df.iloc[-1]['symbol']}|{df.iloc[-1]['timeframe']}] 市场横盘，回归通道斜率过小，跳过。")
        return None

    trend_df['regression_line'] = slope * x + intercept

    deviations = trend_df['close'] - trend_df['regression_line']
    std_dev = np.std(deviations)

    trend_df['upper_band'] = trend_df['regression_line'] + (std_dev * std_dev_multiplier)
    trend_df['lower_band'] = trend_df['regression_line'] - (std_dev * std_dev_multiplier)

    logger.trace(
        f"[{df.iloc[-1]['symbol']}|{df.iloc[-1]['timeframe']}] 在过去{len(trend_df)}根K线中识别到 {trend_type} 趋势回归通道。")

    # 返回的结果现在只包含趋势段的数据
    return {
        "slope": slope,
        "upper_band": trend_df['upper_band'],
        "lower_band": trend_df['lower_band'],
        "trend_length": len(trend_df)
    }
# --- END OF FILE app/analysis/channels.py (DYNAMIC TREND SEGMENT ALGORITHM) ---