# --- START OF FILE app/analysis/channels.py ---
import numpy as np
import pandas as pd
from scipy.signal import find_peaks
from loguru import logger


def detect_trend_channel(df: pd.DataFrame, lookback_period: int = 90, min_touches: int = 3):
    """
    使用线性回归和峰谷检测来识别趋势通道。

    :param df: 输入的OHLCV DataFrame。
    :param lookback_period: 用于分析的回看K线数量。
    :param min_touches: 构成有效趋势线所需的最少接触点（峰/谷）。
    :return: 如果找到通道，则返回包含通道信息的字典，否则返回 None。
    """
    if len(df) < lookback_period:
        return None

    # 只分析最近的数据
    data = df.iloc[-lookback_period:].copy()
    data['idx'] = np.arange(len(data))  # 创建索引用于线性回归

    # 使用scipy的find_peaks找到高点和低点
    # prominence参数有助于过滤掉不重要的微小波动
    avg_price_range = (data['high'] - data['low']).mean()
    high_peaks_idx, _ = find_peaks(data['high'], prominence=avg_price_range * 0.5)
    low_peaks_idx, _ = find_peaks(-data['low'], prominence=avg_price_range * 0.5)

    if len(high_peaks_idx) < min_touches or len(low_peaks_idx) < min_touches:
        # 没有足够的枢轴点来构建通道
        return None

    # ---- 尝试拟合下降通道 ----
    # 使用高点拟合阻力线
    res_highs_x = data.iloc[high_peaks_idx]['idx'].values
    res_highs_y = data.iloc[high_peaks_idx]['high'].values
    # 线性回归: y = slope * x + intercept
    res_slope, res_intercept = np.polyfit(res_highs_x, res_highs_y, 1)

    # 使用低点拟合支撑线
    sup_lows_x = data.iloc[low_peaks_idx]['idx'].values
    sup_lows_y = data.iloc[low_peaks_idx]['low'].values
    sup_slope, sup_intercept = np.polyfit(sup_lows_x, sup_lows_y, 1)

    # 验证是否为有效的下降通道
    # 1. 两条线斜率都为负
    # 2. 两条线大致平行 (斜率差异在一定容忍度内)
    # 3. 大部分K线都在通道内
    is_descending = res_slope < 0 and sup_slope < 0
    are_parallel = abs(res_slope - sup_slope) < abs(res_slope) * 0.3  # 容忍30%的斜率差异

    if is_descending and are_parallel:
        # 计算通道线在每个点的值
        data['resistance_line'] = res_slope * data['idx'] + res_intercept
        data['support_line'] = sup_slope * data['idx'] + sup_intercept

        # 验证条件：大部分高点低于阻力线，大部分低点高于支撑线
        is_valid_channel = (data['high'] < data['resistance_line'] + avg_price_range * 0.1).mean() > 0.8 and \
                           (data['low'] > data['support_line'] - avg_price_range * 0.1).mean() > 0.8

        if is_valid_channel:
            logger.trace(f"[{df.iloc[-1]['symbol']}|{df.iloc[-1]['timeframe']}] 检测到下降通道。")
            return {
                "type": "descending",
                "resistance_line": data['resistance_line'],
                "support_line": data['support_line'],
            }

    # ---- 尝试拟合上升通道 ----
    # (逻辑与下降通道类似)
    is_ascending = res_slope > 0 and sup_slope > 0
    are_parallel = abs(res_slope - sup_slope) < res_slope * 0.3

    if is_ascending and are_parallel:
        data['resistance_line'] = res_slope * data['idx'] + res_intercept
        data['support_line'] = sup_slope * data['idx'] + sup_intercept

        is_valid_channel = (data['high'] < data['resistance_line'] + avg_price_range * 0.1).mean() > 0.8 and \
                           (data['low'] > data['support_line'] - avg_price_range * 0.1).mean() > 0.8

        if is_valid_channel:
            logger.trace(f"[{df.iloc[-1]['symbol']}|{df.iloc[-1]['timeframe']}] 检测到上升通道。")
            return {
                "type": "ascending",
                "resistance_line": data['resistance_line'],
                "support_line": data['support_line'],
            }

    return None
# --- END OF FILE app/analysis/channels.py ---