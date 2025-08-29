# --- START OF FILE app/analysis/channels.py (REGRESSION CHANNEL ALGORITHM) ---
import numpy as np
import pandas as pd
from loguru import logger


def detect_regression_channel(df: pd.DataFrame, lookback_period: int = 90, std_dev_multiplier: float = 2.0):
    """
    【最终版算法】使用线性回归和标准差来构建通道，与TradingView的“回归趋势”工具一致。

    :param df: 输入的OHLCV DataFrame。
    :param lookback_period: 用于计算回归趋势的回看K线数量。
    :param std_dev_multiplier: 标准差倍数，用于确定通道宽度。
    :return: 如果成功构建通道，则返回包含通道信息的字典，否则返回 None。
    """
    if len(df) < lookback_period:
        return None

    data = df.iloc[-lookback_period:].copy()

    # 1. 准备数据进行线性回归
    # x 是时间索引 (0, 1, 2, ...), y 是收盘价
    x = np.arange(len(data))
    y = data['close'].values

    # 2. 计算线性回归线 (y = slope * x + intercept)
    slope, intercept = np.polyfit(x, y, 1)

    # 为了避免在横盘时发出无效信号，要求斜率有一定的幅度
    # 斜率的意义是“每根K线价格的平均变化量”
    # 我们要求这个变化量至少是平均价格的 0.05%
    min_slope_threshold = data['close'].mean() * 0.0005
    if abs(slope) < min_slope_threshold:
        logger.trace(f"[{df.iloc[-1]['symbol']}|{df.iloc[-1]['timeframe']}] 市场横盘，回归通道斜率过小，跳过。")
        return None

    data['regression_line'] = slope * x + intercept

    # 3. 计算价格相对于回归线的标准差
    deviations = data['close'] - data['regression_line']
    std_dev = np.std(deviations)

    # 4. 计算上下通道线
    data['upper_band'] = data['regression_line'] + (std_dev * std_dev_multiplier)
    data['lower_band'] = data['regression_line'] - (std_dev * std_dev_multiplier)

    logger.trace(f"[{df.iloc[-1]['symbol']}|{df.iloc[-1]['timeframe']}] 检测到回归通道，斜率: {slope:.4f}")
    return {
        "slope": slope,
        "upper_band": data['upper_band'],
        "lower_band": data['lower_band'],
    }
# --- END OF FILE app/analysis/channels.py (REGRESSION CHANNEL ALGORITHM) ---