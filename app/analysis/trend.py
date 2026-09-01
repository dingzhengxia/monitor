# --- START OF FILE app/analysis/trend.py (CORRECTED V35.2) ---
import pandas_ta as pta
from loguru import logger
# 本地应用导入
from app.utils import timeframe_to_minutes  # <-- 从 utils 导入


def get_current_trend(df, timeframe, trend_params_config):
    df_trend = df.copy()
    tf_minutes = timeframe_to_minutes(timeframe)
    trend_params = trend_params_config.get('trend_ema_short' if tf_minutes <= 60 else 'trend_ema_long',
                                           trend_params_config.get('trend_ema', {}))

    emas = {'fast': trend_params.get('fast'), 'medium': trend_params.get('medium'), 'long': trend_params.get('long')}
    if not all(period and period > 0 for period in emas.values()):
        logger.debug(f"趋势EMA参数配置不完整或周期不合法: {trend_params}")
        return "趋势未知", "↔️"

    for name, period in emas.items():
        df_trend[f'ema_{name}'] = pta.ema(df_trend['close'], length=period)

    required_cols = [f'ema_{name}' for name in emas.keys()]
    if not all(c in df_trend.columns for c in required_cols):
        return "趋势未知", "↔️"

    df_trend = df_trend.dropna()
    if df_trend.empty:
        return "趋势未知", "↔️"

    last = df_trend.iloc[-1]
    if last['ema_fast'] > last['ema_medium'] and last['ema_medium'] > last['ema_long']:
        return "多头趋势", "🐂"
    if last['ema_fast'] < last['ema_medium'] and last['ema_medium'] < last['ema_long']:
        return "空头趋势", "🐻"

    return "震荡趋势", "↔️"
# --- END OF FILE app/analysis/trend.py (CORRECTED V35.2) ---