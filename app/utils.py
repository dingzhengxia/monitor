import json
from datetime import datetime, timedelta, timezone
from loguru import logger
from app.state import alerted_states

ALERT_STATUS_FILE = 'cooldown_status.json'

def timeframe_to_minutes(tf_str):
    try:
        if not tf_str or len(tf_str) < 2: return 0
        num = int(tf_str[:-1]);
        unit = tf_str[-1].lower()
        if unit == 'm': return num
        if unit == 'h': return num * 60
        if unit == 'd': return num * 24 * 60
        if unit == 'w': return num * 7 * 24 * 60
        return 0
    except (ValueError, TypeError):
        return 0


# 【核心修改】这是最终的、支持对齐的冷却时间计算函数
def calculate_cooldown_time(minutes, align_to_period_end=True):
    """
    计算冷却到期时间。

    :param minutes: 冷却的分钟数。
    :param align_to_period_end: 如果为True，则将到期时间对齐到当前K线周期的结束。
                                minutes 参数此时代表K线周期。
    :return: datetime 对象。
    """
    now_utc = datetime.now(timezone.utc)

    if not align_to_period_end:
        # 传统模式：从现在开始加上指定的分钟数
        if minutes <= 0: return now_utc + timedelta(minutes=1)
        return now_utc + timedelta(minutes=minutes)
    else:
        # 新模式：对齐到周期结束
        # 此时 minutes 参数代表的是时间周期的分钟数
        period_minutes = int(minutes)
        if period_minutes <= 0: return now_utc + timedelta(minutes=1)

        # 处理日线及以上周期
        if period_minutes >= 1440:  # 1天 = 1440分钟
            # 对齐到当天的午夜 (UTC)
            period_end_time = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            return period_end_time

        # 计算自一天开始以来的总分钟数
        total_minutes_of_day = now_utc.hour * 60 + now_utc.minute

        # 计算当前时间点所属周期的开始分钟数
        start_minute_of_period = (total_minutes_of_day // period_minutes) * period_minutes

        # 构建周期开始时间
        period_start_time = now_utc.replace(hour=start_minute_of_period // 60,
                                            minute=start_minute_of_period % 60,
                                            second=0, microsecond=0)

        # 周期结束时间 = 周期开始时间 + 周期长度
        period_end_time = period_start_time + timedelta(minutes=period_minutes)

        # 如果计算出的结束时间早于现在（发生在边界情况，如21:59:59），则推到下一个周期
        if period_end_time < now_utc:
            period_end_time += timedelta(minutes=period_minutes)

        return period_end_time
