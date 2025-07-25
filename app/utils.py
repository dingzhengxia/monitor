# --- START OF FILE app/utils.py (RESTORED to TEXT-ONLY VERSION) ---
import json
import logging
from datetime import datetime, timedelta, timezone
import math

from app.state import alerted_states

logger = logging.getLogger(__name__)
ALERT_STATUS_FILE = 'cooldown_status.json'


def load_alert_states():
    try:
        with open(ALERT_STATUS_FILE, 'r') as f:
            data = json.load(f)
        loaded_states = {k: datetime.fromisoformat(v).replace(tzinfo=timezone.utc) if datetime.fromisoformat(
            v).tzinfo is None else datetime.fromisoformat(v) for k, v in data.items()}
        now_utc = datetime.now(timezone.utc)
        initial_count = len(loaded_states)
        alerted_states.clear()
        alerted_states.update({k: v for k, v in loaded_states.items() if v > now_utc})
        logger.info(f"✅ 成功加载冷却状态。有效条目: {len(alerted_states)} (从 {initial_count} 个中加载)")
    except (FileNotFoundError, json.JSONDecodeError):
        logger.info("ℹ️ 未找到或无法解析冷却状态文件。");
        alerted_states.clear()


def save_alert_states():
    try:
        now_utc = datetime.now(timezone.utc)
        active_states = {k: v for k, v in alerted_states.items() if v > now_utc}
        alerted_states.clear()
        alerted_states.update(active_states)
        with open(ALERT_STATUS_FILE, 'w') as f:
            json.dump({k: v.isoformat() for k, v in active_states.items()}, f, indent=4)
    except Exception as e:
        logger.error(f"❌ 保存冷却状态到文件时出错: {e}", exc_info=True)


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


def calculate_cooldown_time(minutes):
    if minutes <= 0: return datetime.now(timezone.utc) + timedelta(minutes=1)
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)
# --- END OF FILE app/utils.py (RESTORED to TEXT-ONLY VERSION) ---
