# --- START OF FILE app/state.py (ULTIMATE CORRECTED V46.2 - FULL CODE) ---
import json
from datetime import datetime, timezone
import queue
from loguru import logger

ALERT_STATUS_FILE = 'cooldown_status.json'

# 全局共享的状态变量
alerted_states = {}
cached_top_symbols = []
notification_queue = queue.Queue()


# 状态操作函数
def load_alert_states():
    global alerted_states
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
    global alerted_states
    try:
        now_utc = datetime.now(timezone.utc)
        active_states = {k: v for k, v in alerted_states.items() if v > now_utc}
        alerted_states.clear()
        alerted_states.update(active_states)
        with open(ALERT_STATUS_FILE, 'w') as f:
            json.dump({k: v.isoformat() for k, v in active_states.items()}, f, indent=4)
    except Exception as e:
        logger.error(f"❌ 保存冷却状态到文件时出错: {e}", exc_info=True)
# --- END OF FILE app/state.py (ULTIMATE CORRECTED V46.2 - FULL CODE) ---
