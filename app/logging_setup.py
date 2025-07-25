# --- START OF FILE app/logging_setup.py (UPDATED to Silence ccxt) ---
import logging
from logging.handlers import TimedRotatingFileHandler


def setup_logging(level="INFO"):
    log_level = getattr(logging, level.upper(), logging.INFO)

    # 1. 获取根 logger 并设置全局级别
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # 清理旧的 handlers，防止重复输出
    if logger.hasHandlers():
        logger.handlers.clear()

    # 2. 创建并设置格式化器
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S')

    # 3. 创建 handlers
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_format)

    file_handler = TimedRotatingFileHandler(
        filename='monitor.log', when='midnight', interval=1, backupCount=7, encoding='utf-8'
    )
    file_handler.setFormatter(file_format)

    # 4. 将 handlers 添加到根 logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # 5. 【核心修改】: 精准屏蔽特定库的 DEBUG 日志
    # 无论全局级别多低，都让这些库保持安静
    logging.getLogger('ccxt.base.exchange').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)
    logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)

    return logger
# --- END OF FILE app/logging_setup.py (UPDATED to Silence ccxt) ---
