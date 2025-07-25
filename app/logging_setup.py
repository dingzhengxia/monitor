# app/logging_setup.py
from loguru import logger  # 全局对象
import os


def setup_logging(level="INFO"):
    # 1. 创建日志目录
    log_dir = "log"
    os.makedirs(log_dir, exist_ok=True)

    # 2. 清除默认配置
    logger.remove()

    # 3. 控制台输出
    logger.add(
        sink=lambda msg: print(msg, end=""),
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:8}</level> | <cyan>{module}.{function}:{line}</cyan> - {message}",
        level=level,
        colorize=True
    )

    # 4. 文件输出
    log_file = os.path.join(log_dir, "monitor.log")
    logger.add(
        sink=log_file,
        rotation="00:00",
        retention="7 days",
        compression="zip",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:8}</level> | <cyan>{module}.{function}:{line}</cyan> - {message}",
        level=level,
        enqueue=True
    )

    # 5. 动态过滤（不重新赋值logger）
    def filter_low_level(record):
        if record["name"] in ["ccxt.base.exchange", "urllib3"]:
            return record["level"].no >= logger.level("INFO").no
        if record["name"] == "apscheduler.executors.default":
            return record["level"].no >= logger.level("WARNING").no
        return True

    logger.patch(filter_low_level)  # ✅ 直接应用过滤

    return logger  # ✅ 返回全局对象