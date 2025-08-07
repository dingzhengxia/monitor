import signal
import sys
import threading

import ccxt
from apscheduler.schedulers.blocking import BlockingScheduler
# 【核心修改】导入 CronTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from app.config import load_config
from app.logging_setup import setup_logging
from app.services.notification_service import notification_consumer
from app.state import load_alert_states, save_alert_states
from app.tasks.periodic_reporter import run_periodic_report
from app.tasks.signal_scanner import run_signal_check_cycle


def handle_exit(signum, frame):
    logger.info("\n👋 收到退出信号，正在保存状态并优雅关闭...")
    save_alert_states()
    logger.info("✅ 冷却状态已保存。程序退出。")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, handle_exit);
    signal.signal(signal.SIGTERM, handle_exit)
    try:
        config = load_config()
    except (FileNotFoundError, ValueError) as e:
        print(f"错误: {e}");
        return
    logger = setup_logging(config.get('app_settings', {}).get("log_level", "INFO"))

    load_alert_states()

    app_conf = config.get('app_settings', {})
    try:
        exchange = getattr(ccxt, app_conf.get('exchange'))(
            {'enableRateLimit': True, 'options': {'defaultType': app_conf.get('default_market_type')}})
    except (AttributeError, KeyError) as e:
        logger.error(f"❌ 初始化交易所失败: 配置错误或交易所不支持 - {e}");
        return
    except Exception as e:
        logger.error(f"❌ 初始化交易所时发生未知错误: {e}", exc_info=True);
        return

    logger.info("🚀 终极监控与信号程序已启动")
    logger.info(
        f"📊 交易所: {app_conf.get('exchange')} | 市场: {app_conf.get('default_market_type')} | 间隔: {app_conf.get('check_interval_minutes')} 分钟")

    consumer_thread = threading.Thread(target=notification_consumer, daemon=True)
    consumer_thread.start()
    logger.info("✅ 通知队列消费者线程已启动。")

    logger.info("\n📌 首次运行主监控循环...")
    run_signal_check_cycle(exchange, config)
    if config.get('periodic_report', {}).get('enabled', False):
        logger.info("\n📌 首次运行市场报告...")
        try:
            run_periodic_report(exchange, config)
        except Exception as e:
            logger.error(f"首次市场报告失败: {e}", exc_info=True)

    scheduler = BlockingScheduler(timezone='Asia/Shanghai')

    # 【核心修改】更新报告任务的调度逻辑，实现整点对齐
    if config.get('periodic_report', {}).get('enabled', False):
        report_conf = config['periodic_report']
        run_interval_hours = report_conf.get('run_every_hours', 4)

        # --- 动态生成 Cron 触发时间 ---
        # 确保间隔是有效的
        valid_intervals = [1, 2, 3, 4, 6, 8, 12, 24]
        if run_interval_hours not in valid_intervals:
            logger.warning(f"⚠️ 无效的报告间隔 {run_interval_hours} 小时，将默认使用4小时。")
            run_interval_hours = 4

        # 计算一天中所有对齐的触发小时
        trigger_hours = [str(h) for h in range(0, 24, run_interval_hours)]
        trigger_hours_str = ",".join(trigger_hours)
        # ---

        scheduler.add_job(run_periodic_report,
                          CronTrigger(hour=trigger_hours_str, minute='0', second='5'),  # 在整点后5秒触发，确保数据已准备好
                          args=[exchange, config],
                          name="PeriodicReport")
        logger.info(f"   - 周期性市场报告已添加，将在每天的 {trigger_hours_str} 点整运行。")

    interval_minutes = app_conf.get('check_interval_minutes', 15)
    scheduler.add_job(run_signal_check_cycle, IntervalTrigger(minutes=interval_minutes), args=[exchange, config],
                      name="SignalCheckCycle")
    logger.info(f"   - 动态热点监控任务已添加，每 {interval_minutes} 分钟运行一次。")

    logger.info(f"\n📅 调度器已启动，请保持程序运行。按 Ctrl+C 退出。")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()