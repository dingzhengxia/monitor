# --- START OF FILE main.py (UPDATED V46.1 - FULL CODE) ---
import signal, sys, json, logging, threading
import ccxt
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from app.config import load_config
from app.logging_setup import setup_logging
from app.state import load_alert_states
from app.tasks.daily_reporter import run_daily_report
from app.tasks.signal_scanner import run_signal_check_cycle
from app.services.notification_service import notification_consumer


def handle_exit(signum, frame):
    logging.getLogger().info("\n👋 收到退出信号，程序正在优雅关闭...")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, handle_exit);
    signal.signal(signal.SIGTERM, handle_exit)
    try:
        config = load_config()
    except (FileNotFoundError, ValueError) as e:
        print(f"错误: {e}"); return
    logger = setup_logging(config.get('app_settings', {}).get("log_level", "INFO"))
    load_alert_states()
    app_conf = config.get('app_settings', {})
    try:
        exchange = getattr(ccxt, app_conf.get('exchange'))(
            {'enableRateLimit': True, 'options': {'defaultType': app_conf.get('default_market_type')}})
    except (AttributeError, KeyError) as e:
        logger.error(f"❌ 初始化交易所失败: 配置错误或交易所不支持 - {e}"); return
    except Exception as e:
        logger.error(f"❌ 初始化交易所时发生未知错误: {e}", exc_info=True); return

    logger.info("🚀 终极监控与信号程序已启动 (V46.1 - Bug修复版)")
    logger.info(
        f"📊 交易所: {app_conf.get('exchange')} | 市场: {app_conf.get('default_market_type')} | 间隔: {app_conf.get('check_interval_minutes')} 分钟")

    consumer_thread = threading.Thread(target=notification_consumer, daemon=True)
    consumer_thread.start()
    logger.info("✅ 通知队列消费者线程已启动。")

    logger.info("\n📌 首次运行主监控循环...")
    run_signal_check_cycle(exchange, config)
    if config.get('daily_report', {}).get('enabled', False):
        logger.info("\n📌 首次运行市场报告...")
        try:
            run_daily_report(exchange, config)
        except Exception as e:
            logger.error(f"首次市场报告失败: {e}", exc_info=True)
    scheduler = BlockingScheduler(timezone='Asia/Shanghai')
    if config.get('daily_report', {}).get('enabled', False):
        report_conf = config['daily_report']
        scan_time = report_conf.get('scan_time_beijing', '08:30').split(':')
        scheduler.add_job(run_daily_report, CronTrigger(hour=scan_time[0], minute=scan_time[1]),
                          args=[exchange, config], name="DailyReport")
        logger.info(f"   - 每日市场报告已添加，将在每天北京时间 {scan_time[0]}:{scan_time[1]} 运行。")
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
# --- END OF FILE main.py (UPDATED V46.1 - FULL CODE) ---
