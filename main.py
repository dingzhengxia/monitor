import sys
import platform

# V-- 使用这个终极版补丁 --V
# 解决 pandas-ta 在 Windows 上导入 posix 和 pread 的问题
if platform.system() == "Windows":
    class FakePosix:
        """一个模拟模块，提供一个空的 pread 函数来满足导入需求"""

        def pread(self, *args, **kwargs):
            # 这是一个空函数，什么也不做
            pass


    # 将这个模拟类的实例作为 'posix' 模块放入缓存
    sys.modules['posix'] = FakePosix()
# ^-- 补丁结束 --^

import signal
import threading

import ccxt
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from app.config import load_config
from app.logging_setup import setup_logging
from app.services.notification_service import notification_consumer
from app.state import load_alert_states, save_alert_states
from app.tasks.periodic_reporter import run_periodic_report
from app.tasks.signal_scanner import run_signal_check_cycle
from app.tasks.position_protection import protect_positions
from app.utils import timeframe_to_minutes


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
    exchange_kwargs = {
        'enableRateLimit': True,
        'options': {
            'defaultType': app_conf.get('default_market_type')
        }
    }

    # 从环境变量读取 API
    # 不把 API Key 写进 config.json
    import os

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if api_key and api_secret:
        exchange_kwargs["apiKey"] = api_key
        exchange_kwargs["secret"] = api_secret
    else:
        logger.warning(
            "⚠️ 未发现 BINANCE_API_KEY / BINANCE_API_SECRET，"
            "仓位止损保护将无法访问真实账户。"
        )

    try:
        exchange = getattr(ccxt, app_conf.get('exchange'))(
            exchange_kwargs
        )
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

    # 首次运行时，为所有启用的报告都运行一次
    report_configs = config.get('periodic_reports', [])
    if report_configs:
        logger.info("\n📌 首次运行所有已启用的市场报告...")
        for report_conf in report_configs:
            if report_conf.get('enabled', False):
                try:
                    run_periodic_report(exchange, config, report_conf)
                except Exception as e:
                    logger.error(f"首次运行 '{report_conf.get('report_name')}' 失败: {e}", exc_info=True)

    scheduler = BlockingScheduler(timezone='Asia/Shanghai')

    if report_configs:
        logger.info("正在配置周期性报告任务...")
        for idx, report_conf in enumerate(report_configs):
            if report_conf.get('enabled', False):
                report_name = report_conf.get('report_name', f'报告任务-{idx + 1}')
                run_interval_str = report_conf.get('run_interval', '4h')

                run_interval_minutes = timeframe_to_minutes(run_interval_str)
                if run_interval_minutes == 0 or (run_interval_minutes < 1440 and 1440 % run_interval_minutes != 0):
                    logger.error(
                        f"❌ 报告 '{report_name}' 的间隔 '{run_interval_str}' 无效 (无法被24小时整除)，将跳过此任务。")
                    continue

                if run_interval_str == '1d':
                    # 日报，在北京时间每天早上8点运行
                    # 关键修改：显式添加 timezone='Asia/Shanghai'
                    trigger = CronTrigger(hour='8', minute='0', second='10', timezone='Asia/Shanghai')
                else:  # 小时报告
                    run_interval_hours = run_interval_minutes // 60
                    trigger_hours = ",".join([str(h) for h in range(0, 24, run_interval_hours)])
                    # 关键修改：显式添加 timezone='Asia/Shanghai'
                    trigger = CronTrigger(hour=trigger_hours, minute='0', second='10', timezone='Asia/Shanghai')

                scheduler.add_job(run_periodic_report,
                                  trigger,
                                  args=[exchange, config, report_conf],
                                  name=report_name)
                logger.info(f"   - ✅ 已添加 '{report_name}'，调度规则: {trigger}。")

    interval_minutes = app_conf.get('check_interval_minutes', 15)
    scheduler.add_job(run_signal_check_cycle, IntervalTrigger(minutes=interval_minutes), args=[exchange, config],
                      name="SignalCheckCycle")
    logger.info(f"   - 动态热点监控任务已添加，每 {interval_minutes} 分钟运行一次。")
    # ============================================================
    # 仓位止损保护任务
    # 独立于主信号扫描
    # ============================================================

    position_protection_conf = config.get(
        "position_protection",
        {}
    )

    if position_protection_conf.get("enabled", True):
        protection_interval = position_protection_conf.get(
            "interval_minutes",
            5
        )

        scheduler.add_job(
            protect_positions,
            IntervalTrigger(minutes=protection_interval),
            args=[exchange, config],
            name="PositionProtection",
            max_instances=1,
            coalesce=True,
        )

        logger.info(
            f"🛡️ 仓位止损保护任务已添加："
            f"每 {protection_interval} 分钟检查一次，"
            f"止损距离 "
            f"{position_protection_conf.get('stop_loss_percent', 2.0)}%"
        )

    logger.info(f"\n📅 调度器已启动，请保持程序运行。按 Ctrl+C 退出。")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
