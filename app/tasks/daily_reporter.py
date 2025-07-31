# --- START OF FILE app/tasks/daily_reporter.py (WITH IGNORE_FILTERS FLAG) ---
import time
from datetime import datetime, timedelta

from app.services.data_fetcher import get_top_n_symbols_by_volume, fetch_ohlcv_data
from app.services.notification_service import send_alert
from app.state import cached_top_symbols
from loguru import logger


def _get_symbol_in_primary_market(base_symbol, config):
    """辅助函数，将基础货币转换为带计价货币的完整交易对名称"""
    primary_quote = config.get('market_settings', {}).get('dynamic_scan', {}).get('primary_quote_currency',
                                                                                  'USDT').upper()
    market_type = config.get('app_settings', {}).get('default_market_type', 'swap')

    if market_type == 'swap':
        if primary_quote == "USDT":
            return f"{base_symbol.upper()}/USDT:USDT"
        return f"{base_symbol.upper()}/{primary_quote}:{primary_quote}"
    else:  # spot
        return f"{base_symbol.upper()}/{primary_quote}"


def _update_cache_for_report(exchange, config):
    logger.info(" (报告任务)正在更新热门币种缓存...")

    dyn_scan_conf = config.get('market_settings', {}).get('dynamic_scan', {})
    report_conf = config.get('daily_report', {})
    top_n_for_signals = dyn_scan_conf.get('top_n_for_signals', 100)
    top_n_for_report = report_conf.get('top_n_by_volume', 100)
    fetch_n = max(top_n_for_signals, top_n_for_report)

    # 【核心修改】: 调用时传递 ignore_adv_filters=True
    dynamic_symbols_list = get_top_n_symbols_by_volume(
        exchange,
        top_n=fetch_n,
        exclude_list=[s.upper() for s in dyn_scan_conf.get('exclude_symbols', [])],
        market_type=config.get('app_settings', {}).get('default_market_type', 'swap'),
        config=config,
        ignore_adv_filters=True  # <--- 告诉函数忽略配置文件中的跨市场等高级筛选
    )

    static_bases = config.get('market_settings', {}).get('static_symbols', [])
    static_symbols_list = [_get_symbol_in_primary_market(base, config) for base in static_bases]

    final_list = list(dynamic_symbols_list)
    for s in static_symbols_list:
        if s not in final_list:
            final_list.append(s)

    cached_top_symbols.clear()
    cached_top_symbols.extend(final_list)
    logger.info(f"✅ (报告任务)热门币种缓存已更新，当前共监控 {len(cached_top_symbols)} 个交易对。")


def run_daily_report(exchange, config):
    logger.info("--- ☀️ 开始执行每日宏观市场报告 (合约市场) ---")
    try:
        _update_cache_for_report(exchange, config)
        if not cached_top_symbols:
            logger.warning("报告任务中止：热门币种缓存为空。")
            return

        report_conf = config.get('daily_report', {})
        symbols_to_scan = cached_top_symbols[:report_conf.get('top_n_by_volume', 100)]

        gainers_list, consecutive_up_list, volume_ratio_list = [], [], []
        logger.info(f"...正在基于 {len(symbols_to_scan)} 个热门合约生成报告...")

        required_len = 200

        for symbol in symbols_to_scan:
            try:
                df = fetch_ohlcv_data(exchange, symbol, '1d', limit=required_len)
                if df is None or len(df) < report_conf.get('volume_ma_period', 20) + 2:
                    continue

                yesterday = df.iloc[-2]
                if yesterday['open'] > 0:
                    gainers_list.append({'symbol': symbol,
                                         'gain': ((yesterday['close'] - yesterday['open']) / yesterday['open']) * 100})

                count = 0
                for i in range(2, len(df) + 1):
                    if df.iloc[-i]['close'] > df.iloc[-i]['open']:
                        count += 1
                    else:
                        break
                if count >= report_conf.get('min_consecutive_days', 2):
                    consecutive_up_list.append({'symbol': symbol, 'days': count})

                df['volume_ma'] = df['volume'].rolling(window=report_conf.get('volume_ma_period', 20)).mean().shift(1)
                vol_ma = df.iloc[-2]['volume_ma']
                if vol_ma and vol_ma > 0:
                    volume_ratio_list.append(
                        {'symbol': symbol, 'ratio': yesterday['volume'] / vol_ma, 'volume': yesterday['volume'],
                         'volume_ma': vol_ma})

                time.sleep(exchange.rateLimit / 2000)
            except Exception as e:
                logger.debug(f"扫描 {symbol} 报告时出错: {e}")
                continue

        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        title = f"📰 {date_str} 合约市场热点报告"
        message = ""

        if gainers_list:
            sorted_gainers = sorted(gainers_list, key=lambda x: x['gain'], reverse=True)[
                             :report_conf.get('top_n_gainers', 10)]
            message += f"### 🚀 合约昨日涨幅榜\n\n"
            for i, item in enumerate(sorted_gainers):
                message += f"{'🥇🥈🥉🔥'[i if i < 4 else 3]} **{item['symbol']}**\n> **涨幅: {item['gain']:.2f}%**\n\n"

        if volume_ratio_list:
            sorted_ratios = sorted(volume_ratio_list, key=lambda x: x['ratio'], reverse=True)[
                            :report_conf.get('top_n_volume_ratio', 10)]
            message += f"\n---\n\n### 📈 昨日成交量异动榜\n\n"
            for i, item in enumerate(sorted_ratios):
                message += (f"{'🥇🥈🥉⚡️'[i if i < 4 else 3]} **{item['symbol']}**\n"
                            f"> **放量倍数: {item['ratio']:.2f} 倍**\n"
                            f"> (昨日量: {item['volume']:.0f}, 均量: {item['volume_ma']:.0f})\n\n")

        final_consecutive_list = sorted(consecutive_up_list, key=lambda x: x['days'], reverse=True)
        if final_consecutive_list:
            message += f"\n---\n\n### 💪 连涨强势合约\n\n"
            for item in final_consecutive_list:
                message += f"💪 **{item['symbol']}**\n> **连涨: {item['days']} 天** {'🔥' * (item['days'] // 2) if item['days'] > 3 else '🔥' if item['days'] == 3 else ''}\n\n"

        if message:
            send_alert(config, title, message, "Market Report")

        logger.info("--- ✅ 每日宏观市场报告完成 ---")
    except Exception as e:
        logger.error(f"❌ 执行每日报告任务时发生严重错误: {e}", exc_info=True)
