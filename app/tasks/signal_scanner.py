# --- START OF FILE app/tasks/signal_scanner.py (FIXED) ---
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 本地应用导入
from app.state import cached_top_symbols
from app.services.data_fetcher import fetch_ohlcv_data, get_top_n_symbols_by_volume  # 修正导入
from app.analysis.strategies import (
    check_ema_signals, check_kdj_cross, check_volatility_breakout,
    check_volume_breakout, check_rsi_divergence
)

logger = logging.getLogger(__name__)


# 为了保持职责清晰，扫描器自己负责在需要时更新缓存
def _update_cache(exchange, config):
    logger.info(" (扫描任务)正在更新热门币种缓存...")
    dyn_scan_conf = config['market_settings']['dynamic_scan']
    new_symbols = get_top_n_symbols_by_volume(
        exchange,
        top_n=dyn_scan_conf['top_n_for_signals'],
        exclude_list=[s.upper() for s in dyn_scan_conf['exclude_symbols']],
        market_type=config['app_settings']['default_market_type']
    )
    if new_symbols:
        fixed_symbols = set(config['market_settings']['static_symbols'])
        cached_top_symbols.clear()
        cached_top_symbols.extend(sorted(list(fixed_symbols.union(set(new_symbols)))))
        logger.info(f"✅ (扫描任务)热门币种缓存已更新，当前共监控 {len(cached_top_symbols)} 个交易对。")
    else:
        logger.warning("(扫描任务)更新缓存失败。")


STRATEGY_MAP = {
    'ema_cross': {'func': check_ema_signals, 'limit': 170},
    'kdj_cross': {'func': check_kdj_cross, 'limit': 170},
    'volatility_breakout': {'func': check_volatility_breakout, 'limit': 170},
    'volume_breakout': {'func': check_volume_breakout, 'limit': 200},
    'rsi_divergence': {'func': check_rsi_divergence, 'limit': 170},
}


def _check_symbol_all_strategies(symbol, exchange, config):
    logger.debug(f"--- [Thread] 正在检查: {symbol} ---")

    for timeframe in config['market_settings']['timeframes']:
        max_limit = max(s['limit'] for s in STRATEGY_MAP.values())
        df = fetch_ohlcv_data(exchange, symbol, timeframe, max_limit)

        if df is None:
            logger.debug(f"无法获取 {symbol} {timeframe} 的数据，跳过本轮检查。")
            continue

        for name, strategy_info in STRATEGY_MAP.items():
            if config['strategy_params'].get(name, {}).get('enabled', False):
                try:
                    strategy_info['func'](exchange, symbol, timeframe, config, df.copy())
                except Exception as e:
                    logger.error(f"执行策略 {name} on {symbol} {timeframe} 时发生顶层错误: {e}", exc_info=True)

    return f"已完成 {symbol} 的检查"


def run_signal_check_cycle(exchange, config):
    logger.info("=" * 60)
    logger.info(f"🔄 开始执行动态热点监控循环...")

    if config['market_settings']['dynamic_scan']['enabled'] and not cached_top_symbols:
        logger.info("缓存为空，首次获取热门币种列表...")
        _update_cache(exchange, config)  # 使用修正后的本地函数
        if not cached_top_symbols:
            logger.error("首次获取热门币种列表失败，无法执行监控。")
            return

    symbols_to_check = cached_top_symbols if config['market_settings']['dynamic_scan']['enabled'] else \
        config['market_settings'].get('static_symbols', [])

    if not symbols_to_check:
        logger.warning("没有需要监控的交易对。")
        return

    logger.info(f"本轮将使用 {len(symbols_to_check)} 个交易对进行并发扫描...")

    max_workers = config.get('app_settings', {}).get('max_workers', 10)
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='Monitor') as executor:
        futures = {
            executor.submit(_check_symbol_all_strategies, symbol, exchange, config): symbol
            for symbol in symbols_to_check
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                logger.debug(f"任务完成: {result}")
            except Exception as e:
                logger.error(f"任务 {symbol} 在执行中发生严重错误: {e}", exc_info=True)

    logger.info("✅ 动态热点监控循环完成")
# --- END OF FILE app/tasks/signal_scanner.py (FIXED) ---
