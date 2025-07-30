# --- START OF FILE app/tasks/signal_scanner.py (FINAL SIMPLIFIED LOGIC) ---
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
# 本地应用导入
from app.state import cached_top_symbols
from app.services.data_fetcher import fetch_ohlcv_data, get_top_n_symbols_by_volume
from app.analysis.strategies import (
    check_ema_signals, check_kdj_cross, check_volatility_breakout,
    check_volume_breakout, check_rsi_divergence, check_consecutive_candles
)


def _get_symbol_in_primary_market(base_symbol, config):
    """辅助函数，将基础货币转换为带计价货币的完整交易对名称"""
    primary_quote = config.get('market_settings', {}).get('dynamic_scan', {}).get('primary_quote_currency',
                                                                                  'USDT').upper()
    market_type = config.get('app_settings', {}).get('default_market_type', 'swap')

    if market_type == 'swap':
        # 币安永续合约通常格式为 BASE/USDT:USDT
        # 兼容两种格式，一种是带USDT后缀，一种不带
        if primary_quote == "USDT":
            return f"{base_symbol.upper()}/USDT:USDT"
        # 其他计价货币，如BUSD
        return f"{base_symbol.upper()}/{primary_quote}:{primary_quote}"
    else:  # spot
        return f"{base_symbol.upper()}/{primary_quote}"


def _update_cache(exchange, config):
    logger.info(" (扫描任务)正在更新热门币种缓存...")

    # 步骤 1: 获取动态扫描列表
    dyn_scan_conf = config.get('market_settings', {}).get('dynamic_scan', {})
    dynamic_symbols_list = get_top_n_symbols_by_volume(
        exchange,
        top_n=dyn_scan_conf.get('top_n_for_signals', 100),
        exclude_list=[s.upper() for s in dyn_scan_conf.get('exclude_symbols', [])],
        market_type=config.get('app_settings', {}).get('default_market_type', 'swap'),
        config=config
    )

    # 步骤 2: 获取白名单 (static_symbols) 并转换为完整交易对名称
    static_bases = config.get('market_settings', {}).get('static_symbols', [])
    static_symbols_list = [_get_symbol_in_primary_market(base, config) for base in static_bases]

    # 步骤 3: 合并列表，白名单享有豁免权
    # 将动态列表（已按成交量排序）放在前面
    final_list = list(dynamic_symbols_list)
    # 将不在动态列表中的白名单项追加到末尾
    for s in static_symbols_list:
        if s not in final_list:
            final_list.append(s)

    cached_top_symbols.clear()
    cached_top_symbols.extend(final_list)

    logger.info(f"✅ 热门币种缓存已更新，当前共监控 {len(cached_top_symbols)} 个交易对。")


STRATEGY_MAP = {
    'ema_cross': {'func': check_ema_signals, 'limit': 170},
    'kdj_cross': {'func': check_kdj_cross, 'limit': 170},
    'volatility_breakout': {'func': check_volatility_breakout, 'limit': 170},
    'volume_breakout': {'func': check_volume_breakout, 'limit': 200},
    'rsi_divergence': {'func': check_rsi_divergence, 'limit': 170},
    'consecutive_candles': {'func': check_consecutive_candles, 'limit': 50},
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

    # 根据配置构建最终的监控列表
    dyn_scan_enabled = config.get('market_settings', {}).get('dynamic_scan', {}).get('enabled', False)

    if dyn_scan_enabled:
        # 动态扫描开启：获取动态列表 + 合并静态白名单
        _update_cache(exchange, config)
    else:
        # 动态扫描关闭：只使用静态白名单
        static_bases = config.get('market_settings', {}).get('static_symbols', [])
        static_symbols_list = [_get_symbol_in_primary_market(base, config) for base in static_bases]
        cached_top_symbols.clear()
        cached_top_symbols.extend(static_symbols_list)
        logger.info(f"动态扫描已关闭。仅监控 {len(cached_top_symbols)} 个静态交易对。")

    if not cached_top_symbols:
        logger.warning("没有需要监控的交易对。")
        return

    logger.info(f"本轮将使用 {len(cached_top_symbols)} 个交易对进行并发扫描...")

    max_workers = config.get('app_settings', {}).get('max_workers', 10)
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='Monitor') as executor:
        futures = {
            executor.submit(_check_symbol_all_strategies, symbol, exchange, config): symbol
            for symbol in cached_top_symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                logger.debug(f"任务完成: {result}")
            except Exception as e:
                logger.error(f"任务 {symbol} 在执行中发生严重错误: {e}", exc_info=True)

    logger.info("✅ 动态热点监控循环完成")