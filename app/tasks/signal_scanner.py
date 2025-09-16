# --- START OF FILE app/tasks/signal_scanner.py (ARRAY-AWARE) ---
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger

from app.analysis.strategies import (
    check_ema_signals, check_kdj_cross, check_volatility_breakout,
    check_level_breakout, check_rsi_divergence, check_consecutive_candles,
    check_trend_channel_breakout,
    check_order_block_interaction,  # <-- 新增导入
    _get_params_for_timeframe
)
from app.services.data_fetcher import fetch_ohlcv_data, get_top_n_symbols_by_volume
# 本地应用导入
from app.state import cached_top_symbols


def _get_symbol_in_primary_market(base_symbol, config):
    primary_quote = config.get('market_settings', {}).get('dynamic_scan', {}).get('primary_quote_currency',
                                                                                  'USDT').upper()
    market_type = config.get('app_settings', {}).get('default_market_type', 'swap')
    if market_type == 'swap':
        if primary_quote == "USDT": return f"{base_symbol.upper()}/USDT:USDT"
        return f"{base_symbol.upper()}/{primary_quote}:{primary_quote}"
    else:
        return f"{base_symbol.upper()}/{primary_quote}"


def _update_cache(exchange, config):
    logger.info(" (扫描任务)正在更新热门币种缓存...")
    dyn_scan_conf = config.get('market_settings', {}).get('dynamic_scan', {})
    dynamic_symbols_list = get_top_n_symbols_by_volume(
        exchange,
        top_n=dyn_scan_conf.get('top_n_for_signals', 100),
        exclude_list=[s.upper() for s in dyn_scan_conf.get('exclude_symbols', [])],
        market_type=config.get('app_settings', {}).get('default_market_type', 'swap'),
        config=config
    )
    static_bases = config.get('market_settings', {}).get('static_symbols', [])
    static_symbols_list = [_get_symbol_in_primary_market(base, config) for base in static_bases]
    final_list = list(dynamic_symbols_list)
    for s in static_symbols_list:
        if s not in final_list: final_list.append(s)
    cached_top_symbols.clear()
    cached_top_symbols.extend(final_list)
    logger.info(f"✅ 热门币种缓存已更新，当前共监控 {len(cached_top_symbols)} 个交易对。")


STRATEGY_MAP = {
    'ema_cross': {'func': check_ema_signals, 'limit': 170},
    'kdj_cross': {'func': check_kdj_cross, 'limit': 170},
    'volatility_breakout': {'func': check_volatility_breakout, 'limit': 170},
    'level_breakout': {'func': check_level_breakout, 'limit': 200},
    'rsi_divergence': {'func': check_rsi_divergence, 'limit': 170},
    'trend_channel_breakout': {'func': check_trend_channel_breakout, 'limit': 350},  # 增加limit以适应更大的lookback
    'consecutive_candles': {'func': check_consecutive_candles, 'limit': 50},
    # V-- 新增策略 --V
    'order_block_interaction': {'func': check_order_block_interaction, 'limit': 250},  # 需要足够的回看周期
    # ^-- 新增策略 --^
}


def _check_symbol_all_strategies(symbol, exchange, config):
    logger.debug(f"--- [Thread] 正在检查: {symbol} ---")
    global_timeframes = config.get('market_settings', {}).get('timeframes', ['1h', '4h'])

    for timeframe in global_timeframes:
        max_limit = max(s['limit'] for s in STRATEGY_MAP.values())
        df = fetch_ohlcv_data(exchange, symbol, timeframe, max_limit)
        if df is None:
            logger.debug(f"无法获取 {symbol} {timeframe} 的数据，跳过本轮检查。")
            continue

        for name, strategy_info in STRATEGY_MAP.items():
            # 获取策略的原始配置，它可能是一个字典或一个列表
            raw_params_config = config['strategy_params'].get(name, {})

            # 【核心升级】将所有配置统一处理为列表，方便循环
            if not isinstance(raw_params_config, list):
                # 如果不是列表，就把它变成一个只包含它自己的列表
                param_sets = [raw_params_config]
            else:
                param_sets = raw_params_config

            # 循环遍历这个策略的所有配置集 (对于普通策略，这个循环只会执行一次)
            for i, base_params in enumerate(param_sets):
                # 如果这个配置集本身被禁用了，则跳过
                if not base_params.get('enabled', False):
                    continue

                # 获取针对当前时间周期的最终参数
                final_params = _get_params_for_timeframe(base_params, timeframe)

                # 检查当前时间周期是否在该配置集的排除列表中
                if timeframe in final_params.get('exclude_timeframes', []):
                    # 如果策略有名字，就在日志中显示，方便调试
                    strategy_display_name = f"{name} ({final_params.get('name', i)})"
                    logger.trace(f"策略 {strategy_display_name} 已配置为在 {timeframe} 周期上跳过。")
                    continue

                try:
                    # 将最终参数和配置索引传递给策略函数
                    strategy_info['func'](exchange, symbol, timeframe, config, df.copy(), final_params, i)
                except Exception as e:
                    logger.error(f"执行策略 {name} on {symbol} {timeframe} 时发生顶层错误: {e}", exc_info=True)

    return f"已完成 {symbol} 的检查"


def run_signal_check_cycle(exchange, config):
    logger.info("=" * 60)
    logger.info(f"🔄 开始执行动态热点监控循环...")
    dyn_scan_enabled = config.get('market_settings', {}).get('dynamic_scan', {}).get('enabled', False)

    if dyn_scan_enabled:
        _update_cache(exchange, config)
    else:
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
        futures = {executor.submit(_check_symbol_all_strategies, symbol, exchange, config): symbol for symbol in
                   cached_top_symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                logger.debug(f"任务完成: {result}")
            except Exception as e:
                logger.error(f"任务 {symbol} 在执行中发生严重错误: {e}", exc_info=True)
    logger.info("✅ 动态热点监控循环完成")
# --- END OF FILE app/tasks/signal_scanner.py (ARRAY-AWARE) ---