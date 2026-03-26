# --- START OF FILE app/tasks/signal_scanner.py ---
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

from app.analysis.strategies import (
    check_ema_signals, check_kdj_cross, check_volatility_breakout,
    check_level_breakout, check_rsi_divergence, check_consecutive_candles,
    check_trend_channel_breakout,
    check_order_block_interaction,
    check_high_funding_rate,
    check_ma_breakout,        # <-- 注册新的 MA策略
    _get_params_for_timeframe
)
from app.services.data_fetcher import fetch_ohlcv_data, get_top_n_symbols_by_volume
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
    logger.info(" (主扫描任务)正在更新热门币种缓存(K线分析用)...")
    dyn_scan_conf = config.get('market_settings', {}).get('dynamic_scan', {})

    top_n = dyn_scan_conf.get('top_n_for_signals', 100)

    dynamic_symbols_list = get_top_n_symbols_by_volume(
        exchange,
        top_n=top_n,
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
    logger.info(f"✅ 主缓存更新完毕，共监控 {len(cached_top_symbols)} 个交易对。")


# --- 策略映射表 ---
STRATEGY_MAP = {
    'ema_cross': {'func': check_ema_signals, 'limit': 170},
    'ma_breakout': {'func': check_ma_breakout, 'limit': 150},
    'kdj_cross': {'func': check_kdj_cross, 'limit': 170},
    'volatility_breakout': {'func': check_volatility_breakout, 'limit': 170},
    # V--- 【核心修改】把 limit 加大到 400，让日线能拉取足够多的K线寻找前高前低 ---V
    'level_breakout': {'func': check_level_breakout, 'limit': 400},
    'rsi_divergence': {'func': check_rsi_divergence, 'limit': 170},
    'trend_channel_breakout': {'func': check_trend_channel_breakout, 'limit': 350},
    'consecutive_candles': {'func': check_consecutive_candles, 'limit': 50},
    'order_block_interaction': {'func': check_order_block_interaction, 'limit': 250},
}


def _check_symbol_all_strategies(symbol, exchange, config):
    global_timeframes = config.get('market_settings', {}).get('timeframes', ['1h', '4h'])

    for timeframe in global_timeframes:
        max_limit = max(s['limit'] for s in STRATEGY_MAP.values())
        df = fetch_ohlcv_data(exchange, symbol, timeframe, max_limit)
        if df is None:
            continue

        for name, strategy_info in STRATEGY_MAP.items():
            raw_params_config = config['strategy_params'].get(name, {})
            if not isinstance(raw_params_config, list):
                param_sets = [raw_params_config]
            else:
                param_sets = raw_params_config

            for i, base_params in enumerate(param_sets):
                if not base_params.get('enabled', False):
                    continue
                final_params = _get_params_for_timeframe(base_params, timeframe)
                if timeframe in final_params.get('exclude_timeframes', []):
                    continue
                try:
                    strategy_info['func'](exchange, symbol, timeframe, config, df.copy(), final_params, i)
                except Exception as e:
                    logger.error(f"执行策略 {name} on {symbol} {timeframe} 时发生错误: {e}")
    return symbol


def _run_broad_funding_scan(exchange, config):
    fund_conf = config.get('strategy_params', {}).get('high_funding_rate', {})
    if not fund_conf.get('enabled', False):
        return

    scan_limit = fund_conf.get('scan_top_n', 500)
    logger.info(f"💰 开始执行大范围资金费率监控 (Top {scan_limit})...")

    dyn_scan_conf = config.get('market_settings', {}).get('dynamic_scan', {})
    broad_symbols = get_top_n_symbols_by_volume(
        exchange,
        top_n=scan_limit,
        exclude_list=[s.upper() for s in dyn_scan_conf.get('exclude_symbols', [])],
        market_type=config.get('app_settings', {}).get('default_market_type', 'swap'),
        config=config,
        ignore_adv_filters=True
    )

    if not broad_symbols:
        logger.warning("无法获取资金费率扫描列表")
        return

    logger.info(f"   - 获取到 {len(broad_symbols)} 个交易对，正在检查费率...")

    def check_funding_task(sym):
        try:
            check_high_funding_rate(exchange, sym, '4h', config, None, fund_conf)
        except Exception as e:
            logger.error(f"检查费率 {sym} 出错: {e}")

    with ThreadPoolExecutor(max_workers=20, thread_name_prefix='FundScan') as executor:
        futures = {executor.submit(check_funding_task, sym): sym for sym in broad_symbols}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                pass

    logger.info("✅ 资金费率大范围扫描完成。")


def run_signal_check_cycle(exchange, config):
    logger.info("=" * 60)
    logger.info(f"🔄 开始执行监控循环...")

    try:
        _run_broad_funding_scan(exchange, config)
    except Exception as e:
        logger.error(f"资金费率扫描任务失败: {e}", exc_info=True)

    dyn_scan_enabled = config.get('market_settings', {}).get('dynamic_scan', {}).get('enabled', False)
    if dyn_scan_enabled:
        _update_cache(exchange, config)
    else:
        static_bases = config.get('market_settings', {}).get('static_symbols', [])
        static_symbols_list = [_get_symbol_in_primary_market(base, config) for base in static_bases]
        cached_top_symbols.clear()
        cached_top_symbols.extend(static_symbols_list)

    if not cached_top_symbols:
        logger.warning("没有需要监控 K 线的交易对。")
        return

    logger.info(f"📊 开始 K 线技术分析扫描 (Top {len(cached_top_symbols)})...")
    max_workers = config.get('app_settings', {}).get('max_workers', 10)

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='TechScan') as executor:
        futures = {executor.submit(_check_symbol_all_strategies, symbol, exchange, config): symbol for symbol in
                   cached_top_symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"K线分析任务 {symbol} 出错: {e}")

    logger.info("✅ 全流程扫描完成")
# --- END OF FILE app/tasks/signal_scanner.py ---