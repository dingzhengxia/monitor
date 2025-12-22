# --- START OF FILE app/tasks/signal_scanner.py (SPLIT SCAN VERSION) ---
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

from app.analysis.strategies import (
    check_ema_signals, check_kdj_cross, check_volatility_breakout,
    check_level_breakout, check_rsi_divergence, check_consecutive_candles,
    check_trend_channel_breakout,
    check_order_block_interaction,
    check_high_funding_rate,  # 依然导入，但在独立循环中使用
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
    """ 更新主策略用的 Top N (通常是100) """
    logger.info(" (主扫描任务)正在更新热门币种缓存(K线分析用)...")
    dyn_scan_conf = config.get('market_settings', {}).get('dynamic_scan', {})

    # 主策略的扫描范围 (例如 100)
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
# 注意：这里【不】包含 high_funding_rate，因为它现在是独立运行的
STRATEGY_MAP = {
    'ema_cross': {'func': check_ema_signals, 'limit': 170},
    'kdj_cross': {'func': check_kdj_cross, 'limit': 170},
    'volatility_breakout': {'func': check_volatility_breakout, 'limit': 170},
    'level_breakout': {'func': check_level_breakout, 'limit': 200},
    'rsi_divergence': {'func': check_rsi_divergence, 'limit': 170},
    'trend_channel_breakout': {'func': check_trend_channel_breakout, 'limit': 350},
    'consecutive_candles': {'func': check_consecutive_candles, 'limit': 50},
    'order_block_interaction': {'func': check_order_block_interaction, 'limit': 250},
}


def _check_symbol_all_strategies(symbol, exchange, config):
    """ 执行所有依赖 K 线数据的策略 """
    # logger.debug(f"--- [Thread] 正在检查 K线策略: {symbol} ---")
    global_timeframes = config.get('market_settings', {}).get('timeframes', ['1h', '4h'])

    for timeframe in global_timeframes:
        max_limit = max(s['limit'] for s in STRATEGY_MAP.values())
        # 这里是耗时操作：下载K线
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
    """
    【新增】专门用于大范围扫描资金费率的函数
    不需要下载 K 线，所以速度很快，可以扫描 Top 500
    """
    fund_conf = config.get('strategy_params', {}).get('high_funding_rate', {})
    if not fund_conf.get('enabled', False):
        return

    # 获取扫描范围，默认500
    scan_limit = fund_conf.get('scan_top_n', 500)
    logger.info(f"💰 开始执行大范围资金费率监控 (Top {scan_limit})...")

    # 1. 获取 Top N 列表 (这里单独获取，不影响主缓存)
    dyn_scan_conf = config.get('market_settings', {}).get('dynamic_scan', {})
    broad_symbols = get_top_n_symbols_by_volume(
        exchange,
        top_n=scan_limit,
        exclude_list=[s.upper() for s in dyn_scan_conf.get('exclude_symbols', [])],
        market_type=config.get('app_settings', {}).get('default_market_type', 'swap'),
        config=config,
        ignore_adv_filters=True  # 费率监控可以放宽一点条件
    )

    if not broad_symbols:
        logger.warning("无法获取资金费率扫描列表")
        return

    logger.info(f"   - 获取到 {len(broad_symbols)} 个交易对，正在检查费率...")

    # 2. 定义具体的检查任务
    def check_funding_task(sym):
        # 资金费率是全局的，不需要真实的K线df，传 None 即可
        # timeframe 也可以给个默认的 '4h'，主要用于冷却计算的基准
        try:
            check_high_funding_rate(exchange, sym, '4h', config, None, fund_conf)
        except Exception as e:
            logger.error(f"检查费率 {sym} 出错: {e}")

    # 3. 并发执行
    # 不需要太多线程，因为 check_high_funding_rate 内部只是简单API请求
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

    # --- 任务 1: 资金费率大范围扫描 (Top 500) ---
    # 我们先跑这个，因为它快
    try:
        _run_broad_funding_scan(exchange, config)
    except Exception as e:
        logger.error(f"资金费率扫描任务失败: {e}", exc_info=True)

    # --- 任务 2: 传统 K线技术分析扫描 (Top 100) ---
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
# --- END OF FILE app/tasks/signal_scanner.py (SPLIT SCAN VERSION) ---