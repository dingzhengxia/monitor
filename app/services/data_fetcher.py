# --- START OF FILE app/services/data_fetcher.py (WITH IGNORE_FILTERS FLAG) ---
import time
import pandas as pd
from loguru import logger
from collections import defaultdict


def get_top_n_symbols_by_volume(exchange, top_n=100, exclude_list=[], market_type='swap', retries=5, config=None,
                                ignore_adv_filters=False):
    # 步骤 0: 解析配置，并提供安全的默认值
    scan_conf = config.get('market_settings', {}).get('dynamic_scan', {}) if config else {}
    primary_quote = scan_conf.get('primary_quote_currency', 'USDT').upper()

    # 【核心修改】: 根据 ignore_adv_filters 标志决定是否启用高级筛选
    cross_filter_enabled = False
    if not ignore_adv_filters:
        cross_filter_conf = scan_conf.get('cross_market_filter', {})
        cross_filter_enabled = cross_filter_conf.get('enabled', False)

    must_exist_quotes = set([q.upper() for q in scan_conf.get('cross_market_filter', {}).get('must_exist_in', [])])

    logger.info(f"...正在从 {exchange.id} 获取所有交易对的24h行情数据 (目标市场: {market_type})...")
    logger.info(f"主计价货币: {primary_quote}")

    if cross_filter_enabled and not ignore_adv_filters:
        if must_exist_quotes:
            logger.info(f"🎯 跨市场验证已激活。币种必须同时存在于: {primary_quote} AND {', '.join(must_exist_quotes)}")
        else:
            logger.warning("⚠️ 跨市场验证已启用，但 'must_exist_in' 列表为空。将只扫描主市场。")
    else:
        if ignore_adv_filters:
            logger.info(f"常规动态扫描模式 (已忽略高级筛选)。")
        else:
            logger.info(f"常规动态扫描模式。")

    for i in range(retries):
        try:
            tickers = exchange.fetch_tickers()
            logger.info(f"...获取成功，共 {len(tickers)} 个ticker，正在处理...")

            # ... (后续代码与之前版本完全相同) ...

            # 步骤 1: 构建地图
            base_to_quotes_map = defaultdict(set)
            primary_market_tickers = {}

            for symbol_str, ticker in tickers.items():
                if not ticker: continue

                symbol = ticker.get('symbol', symbol_str)
                is_swap = ticker.get('swap', False) or ':' in symbol
                if market_type == 'swap' and not is_swap: continue

                is_spot = ticker.get('spot', False) or '/' in symbol and not is_swap
                if market_type == 'spot' and not is_spot: continue

                base = ticker.get('base', symbol.split('/')[0].split(':')[0]).upper()
                quote = ticker.get('quote', symbol.split(':')[-1] if ':' in symbol else symbol.split('/')[-1]).upper()

                if base in exclude_list:
                    continue

                base_to_quotes_map[base].add(quote)

                if quote == primary_quote:
                    primary_market_tickers[base] = ticker

            # 步骤 2: 筛选动态候选币种
            candidate_bases = set()

            if cross_filter_enabled and must_exist_quotes and not ignore_adv_filters:
                required_quotes_for_check = must_exist_quotes.union({primary_quote})
                for base, existing_quotes in base_to_quotes_map.items():
                    if required_quotes_for_check.issubset(existing_quotes):
                        candidate_bases.add(base)
                logger.info(f"通过跨市场验证的币种有 {len(candidate_bases)} 个。")
            else:
                candidate_bases = set(primary_market_tickers.keys())

            # 步骤 3: 排序并返回动态列表
            dynamic_candidates = []
            for base in candidate_bases:
                if base in primary_market_tickers:
                    ticker = primary_market_tickers[base]
                    if ticker.get('quoteVolume', 0) > 0:
                        dynamic_candidates.append({
                            'symbol': ticker['symbol'],
                            'volume': ticker['quoteVolume']
                        })

            sorted_tickers = sorted(dynamic_candidates, key=lambda x: x['volume'], reverse=True)[:top_n]
            logger.info(f"✅ 成功筛选出 {len(sorted_tickers)} 个动态交易对。")
            return [t['symbol'] for t in sorted_tickers]

        except Exception as e:
            logger.warning(f"获取行情数据失败 (尝试 {i + 1}/{retries}): {e}")
            if i < retries - 1:
                time.sleep(exchange.rateLimit / 1000)
            else:
                logger.error(f"❌ 已达到最大重试次数。");
                return []
    return []


def fetch_ohlcv_data(exchange, symbol, timeframe, limit):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < 50:
            return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        return df
    except Exception as e:
        logger.debug(f"为 {symbol} {timeframe} 获取OHLCV数据失败: {e}")
        return None