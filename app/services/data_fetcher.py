# --- START OF FILE app/services/data_fetcher.py ---
import logging
import time
import pandas as pd

logger = logging.getLogger(__name__)

def get_top_n_symbols_by_volume(exchange, top_n=100, exclude_list=[], market_type='swap', retries=5):
    logger.info(f"...正在从 {exchange.id} 获取所有交易对的24h行情数据 (目标市场: {market_type})...")
    for i in range(retries):
        try:
            tickers = exchange.fetch_tickers()
            logger.info(f"...获取成功，共 {len(tickers)} 个ticker，正在筛选...")
            usdt_tickers = []
            for symbol_str, ticker in tickers.items():
                if not ticker or ticker.get('quoteVolume', 0) == 0: continue
                symbol = ticker.get('symbol', symbol_str); is_swap = ticker.get('swap', False) or ':' in symbol
                if market_type == 'swap' and not is_swap: continue
                is_spot = ticker.get('spot', False) or '/' in symbol and not is_swap
                if market_type == 'spot' and not is_spot: continue
                quote = ticker.get('quote', '').upper()
                if not (quote == 'USDT' or (is_swap and symbol.endswith(':USDT'))): continue
                base = ticker.get('base', symbol.split('/')[0].split(':')[0])
                if base.upper() in exclude_list: continue
                usdt_tickers.append({'symbol': symbol, 'volume': ticker['quoteVolume']})
            sorted_tickers = sorted(usdt_tickers, key=lambda x: x['volume'], reverse=True)[:top_n]
            logger.info(f"✅ 成功筛选出成交额排名前 {len(sorted_tickers)} 的 {market_type.upper()} USDT 交易对。")
            return [t['symbol'] for t in sorted_tickers]
        except Exception as e:
            logger.warning(f"获取行情数据失败 (尝试 {i + 1}/{retries}): {e}")
            if i < retries - 1: time.sleep(exchange.rateLimit / 1000)
            else: logger.error(f"❌ 已达到最大重试次数。"); return []
    return []

def fetch_ohlcv_data(exchange, symbol, timeframe, limit):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < 50: # 基本的数据有效性检查
            return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        return df
    except Exception as e:
        logger.debug(f"为 {symbol} {timeframe} 获取OHLCV数据失败: {e}")
        return None
# --- END OF FILE app/services/data_fetcher.py ---