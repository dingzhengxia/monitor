import time
from datetime import datetime
import pandas_ta as pta
import numpy as np
from app.services.data_fetcher import get_top_n_symbols_by_volume, fetch_ohlcv_data, fetch_fear_greed_index
from app.services.notification_service import send_alert
from app.state import cached_top_symbols
from loguru import logger


def _get_symbol_in_primary_market(base_symbol, config):
    primary_quote = config.get('market_settings', {}).get('dynamic_scan', {}).get('primary_quote_currency',
                                                                                  'USDT').upper()
    market_type = config.get('app_settings', {}).get('default_market_type', 'swap')

    if market_type == 'swap':
        if primary_quote == "USDT":
            return f"{base_symbol.upper()}/USDT:USDT"
        return f"{base_symbol.upper()}/{primary_quote}:{primary_quote}"
    else:  # spot
        return f"{base_symbol.upper()}/{primary_quote}"


def _update_cache_for_report(exchange, config, report_conf):
    report_name = report_conf.get("report_name", "报告任务")
    logger.info(f" ({report_name})正在更新热门币种缓存...")
    dyn_scan_conf = config.get('market_settings', {}).get('dynamic_scan', {})
    top_n_for_signals = dyn_scan_conf.get('top_n_for_signals', 100)
    top_n_for_report = report_conf.get('top_n_by_volume', 100)
    fetch_n = max(top_n_for_signals, top_n_for_report)

    dynamic_symbols_list = get_top_n_symbols_by_volume(
        exchange,
        top_n=fetch_n,
        exclude_list=[s.upper() for s in dyn_scan_conf.get('exclude_symbols', [])],
        market_type=config.get('app_settings', {}).get('default_market_type', 'swap'),
        config=config,
        ignore_adv_filters=True
    )

    static_bases = config.get('market_settings', {}).get('static_symbols', [])
    static_symbols_list = [_get_symbol_in_primary_market(base, config) for base in static_bases]

    final_list = list(dynamic_symbols_list)
    for s in static_symbols_list:
        if s not in final_list:
            final_list.append(s)

    cached_top_symbols.clear()
    cached_top_symbols.extend(final_list)
    logger.info(f"✅ ({report_name})热门币种缓存已更新，当前共监控 {len(cached_top_symbols)} 个交易对。")


def run_periodic_report(exchange, config, report_conf):
    report_name = report_conf.get("report_name", "周期报告")
    logger.info(f"--- 📊 开始执行 '{report_name}' ---")
    try:
        sentiment_conf = report_conf.get('market_sentiment', {})

        _update_cache_for_report(exchange, config, report_conf)
        if not cached_top_symbols:
            logger.warning(f"'{report_name}' 中止：热门币种缓存为空。")
            return

        fear_greed_data = None
        if sentiment_conf.get('enabled', False):
            fear_greed_data = fetch_fear_greed_index()

        report_tf = report_conf.get('run_interval', '4h')
        symbols_to_scan = cached_top_symbols[:report_conf.get('top_n_by_volume', 100)]

        gainers_list, consecutive_up_list, volume_ratio_list = [], [], []
        overbought_list, oversold_list = [], []

        logger.info(f"...正在基于 {len(symbols_to_scan)} 个热门合约和 {report_tf} 周期生成 '{report_name}'...")

        required_len = max(200, sentiment_conf.get('rsi_period', 14) + 50)

        for i, symbol in enumerate(symbols_to_scan):
            try:
                df = fetch_ohlcv_data(exchange, symbol, report_tf, limit=required_len)
                if df is None or len(df) < report_conf.get('volume_ma_period', 20) + 2:
                    continue

                last_closed_candle = df.iloc[-2]
                if last_closed_candle['open'] > 0:
                    gainers_list.append({'symbol': symbol,
                                         'gain': ((last_closed_candle['close'] - last_closed_candle['open']) /
                                                  last_closed_candle['open']) * 100})

                count = 0
                for j in range(2, len(df) + 1):
                    candle = df.iloc[-j]
                    if candle['close'] > candle['open']:
                        count += 1
                    else:
                        break
                if count >= report_conf.get('min_consecutive_candles', 2):
                    consecutive_up_list.append({'symbol': symbol, 'candles': count})

                df['volume_ma'] = df['volume'].rolling(window=report_conf.get('volume_ma_period', 20)).mean().shift(1)
                vol_ma = df.iloc[-2]['volume_ma']
                if vol_ma and vol_ma > 0:
                    volume_ratio_list.append(
                        {'symbol': symbol, 'ratio': last_closed_candle['volume'] / vol_ma,
                         'volume': last_closed_candle['volume'],
                         'volume_ma': vol_ma})

                if sentiment_conf.get('enabled', False) and i < 10:
                    if len(df) >= sentiment_conf.get('rsi_period', 14) + 1:
                        df['rsi'] = pta.rsi(df['close'], length=sentiment_conf.get('rsi_period', 14))
                        last_rsi = df['rsi'].iloc[-2]

                        if last_rsi is not None and not np.isnan(last_rsi):
                            overbought_threshold = sentiment_conf.get('rsi_overbought', 70)
                            oversold_threshold = sentiment_conf.get('rsi_oversold', 30)

                            if overbought_threshold < last_rsi < 100:
                                overbought_list.append({'symbol': symbol, 'rsi': last_rsi})
                            elif 0 < last_rsi < oversold_threshold:
                                oversold_list.append({'symbol': symbol, 'rsi': last_rsi})

                time.sleep(exchange.rateLimit / 2000)
            except Exception as e:
                logger.debug(f"扫描 {symbol} 报告时出错: {e}")
                continue

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        title = f"📰 {report_name} ({now_str}, {report_tf}周期)"
        message = ""

        if fear_greed_data:
            fng_emoji = "😱" if fear_greed_data['classification'] in ["Fear", "Extreme Fear"] else "🤑"
            message += f"### {fng_emoji} 市场情绪: {fear_greed_data['classification']} ({fear_greed_data['value']}/100)\n\n---\n\n"

        if gainers_list:
            sorted_gainers = sorted(gainers_list, key=lambda x: x['gain'], reverse=True)[
                             :report_conf.get('top_n_gainers', 10)]
            message += f"### 🚀 {report_tf} 周期涨幅榜\n\n"
            for i, item in enumerate(sorted_gainers):
                message += f"{'🥇🥈🥉🔥'[i if i < 4 else 3]} **{item['symbol']}**\n> **涨幅: {item['gain']:.2f}%**\n\n"

        if volume_ratio_list:
            sorted_ratios = sorted(volume_ratio_list, key=lambda x: x['ratio'], reverse=True)[
                            :report_conf.get('top_n_volume_ratio', 10)]
            message += f"\n---\n\n### 📈 {report_tf} 周期成交量异动\n\n"
            for i, item in enumerate(sorted_ratios):
                message += (f"{'🥇🥈🥉⚡️'[i if i < 4 else 3]} **{item['symbol']}**\n"
                            f"> **放量倍数: {item['ratio']:.2f} 倍**\n"
                            f"> (周期量: {item['volume']:.0f}, 均量: {item['volume_ma']:.0f})\n\n")

        if overbought_list:
            message += f"\n---\n\n### 🥵 {report_tf} 周期热门区超买合约\n\n"
            for item in overbought_list:
                message += f"🥵 **{item['symbol']}**\n> **RSI: {item['rsi']:.1f}**\n\n"

        if oversold_list:
            message += f"\n---\n\n### 🥶 {report_tf} 周期热门区超卖合约\n\n"
            for item in oversold_list:
                message += f"🥶 **{item['symbol']}**\n> **RSI: {item['rsi']:.1f}**\n\n"

        final_consecutive_list = sorted(consecutive_up_list, key=lambda x: x['candles'], reverse=True)
        if final_consecutive_list:
            message += f"\n---\n\n### 💪 {report_tf} 周期连涨强势合约\n\n"
            for item in final_consecutive_list:
                message += f"💪 **{item['symbol']}**\n> **连涨: {item['candles']} 根** {'🔥' * (item['candles'] // 2) if item['candles'] > 3 else '🔥' if item['candles'] == 3 else ''}\n\n"

        if message:
            send_alert(config, title, message, "Market Report")

        logger.info(f"--- ✅ '{report_name}' 完成 ---")
    except Exception as e:
        logger.error(f"❌ 执行 '{report_name}' 时发生严重错误: {e}", exc_info=True)