# ==============================================================================
# 终极监控与信号程序 (V34.1 - 启动流程与日志优化版)
# ==============================================================================
import base64
import hashlib
import hmac
import json
import logging
import math
import signal
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler

import ccxt
import pandas as pd
import pandas_ta as pta
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from plyer import notification


# ==============================================================================
# 1. 初始化 & 全局配置 (无变化)
# ==============================================================================
def setup_logging(level="INFO"):
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    if logger.hasHandlers(): logger.handlers.clear()
    console_handler = logging.StreamHandler()
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-m-%d %H:%M:%S')
    console_handler.setFormatter(console_format)
    file_handler = TimedRotatingFileHandler(filename='monitor.log', when='midnight', interval=1, backupCount=7,
                                            encoding='utf-8')
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s',
                                    datefmt='%Y-m-%d %H:%M:%S')
    file_handler.setFormatter(file_format)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
    return logger


logger = None
ALERT_STATUS_FILE = 'cooldown_status.json'
alerted_states = {}
cached_top_symbols = []


# ==============================================================================
# 2. 核心工具函数 (无变化)
# ==============================================================================
def load_alert_states():
    global alerted_states
    try:
        with open(ALERT_STATUS_FILE, 'r') as f:
            data = json.load(f)
        alerted_states = {k: datetime.fromisoformat(v).replace(tzinfo=timezone.utc) if datetime.fromisoformat(
            v).tzinfo is None else datetime.fromisoformat(v) for k, v in data.items()}
        now_utc = datetime.now(timezone.utc)
        initial_count = len(alerted_states)
        alerted_states = {k: v for k, v in alerted_states.items() if v > now_utc}
        logger.info(f"✅ 成功加载冷却状态。有效条目: {len(alerted_states)} (从 {initial_count} 个中加载)")
    except (FileNotFoundError, json.JSONDecodeError):
        logger.info("ℹ️ 未找到或无法解析冷却状态文件。");
        alerted_states = {}


def save_alert_states():
    global alerted_states
    try:
        now_utc = datetime.now(timezone.utc)
        active_states = {k: v for k, v in alerted_states.items() if v > now_utc}
        alerted_states = active_states
        with open(ALERT_STATUS_FILE, 'w') as f:
            json.dump({k: v.isoformat() for k, v in active_states.items()}, f, indent=4)
    except Exception as e:
        logger.error(f"❌ 保存冷却状态到文件时出错: {e}", exc_info=True)


def timeframe_to_minutes(tf_str):
    try:
        if not tf_str or len(tf_str) < 2: return 0
        num = int(tf_str[:-1]);
        unit = tf_str[-1].lower()
        if unit == 'm': return num
        if unit == 'h': return num * 60
        if unit == 'd': return num * 24 * 60
        if unit == 'w': return num * 7 * 24 * 60
        return 0
    except (ValueError, TypeError):
        return 0


def calculate_cooldown_time(minutes):
    if minutes <= 0: return datetime.now(timezone.utc) + timedelta(minutes=1)
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def get_current_trend(df, timeframe, trend_params_config):
    df_trend = df.copy();
    tf_minutes = timeframe_to_minutes(timeframe)
    trend_params = trend_params_config.get('trend_ema_short' if tf_minutes <= 60 else 'trend_ema_long',
                                           trend_params_config.get('trend_ema', {}))
    if 'fast' not in trend_params and 'medium' not in trend_params and 'long_period' in trend_params:
        ema_long_period = trend_params['long_period']
        df_trend[f'ema_long'] = pta.ema(df_trend['close'], length=ema_long_period)
        df_trend = df_trend.dropna()
        if df_trend.empty: return "趋势未知", "↔️"
        last = df_trend.iloc[-1]
        if len(df_trend) >= 2:
            prev_ema_long = df_trend.iloc[-2][f'ema_long']
            if last[f'ema_long'] > prev_ema_long * 1.0005:
                return "多头趋势", "🐂"
            elif last[f'ema_long'] < prev_ema_long * 0.9995:
                return "空头趋势", "🐻"
        return "震荡趋势", "↔️"
    emas = {'fast': trend_params.get('fast'), 'medium': trend_params.get('medium'), 'long': trend_params.get('long')}
    if not all(period and period > 0 for period in emas.values()):
        logger.debug(f"趋势EMA参数配置不完整或周期不合法: {trend_params}");
        return "趋势未知", "↔️"
    for name, period in emas.items(): df_trend[f'ema_{name}'] = pta.ema(df_trend['close'], length=period)
    required_cols = [f'ema_{name}' for name in emas.keys()]
    if not all(c in df_trend.columns for c in required_cols): return "趋势未知", "↔️"
    df_trend = df_trend.dropna();
    if df_trend.empty: return "趋势未知", "↔️"
    last = df_trend.iloc[-1]
    if last['ema_fast'] > last['ema_medium'] and last['ema_medium'] > last['ema_long'] and last['ema_fast'] > last[
        'ema_long']: return "多头趋势", "🐂"
    if last['ema_fast'] < last['ema_medium'] and last['ema_medium'] < last['ema_long'] and last['ema_fast'] < last[
        'ema_long']: return "空头趋势", "🐻"
    return "震荡趋势", "↔️"


def _calculate_dynamic_multiplier(symbol, config, conf_key, fallback_multiplier, min_default, max_default):
    dyn_conf = config['strategy_params'].get(conf_key, {})
    if not dyn_conf.get('enabled', False):
        return fallback_multiplier
    global cached_top_symbols
    try:
        rank = cached_top_symbols.index(symbol) + 1
    except (ValueError, TypeError):
        return dyn_conf.get('max_multiplier', fallback_multiplier)
    method = dyn_conf.get('method')
    min_mult = dyn_conf.get('min_multiplier', min_default)
    max_mult = dyn_conf.get('max_multiplier', max_default)
    total_ranks = dyn_conf.get('apply_to_rank_n', 100)
    if method == 'linear':
        if total_ranks <= 1: return min_mult
        slope = (max_mult - min_mult) / (total_ranks - 1)
        multiplier = min_mult + (rank - 1) * slope
        return max(min_mult, min(multiplier, max_mult))
    elif method == 'stepped':
        step_size = dyn_conf.get('rank_step_size', 10)
        if step_size <= 0: return fallback_multiplier
        num_steps = math.ceil(total_ranks / step_size)
        if num_steps <= 1: return min_mult
        increment_per_step = (max_mult - min_mult) / (num_steps - 1)
        current_step_index = math.floor((rank - 1) / step_size)
        multiplier = min_mult + current_step_index * increment_per_step
        return max(min_mult, min(multiplier, max_mult))
    return fallback_multiplier


def get_dynamic_volume_multiplier(symbol, config, fallback_multiplier):
    return _calculate_dynamic_multiplier(symbol, config, 'dynamic_volume_multipliers', fallback_multiplier, 2.5, 10.0)


def get_dynamic_atr_multiplier(symbol, config, fallback_multiplier):
    return _calculate_dynamic_multiplier(symbol, config, 'dynamic_atr_multipliers', fallback_multiplier, 2.5, 5.0)


def is_realtime_volume_over(df, tf_minutes, volume_ma_period, multiplier):
    df_vol = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df_vol['timestamp']): df_vol['timestamp'] = pd.to_datetime(
        df_vol['timestamp'], unit='ms', utc=True)
    if len(df_vol) < volume_ma_period + 1: return False, "", 0.0
    df_vol['volume_ma'] = df_vol['volume'].rolling(volume_ma_period).mean().shift(1)
    df_vol = df_vol.dropna().reset_index(drop=True)
    if len(df_vol) < 1: return False, "", 0.0
    current = df_vol.iloc[-1];
    now_utc = datetime.now(timezone.utc);
    start_time = current['timestamp']
    if start_time.tzinfo is None: start_time = start_time.tz_localize('UTC')
    minutes_elapsed = (now_utc - start_time).total_seconds() / 60
    MIN_TIME_RATIO = 0.05;
    time_ratio = max(minutes_elapsed / tf_minutes, MIN_TIME_RATIO) if tf_minutes > 0 else 1.0
    time_ratio = min(time_ratio, 1.0);
    actual_time_progress = minutes_elapsed / tf_minutes if tf_minutes > 0 else 1.0
    dynamic_baseline = current['volume_ma'] * time_ratio
    is_over = current['volume'] > (dynamic_baseline * multiplier)
    actual_ratio = (current['volume'] / dynamic_baseline) if dynamic_baseline > 0 else float('inf')
    text = (f"**成交量分析** (周期进行{actual_time_progress:.0%}):\n"
            f"> **当前量**: {current['volume']:.0f} **(为动态基准的 {actual_ratio:.1f} 倍)**\n"
            f"> **动态基准**: {dynamic_baseline:.0f} (已按时间调整)\n"
            f"> **放量阈值({multiplier:.1f}x)**: {(dynamic_baseline * multiplier):.0f}")
    return is_over, text, actual_ratio


def calculate_pivot_points(daily_ohlc):
    if not isinstance(daily_ohlc, dict) or not all(k in daily_ohlc for k in ['high', 'low', 'close']):
        return [], []
    h, l, c = daily_ohlc['high'], daily_ohlc['low'], daily_ohlc['close']
    p = (h + l + c) / 3
    r1 = 2 * p - l
    s1 = 2 * p - h
    r2 = p + (h - l)
    s2 = p - (h - l)
    r3 = h + 2 * (p - l)
    s3 = l - 2 * (h - p)
    resistances = [{'level': r, 'type': f'R{i + 1}'} for i, r in enumerate([r1, r2, r3])]
    supports = [{'level': s, 'type': f'S{i + 1}'} for i, s in enumerate([s1, s2, s3])]
    return resistances, supports


def find_fractal_levels(df, period=2):
    highs = []
    lows = []
    is_fractal_high = (df['high'] > df['high'].shift(1)) & (df['high'] > df['high'].shift(2)) & \
                      (df['high'] > df['high'].shift(-1)) & (df['high'] > df['high'].shift(-2))
    is_fractal_low = (df['low'] < df['low'].shift(1)) & (df['low'] < df['low'].shift(2)) & \
                     (df['low'] < df['low'].shift(-1)) & (df['low'] < df['low'].shift(-2))
    fractal_high_points = df[is_fractal_high]
    fractal_low_points = df[is_fractal_low]
    for _, row in fractal_high_points.iterrows():
        highs.append({'level': row['high'], 'type': 'Fractal High'})
    for _, row in fractal_low_points.iterrows():
        lows.append({'level': row['low'], 'type': 'Fractal Low'})
    return highs, lows


# ==============================================================================
# 3. 通知模块 (无变化)
# ==============================================================================
def send_desktop_notification(title, message, timeout=10):
    try:
        notification.notify(title=title, message=message, app_name='Crypto Monitor', timeout=timeout); logger.info(
            f"✅ 桌面通知发送成功: {title}")
    except Exception as e:
        logger.error(f"❌ 发送桌面通知时出错: {e}", exc_info=True)


def send_dingtalk_alert(config, title, message, symbol="N/A"):
    notif_conf = config['notification_settings']
    if notif_conf['desktop']['enabled']: timeout = notif_conf['desktop'].get('timeout_seconds',
                                                                             10); send_desktop_notification(title,
                                                                                                            f"交易对: {symbol}",
                                                                                                            timeout=timeout)
    if notif_conf['dingtalk']['enabled']:
        try:
            webhook_url = notif_conf['dingtalk']['webhook_url'];
            secret = notif_conf['dingtalk']['secret']
            if secret:
                timestamp = str(round(time.time() * 1000));
                secret_enc = secret.encode('utf-8');
                string_to_sign = f'{timestamp}\n{secret}';
                hmac_code = hmac.new(secret_enc, string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest();
                sign = urllib.parse.quote_plus(base64.b64encode(hmac_code));
                url_with_sign = f"{webhook_url}&timestamp={timestamp}&sign={sign}"

            else:
                url_with_sign = webhook_url
            payload = {"msgtype": "markdown", "markdown": {"title": title, "text": f"### {title}\n\n{message}"},
                       "at": {"isAtAll": False}}
            response = requests.post(url_with_sign, data=json.dumps(payload),
                                     headers={'Content-Type': 'application/json'}, timeout=15)
            if response.json().get('errcode') == 0:
                logger.info(f"✅ 钉钉警报发送成功: {title}")
            else:
                logger.error(f"❌ 钉钉警报发送失败: {response.json()}")
        except Exception as e:
            logger.error(f"❌ 发送钉钉警报时发生错误: {e}", exc_info=True)


# ==============================================================================
# 4. 数据获取与市场扫描模块 (无变化)
# ==============================================================================
def get_top_n_symbols_by_volume(exchange, top_n=100, exclude_list=[], market_type='swap', retries=5):
    logger.info(f"...正在从 {exchange.id} 获取所有交易对的24h行情数据 (目标市场: {market_type})...")
    for i in range(retries):
        try:
            tickers = exchange.fetch_tickers();
            logger.info(f"...获取成功，共 {len(tickers)} 个ticker，正在筛选...")
            usdt_tickers = []
            for symbol_str, ticker in tickers.items():
                if not ticker or ticker.get('quoteVolume', 0) == 0: continue
                symbol = ticker.get('symbol', symbol_str);
                is_swap = ticker.get('swap', False) or ':' in symbol
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
            if i < retries - 1:
                time.sleep(exchange.rateLimit / 1000)
            else:
                logger.error(f"❌ 已达到最大重试次数。"); return []
    return []


def update_top_symbols_cache(exchange, config):
    global cached_top_symbols;
    logger.info("🔄 正在更新热门币种缓存...")
    dyn_scan_conf = config['market_settings']['dynamic_scan']
    new_symbols = get_top_n_symbols_by_volume(exchange, top_n=dyn_scan_conf['top_n_for_signals'],
                                              exclude_list=[s.upper() for s in dyn_scan_conf['exclude_symbols']],
                                              market_type=config['app_settings']['default_market_type'])
    if new_symbols:
        fixed_symbols = set(config['market_settings']['static_symbols'])
        cached_top_symbols = sorted(list(fixed_symbols.union(set(new_symbols))))
        logger.info(f"✅ 热门币种缓存已更新，当前共监控 {len(cached_top_symbols)} 个交易对。")
    else:
        logger.warning("更新缓存失败，将继续使用旧的列表（如有）。")


def run_daily_report(exchange, config):
    logger.info("--- ☀️ 开始执行每日宏观市场报告 (合约市场) ---")
    try:
        update_top_symbols_cache(exchange, config)
        if not cached_top_symbols: logger.warning("报告任务中止：热门币种缓存为空，无法生成报告。"); return
        report_conf = config['daily_report']
        symbols_to_scan = cached_top_symbols[:report_conf['top_n_by_volume']]
        gainers_list, consecutive_up_list, volume_ratio_list = [], [], []
        logger.info(f"...正在基于 {len(symbols_to_scan)} 个热门合约生成报告...")
        required_len = max(report_conf['max_consecutive_check_days'] + 2, report_conf['volume_ma_period'] + 2)
        for symbol in symbols_to_scan:
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, '1d', limit=required_len)
                if len(ohlcv) < required_len: continue
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                yesterday = df.iloc[-2]
                if yesterday['open'] > 0: gainers_list.append(
                    {'symbol': symbol, 'gain': ((yesterday['close'] - yesterday['open']) / yesterday['open']) * 100})
                count = 0
                for i in range(2, len(df) + 1):
                    if df.iloc[-i]['close'] > df.iloc[-i]['open']:
                        count += 1
                    else:
                        break
                if count >= report_conf['min_consecutive_days']: consecutive_up_list.append(
                    {'symbol': symbol, 'days': count})
                df['volume_ma'] = df['volume'].rolling(window=report_conf['volume_ma_period']).mean().shift(1)
                vol_ma = df.iloc[-2]['volume_ma']
                if vol_ma and vol_ma > 0: volume_ratio_list.append(
                    {'symbol': symbol, 'ratio': yesterday['volume'] / vol_ma, 'volume': yesterday['volume'],
                     'volume_ma': vol_ma})
                time.sleep(exchange.rateLimit / 2000)
            except Exception as e:
                logger.debug(f"扫描 {symbol} 报告时出错: {e}"); continue
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d');
        title = f"📰 {date_str} 合约市场热点报告";
        message = ""
        if gainers_list:
            sorted_gainers = sorted(gainers_list, key=lambda x: x['gain'], reverse=True)[:report_conf['top_n_gainers']]
            message += f"### 🚀 合约昨日涨幅榜\n\n";
            for i, item in enumerate(
                sorted_gainers): message += f"{'🥇🥈🥉🔥'[i if i < 4 else 3]} **{item['symbol']}**\n> **涨幅: {item['gain']:.2f}%**\n\n"
        if volume_ratio_list:
            sorted_ratios = sorted(volume_ratio_list, key=lambda x: x['ratio'], reverse=True)[
                            :report_conf['top_n_volume_ratio']]
            message += f"\n---\n\n### 📈 昨日成交量异动榜\n\n"
            for i, item in enumerate(sorted_ratios): message += (
                f"{'🥇🥈🥉⚡️'[i if i < 4 else 3]} **{item['symbol']}**\n> **放量倍数: {item['ratio']:.2f} 倍**\n> (昨日量: {item['volume']:.0f}, 均量: {item['volume_ma']:.0f})\n\n")
        if consecutive_up_list:
            sorted_consecutive = sorted(consecutive_up_list, key=lambda x: x['days'], reverse=True)
            message += f"\n---\n\n### 💪 连涨强势合约\n\n"
            for item in sorted_consecutive: message += f"💪 **{item['symbol']}**\n> **连涨: {item['days']} 天** {'🔥' * (item['days'] // 2) if item['days'] > 3 else '🔥' if item['days'] == 3 else ''}\n\n"
        if message: send_dingtalk_alert(config, title, message, "Market Report")
        logger.info("--- ✅ 每日宏观市场报告完成 ---")
    except Exception as e:
        logger.error(f"❌ 执行每日报告任务时发生严重错误: {e}", exc_info=True)


# ==============================================================================
# 5. 策略逻辑函数 (无变化)
# ==============================================================================
def check_ema_signals(exchange, symbol, timeframe, config):
    try:
        now_utc = datetime.now(timezone.utc);
        tf_minutes = timeframe_to_minutes(timeframe);
        cooldown_minutes = tf_minutes
        params = config['strategy_params'];
        ema_params = params['ema_cross']
        atr_period = ema_params.get('atr_period', 14);
        atr_multiplier = ema_params.get('atr_multiplier', 0.3)
        trend_params_config = params
        selected_trend_params = trend_params_config.get('trend_ema_short' if tf_minutes <= 60 else 'trend_ema_long',
                                                        trend_params_config.get('trend_ema', {}))
        limit = max(ema_params['period'], selected_trend_params.get('long', 120), atr_period) + 50
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < limit - 49: return
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df.ta.atr(length=atr_period, append=True);
        atr_col = f"ATRr_{atr_period}"
        if atr_col not in df.columns: return
        trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, trend_params_config);
        trend_message = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"
        ema_col = f"EMA_{ema_params['period']}";
        df.ta.ema(length=ema_params['period'], append=True)
        if ema_col not in df.columns: return
        df = df.dropna().reset_index(drop=True)
        if len(df) < 2: return
        current, prev = df.iloc[-1], df.iloc[-2]
        if pd.isna(current[atr_col]) or current[atr_col] == 0: return
        bullish = (current['close'] > current[ema_col] + atr_multiplier * current[atr_col]) and (
                    prev['high'] < prev[ema_col])
        bearish = (current['close'] < current[ema_col] - atr_multiplier * current[atr_col]) and (
                    prev['low'] > prev[ema_col])
        if bullish or bearish:
            vol_text = "";
            actual_vol_ratio = 0.0
            if ema_params.get('volume_confirm', False):
                dynamic_multiplier = get_dynamic_volume_multiplier(symbol, config,
                                                                   ema_params.get('volume_multiplier', 1.5))
                vol_br_params = params.get('volume_breakout', {})
                is_vol_over, vol_text, actual_vol_ratio = is_realtime_volume_over(df, tf_minutes,
                                                                                  vol_br_params.get('volume_ma_period',
                                                                                                    20),
                                                                                  dynamic_multiplier)
                if not is_vol_over: return
            cross_type = "UP" if bullish else "DOWN";
            alert_key = f"{symbol}_{timeframe}_EMACROSS_VALID_{cross_type}_REALTIME"
            if alerted_states.get(alert_key) and now_utc < alerted_states[alert_key]: return
            action = "有效突破" if bullish else "有效跌破"
            title_vol_part = f"({actual_vol_ratio:.1f}x) " if ema_params.get('volume_confirm', False) else ""
            title = f"🚀 EMA 放量 {title_vol_part}{action}: {symbol} ({timeframe})".replace("放量  ", "")
            message = (
                f"{trend_message}**信号**: 价格 **{'放量' if ema_params.get('volume_confirm', False) else ''}实时{action}** EMA({ema_params['period']})。\n\n"
                f"**当前价**: {current['close']:.4f}\n**EMA值**: {current[ema_col]:.4f}\n"
                f"**ATR值 ({atr_period})**: {current[atr_col]:.4f}\n"
                f"**ATR缓冲区**: {atr_multiplier}x ATR = {(atr_multiplier * current[atr_col]):.4f}\n\n{vol_text}")
            send_dingtalk_alert(config, title, message, symbol);
            alerted_states[alert_key] = calculate_cooldown_time(cooldown_minutes);
            save_alert_states()
    except Exception as e:
        logger.error(f"❌ 处理 {symbol} {timeframe} (EMA信号) 时出错: {e}", exc_info=True)


def check_kdj_cross(exchange, symbol, timeframe, config):
    try:
        now_utc = datetime.now(timezone.utc);
        tf_minutes = timeframe_to_minutes(timeframe);
        cooldown_minutes = tf_minutes / 2
        params = config['strategy_params'];
        kdj_params = params['kdj_cross']
        trend_params_config = params
        selected_trend_params = trend_params_config.get('trend_ema_short' if tf_minutes <= 60 else 'trend_ema_long',
                                                        trend_params_config.get('trend_ema', {}))
        limit = max(kdj_params['fast_k'] * 3, selected_trend_params.get('long', 120),
                    params.get('volume_breakout', {}).get('volume_ma_period', 20)) + 50
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < limit - 49: return
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, trend_params_config);
        trend_message = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"
        df.ta.kdj(fast=kdj_params['fast_k'], slow=kdj_params['slow_k'], signal=kdj_params['slow_d'], append=True)
        k_col, d_col = f"K_{kdj_params['fast_k']}_{kdj_params['slow_k']}_{kdj_params['slow_d']}", f"D_{kdj_params['fast_k']}_{kdj_params['slow_k']}_{kdj_params['slow_d']}"
        if k_col not in df.columns or d_col not in df.columns: return
        df = df.dropna().reset_index(drop=True)
        if len(df) < 2: return
        current, prev = df.iloc[-1], df.iloc[-2]
        golden = current[k_col] > current[d_col] and prev[k_col] <= prev[d_col]
        death = current[k_col] < current[d_col] and prev[k_col] >= prev[d_col]
        if not (golden or death): return
        signal_type_desc = ""
        if "多头趋势" in trend_status:
            if golden:
                signal_type_desc = "顺势看涨 (入场机会)"
            elif death:
                signal_type_desc = "回调警示 (减仓风险)"
        elif "空头趋势" in trend_status:
            if death:
                signal_type_desc = "顺势看跌 (入场机会)"
            elif golden:
                signal_type_desc = "反弹警示 (空单止盈/反弹风险)"
        else:
            if golden:
                signal_type_desc = "震荡金叉 (反弹机会)"
            elif death:
                signal_type_desc = "震荡死叉 (下跌机会)"
        if not signal_type_desc: return
        vol_text = "";
        actual_vol_ratio = 0.0
        if kdj_params.get('volume_confirm', False):
            dynamic_multiplier = get_dynamic_volume_multiplier(symbol, config, kdj_params.get('volume_multiplier', 1.5))
            vol_br_params = params.get('volume_breakout', {})
            is_vol_over, vol_text, actual_vol_ratio = is_realtime_volume_over(df, tf_minutes,
                                                                              vol_br_params.get('volume_ma_period', 20),
                                                                              dynamic_multiplier)
            if not is_vol_over: return
        alert_key = f"{symbol}_{timeframe}_KDJ_{signal_type_desc.split(' ')[0]}_REALTIME"
        if alerted_states.get(alert_key) and now_utc < alerted_states[alert_key]: return
        emoji_map = {"看涨": "📈", "看跌": "📉", "警示": "⚠️", "金叉": "📈", "死叉": "📉", "机会": "💡"};
        emoji = emoji_map.get(signal_type_desc.split(' ')[0].replace("顺势", "").replace("震荡", ""), "⚙️")
        title_vol_part = f"({actual_vol_ratio:.1f}x) " if kdj_params.get('volume_confirm', False) else ""
        title = f"{emoji} KDJ {title_vol_part}信号: {signal_type_desc} ({symbol} {timeframe})".replace("  ", " ")
        message = (f"{trend_message}**信号解读**: {signal_type_desc}信号出现。\n\n"
                   f"**当前K/D值**: {current[k_col]:.2f} / {current[d_col]:.2f}\n"
                   f"**当前价**: {current['close']:.4f}\n\n{vol_text}")
        send_dingtalk_alert(config, title, message, symbol);
        alerted_states[alert_key] = calculate_cooldown_time(cooldown_minutes);
        save_alert_states()
    except Exception as e:
        logger.error(f"❌ 处理 {symbol} {timeframe} (KDJ信号) 时出错: {e}", exc_info=True)


def check_volatility_breakout(exchange, symbol, timeframe, config):
    try:
        now_utc = datetime.now(timezone.utc);
        tf_minutes = timeframe_to_minutes(timeframe);
        cooldown_minutes = tf_minutes
        params = config['strategy_params'];
        vol_params = params['volatility_breakout']
        trend_params_config = params
        selected_trend_params = trend_params_config.get('trend_ema_short' if tf_minutes <= 60 else 'trend_ema_long',
                                                        trend_params_config.get('trend_ema', {}))
        alert_key = f"{symbol}_{timeframe}_VOLATILITY_REALTIME"
        if alerted_states.get(alert_key) and now_utc < alerted_states[alert_key]: return
        vol_br_params = params.get('volume_breakout', {});
        volume_ma_period = vol_br_params.get('volume_ma_period', 20)
        limit = max(vol_params.get('atr_period', 14) + 5, selected_trend_params.get('long', 120), volume_ma_period) + 50
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < limit - 49: return
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, trend_params_config);
        trend_message = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"
        atr_period = vol_params.get('atr_period', 14)
        dynamic_atr_multiplier = get_dynamic_atr_multiplier(symbol, config, vol_params.get('atr_multiplier', 2.5))
        atr_col = f"ATRr_{atr_period}";
        df.ta.atr(length=atr_period, append=True)
        if atr_col not in df.columns: return
        df = df.dropna().reset_index(drop=True)
        if len(df) < 2: return
        current, prev = df.iloc[-1], df.iloc[-2]
        if pd.isna(prev[atr_col]) or prev[atr_col] == 0: return
        is_volatility_breakout = (current['high'] - current['low']) > prev[atr_col] * dynamic_atr_multiplier
        if is_volatility_breakout:
            vol_text = "";
            actual_vol_ratio = 0.0;
            volume_confirmed = False
            if vol_params.get('volume_confirm', False):
                dynamic_vol_multiplier = get_dynamic_volume_multiplier(symbol, config,
                                                                       vol_params.get('volume_multiplier', 2.0))
                is_vol_over, vol_text, actual_vol_ratio = is_realtime_volume_over(df, tf_minutes, volume_ma_period,
                                                                                  dynamic_vol_multiplier)
                if not is_vol_over: return
                volume_confirmed = True
            title_vol_part = f"({actual_vol_ratio:.1f}x) " if volume_confirmed else ""
            title = f"💥 放量 {title_vol_part}盘中波动异常: {symbol} ({timeframe})".replace("放量  ", "")
            message = (f"{trend_message}"
                       f"**波动分析**:\n> **当前波幅**: `{current['high'] - current['low']:.4f}`\n"
                       f"> **ATR阈值({dynamic_atr_multiplier:.1f}x)**: `{(prev[atr_col] * dynamic_atr_multiplier):.4f}`\n"
                       f"> (参考ATR: `{prev[atr_col]:.4f}`)\n\n"
                       f"{vol_text}")
            send_dingtalk_alert(config, title, message, symbol);
            alerted_states[alert_key] = calculate_cooldown_time(cooldown_minutes);
            save_alert_states()
    except Exception as e:
        logger.error(f"❌ 处理 {symbol} {timeframe} (波动率信号) 时出错: {e}", exc_info=True)


def check_volume_breakout(exchange, symbol, timeframe, config):
    try:
        now_utc = datetime.now(timezone.utc);
        tf_minutes = timeframe_to_minutes(timeframe);
        cooldown_minutes = tf_minutes
        params = config['strategy_params'];
        vol_br_params = params['volume_breakout']
        level_conf = vol_br_params.get('level_detection', {})
        if not level_conf.get('method') == 'advanced': return
        limit = 200
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < 50: return
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        current_price = df.iloc[-1]['close']
        all_resistances = [];
        all_supports = []
        if level_conf.get('fractals', {}).get('enabled', False):
            fractal_period = level_conf['fractals'].get('period', 2)
            fractal_highs, fractal_lows = find_fractal_levels(df.iloc[:-fractal_period], fractal_period)
            all_resistances.extend(fractal_highs)
            all_supports.extend(fractal_lows)
        if level_conf.get('pivots', {}).get('enabled', False):
            try:
                daily_ohlcv_list = exchange.fetch_ohlcv(symbol, '1d', limit=2)
                if len(daily_ohlcv_list) >= 2:
                    prev_day_data = daily_ohlcv_list[-2]
                    prev_day_ohlc = {'high': prev_day_data[2], 'low': prev_day_data[3], 'close': prev_day_data[4]}
                    pivot_resistances, pivot_supports = calculate_pivot_points(prev_day_ohlc)
                    all_resistances.extend(pivot_resistances)
                    all_supports.extend(pivot_supports)
            except Exception as e:
                logger.debug(f"为 {symbol} 获取枢轴点数据失败: {e}")
        resistances = sorted([r for r in all_resistances if r['level'] > current_price], key=lambda x: x['level'])
        supports = sorted([s for s in all_supports if s['level'] < current_price], key=lambda x: x['level'],
                          reverse=True)
        confluence_pct = level_conf.get('confluence_pct', 0.2) / 100.0

        def merge_levels(levels):
            if not levels: return []
            merged = [];
            current_merge = levels[0];
            current_merge['types'] = [current_merge['type']]
            for next_level in levels[1:]:
                if abs(next_level['level'] - current_merge['level']) / current_merge['level'] <= confluence_pct:
                    current_merge['level'] = (current_merge['level'] + next_level['level']) / 2
                    current_merge['types'].append(next_level['type'])
                else:
                    merged.append(current_merge);
                    current_merge = next_level;
                    current_merge['types'] = [current_merge['type']]
            merged.append(current_merge)
            return merged

        resistances = merge_levels(resistances)
        supports = merge_levels(supports)
        if not resistances and not supports: return
        df.ta.atr(length=vol_br_params.get('atr_period', 14), append=True)
        df = df.dropna().reset_index(drop=True)
        if len(df) < 2: return
        current, prev = df.iloc[-1], df.iloc[-2]
        atr_val = current[f"ATRr_{vol_br_params.get('atr_period', 14)}"]
        atr_buffer = atr_val * vol_br_params.get('atr_multiplier', 0.2)
        trend_status, _ = get_current_trend(df.copy(), timeframe, params)
        if "多头" in trend_status and resistances:
            closest_res = resistances[0]
            if current['close'] > closest_res['level'] + atr_buffer and prev['high'] < closest_res['level']:
                check_and_notify_breakout(exchange, symbol, timeframe, config, df, "resistance", closest_res,
                                          current['close'])
        if "空头" in trend_status and supports:
            closest_sup = supports[0]
            if current['close'] < closest_sup['level'] - atr_buffer and prev['low'] > closest_sup['level']:
                check_and_notify_breakout(exchange, symbol, timeframe, config, df, "support", closest_sup,
                                          current['close'])
    except Exception as e:
        logger.error(f"❌ 处理 {symbol} {timeframe} (高级量价突破) 时出错: {e}", exc_info=True)


def check_and_notify_breakout(exchange, symbol, timeframe, config, df, break_type, level_info, current_price):
    params = config['strategy_params']
    vol_br_params = params['volume_breakout']
    tf_minutes = timeframe_to_minutes(timeframe)
    dynamic_multiplier = get_dynamic_volume_multiplier(symbol, config, vol_br_params.get('volume_multiplier', 1.5))
    is_vol_over, vol_text, actual_vol_ratio = is_realtime_volume_over(df, tf_minutes,
                                                                      vol_br_params.get('volume_ma_period', 20),
                                                                      dynamic_multiplier)
    if not is_vol_over: return
    level_type_str = "+".join(sorted(list(set(level_info['types']))))
    alert_key = f"{symbol}_{timeframe}_ADV_BREAK_{break_type}_{level_type_str}_{level_info['level']:.4f}"
    if alerted_states.get(alert_key) and datetime.now(timezone.utc) < alerted_states[alert_key]: return
    is_confluence = len(level_info['types']) > 1
    direction = "突破关键阻力" if break_type == "resistance" else "跌破关键支撑"
    level_prefix = "🔥共振区域" if is_confluence else "水平位"
    emoji = "🚨" if break_type == "resistance" else "📉"
    title = f"{emoji} 放量({actual_vol_ratio:.1f}x) {direction}: {symbol} ({timeframe})"
    message = (f"**信号**: **放量({actual_vol_ratio:.1f}x) {direction}**!\n\n"
               f"**突破类型**: {level_prefix} ({level_type_str})\n"
               f"**关键价位**: {level_info['level']:.4f}\n"
               f"**当前价格**: {current_price:.4f}\n\n"
               f"{vol_text}")
    send_dingtalk_alert(config, title, message, symbol)
    alerted_states[alert_key] = calculate_cooldown_time(tf_minutes)
    save_alert_states()


def check_rsi_divergence(exchange, symbol, timeframe, config):
    try:
        now_utc = datetime.now(timezone.utc);
        tf_minutes = timeframe_to_minutes(timeframe);
        cooldown_minutes = tf_minutes * 2
        params = config['strategy_params'];
        rsi_params = params['rsi_divergence'];
        trend_params_config = params
        selected_trend_params = trend_params_config.get('trend_ema_short' if tf_minutes <= 60 else 'trend_ema_long',
                                                        trend_params_config.get('trend_ema', {}))
        limit = max(rsi_params['lookback_period'] + rsi_params['rsi_period'],
                    selected_trend_params.get('long', 120)) + 50
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < limit - 49: return
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        trend_status, trend_emoji = get_current_trend(df.copy(), timeframe, trend_params_config);
        trend_message = f"**当前趋势**: {trend_emoji} {trend_status}\n\n"
        df['rsi'] = pta.rsi(df['close'], length=rsi_params['rsi_period'])
        if 'rsi' not in df.columns: return
        df = df.dropna().reset_index(drop=True)
        if len(df) < rsi_params['lookback_period'] + 1: return
        recent_df, current = df.iloc[-rsi_params['lookback_period'] - 1:-1], df.iloc[-1]
        if current['close'] > recent_df['close'].max() and current['rsi'] < recent_df['rsi'].max():
            alert_key = f"{symbol}_{timeframe}_DIV_TOP_REALTIME"
            if not (alerted_states.get(alert_key) and now_utc < alerted_states[alert_key]):
                title, message = f"🚩 实时RSI顶背离风险: {symbol} ({timeframe})", trend_message + "**信号**: 价格创近期新高，但RSI指标出现衰弱迹象（潜在反转/回调风险）。"
                send_dingtalk_alert(config, title, message, symbol);
                alerted_states[alert_key] = calculate_cooldown_time(cooldown_minutes);
                save_alert_states()
        if current['close'] < recent_df['close'].min() and current['rsi'] > recent_df['rsi'].min():
            alert_key = f"{symbol}_{timeframe}_DIV_BOTTOM_REALTIME"
            if not (alerted_states.get(alert_key) and now_utc < alerted_states[alert_key]):
                title, message = f"⛳️ 实时RSI底背离机会: {symbol} ({timeframe})", trend_message + "**信号**: 价格创近期新低，但RSI指标出现企稳迹象（潜在反转/反弹机会）。"
                send_dingtalk_alert(config, title, message, symbol);
                alerted_states[alert_key] = calculate_cooldown_time(cooldown_minutes);
                save_alert_states()
    except Exception as e:
        logger.error(f"❌ 处理 {symbol} {timeframe} (RSI背离) 时出错: {e}", exc_info=True)


# ==============================================================================
# 6. 主调度与执行模块
# ==============================================================================
def run_signal_check_cycle(exchange, config):
    global cached_top_symbols;
    logger.info("=" * 60);
    logger.info(f"🔄 开始执行动态热点监控循环...")
    if config['market_settings']['dynamic_scan']['enabled'] and not cached_top_symbols:
        logger.info("缓存为空，首次获取热门币种列表...");
        update_top_symbols_cache(exchange, config)
        if not cached_top_symbols: logger.error("首次获取热门币种列表失败，无法执行监控。"); return
    symbols_to_check = cached_top_symbols if config['market_settings']['dynamic_scan']['enabled'] else \
    config['market_settings']['static_symbols']
    if not symbols_to_check: logger.warning("没有需要监控的交易对。"); return
    logger.info(f"本轮将使用 {len(symbols_to_check)} 个交易对进行并发扫描...")
    strategies = {'ema_cross': check_ema_signals, 'kdj_cross': check_kdj_cross,
                  'volatility_breakout': check_volatility_breakout, 'volume_breakout': check_volume_breakout,
                  'rsi_divergence': check_rsi_divergence}

    def check_symbol_all_strategies(symbol):
        logger.debug(f"--- [Thread] 正在检查: {symbol} ---")  # DEBUG级别日志
        for timeframe in config['market_settings']['timeframes']:
            for name, func in strategies.items():
                if config['strategy_params'].get(name, {}).get('enabled', False):
                    try:
                        func(exchange, symbol, timeframe, config); time.sleep(exchange.rateLimit / 2000)
                    except Exception as e:
                        logger.error(f"执行策略 {name} on {symbol} {timeframe} 时发生顶层错误: {e}", exc_info=True)
        return f"已完成 {symbol} 的检查"

    max_workers = config.get('app_settings', {}).get('max_workers', 10)
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='Monitor') as executor:
        futures = {executor.submit(check_symbol_all_strategies, symbol): symbol for symbol in symbols_to_check}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result(); logger.debug(f"任务完成: {result}")
            except Exception as e:
                logger.error(f"任务 {symbol} 在执行中发生严重错误: {e}", exc_info=True)
    logger.info("✅ 动态热点监控循环完成")


def handle_exit(signum, frame):
    logger.info("\n👋 收到退出信号，程序正在优雅关闭...");
    sys.exit(0)


def main():
    global logger;
    signal.signal(signal.SIGINT, handle_exit);
    signal.signal(signal.SIGTERM, handle_exit)
    try:
        config_file_paths = ['config.json', 'config/config.json']
        config = None
        for path in config_file_paths:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                break
            except FileNotFoundError:
                continue
        if config is None: raise FileNotFoundError(f"错误: 找不到配置文件，尝试了 {', '.join(config_file_paths)}")
    except json.JSONDecodeError as e:
        print(f"错误: 配置文件 '{path}' 格式不正确。 {e}"); return
    except FileNotFoundError as e:
        print(e); return

    logger = setup_logging(config['app_settings'].get("log_level", "INFO"));
    load_alert_states()
    app_conf = config['app_settings']
    try:
        exchange = getattr(ccxt, app_conf['exchange'])(
            {'enableRateLimit': True, 'options': {'defaultType': app_conf['default_market_type']}})
    except AttributeError:
        logger.error(f"❌ 交易所 '{app_conf['exchange']}' 不支持"); return
    except Exception as e:
        logger.error(f"❌ 初始化交易所失败: {e}", exc_info=True); return

    logger.info("🚀 终极监控与信号程序已启动 (V34.1 - 启动流程与日志优化版)")
    logger.info(
        f"📊 交易所: {app_conf['exchange']} | 市场: {app_conf['default_market_type']} | 间隔: {app_conf['check_interval_minutes']} 分钟")

    # 移除启动时的预加载，让各个任务自行按需加载
    # if config['market_settings']['dynamic_scan']['enabled']:
    #     logger.info("\n📌 正在初始化热门币种缓存...")
    #     update_top_symbols_cache(exchange, config)

    logger.info("\n📌 首次运行主监控循环...")
    run_signal_check_cycle(exchange, config)

    if config['daily_report']['enabled']:
        logger.info("\n📌 首次运行市场报告...")
        try:
            run_daily_report(exchange, config)
        except Exception as e:
            logger.error(f"首次市场报告失败: {e}", exc_info=True)

    scheduler = BlockingScheduler(timezone='Asia/Shanghai')
    if config['daily_report']['enabled']:
        report_conf = config['daily_report']
        scan_time = report_conf['scan_time_beijing'].split(':')
        scheduler.add_job(run_daily_report, CronTrigger(hour=scan_time[0], minute=scan_time[1]),
                          args=[exchange, config], name="DailyReport")
        logger.info(f"   - 每日市场报告(及缓存更新)已添加，将在每天北京时间 {scan_time[0]}:{scan_time[1]} 运行。")

    scheduler.add_job(run_signal_check_cycle, IntervalTrigger(minutes=app_conf['check_interval_minutes']),
                      args=[exchange, config], name="SignalCheckCycle")
    logger.info(f"   - 动态热点监控任务已添加，每 {app_conf['check_interval_minutes']} 分钟运行一次。")
    logger.info(f"\n📅 调度器已启动，请保持程序运行。按 Ctrl+C 退出。")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()