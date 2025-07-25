# --- START OF FILE app/services/notification_service.py (CORRECTED IMPORTS V46.2) ---
import base64
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
import os
import threading
import queue

import requests
from plyer import notification

# 【核心修正】: 只从 state 导入队列
from app.state import notification_queue

logger = logging.getLogger(__name__)


def _send_desktop_notification(title, message, timeout=10):
    try:
        notification.notify(title=title, message=message, app_name='Crypto Monitor', timeout=timeout)
        logger.info(f"✅ 桌面通知发送成功: {title}")
    except Exception as e:
        logger.error(f"❌ 发送桌面通知时出错: {e}", exc_info=True)


def _send_dingtalk_request(config, payload):
    """ 封装了签名和发送请求的通用函数 """
    dingtalk_conf = config.get('notification_settings', {}).get('dingtalk', {})
    webhook_url = dingtalk_conf.get('webhook_url')
    secret = dingtalk_conf.get('secret')

    if not webhook_url:
        logger.warning("钉钉 webhook_url 未配置")
        return False

    url_with_sign = webhook_url
    if secret:
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8');
        string_to_sign = f'{timestamp}\n{secret}'
        hmac_code = hmac.new(secret_enc, string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        url_with_sign = f"{webhook_url}&timestamp={timestamp}&sign={sign}"

    try:
        response = requests.post(url_with_sign, data=json.dumps(payload), headers={'Content-Type': 'application/json'},
                                 timeout=60)
        if response.json().get('errcode') == 0:
            return True
        else:
            logger.error(f"钉钉API返回错误: {response.json()}")
            return False
    except Exception as e:
        logger.error(f"请求钉钉API时发生错误: {e}", exc_info=True)
        return False


def send_alert(config, title, message, symbol="N/A", image_path=None):
    # 消息放入队列
    notification_queue.put({
        "config": config,
        "title": title,
        "message": message,
        "symbol": symbol,
        "image_path": image_path
    })


def notification_consumer():
    """
    一个后台线程，不断从队列中取出消息并立即发送。
    """
    while True:
        try:
            item = notification_queue.get()  # 阻塞直到有消息

            config = item['config']
            title = item['title']
            message = item['message']
            symbol = item['symbol']

            notif_conf = config.get('notification_settings', {})

            # 发送桌面通知
            if notif_conf.get('desktop', {}).get('enabled', False):
                _send_desktop_notification(title, f"交易对: {symbol}",
                                           timeout=notif_conf['desktop'].get('timeout_seconds', 10))

            # 发送钉钉通知
            if notif_conf.get('dingtalk', {}).get('enabled', False):
                payload = {
                    "msgtype": "markdown",
                    "markdown": {"title": title, "text": f"### {title}\n\n{message}"},
                    "at": {"isAtAll": False}
                }
                if _send_dingtalk_request(config, payload):
                    logger.info(f"✅ (队列)钉钉消息发送成功: {title}")
                else:
                    logger.error(f"❌ (队列)钉钉消息发送失败: {title}")

            notification_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"通知消费者线程发生错误: {e}", exc_info=True)
# --- END OF FILE app/services/notification_service.py (CORRECTED IMPORTS V46.2) ---
