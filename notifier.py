"""
Telegram 通知模块
支持交易通知、风控告警、状态报告
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Optional
from config import TELEGRAM

logger = logging.getLogger(__name__)

# 尝试导入 aiohttp，若无则降级
try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False
    import urllib.request
    import json as _json


class TelegramNotifier:
    """Telegram Bot 通知器"""

    BASE_URL = "https://api.telegram.org/bot{token}"

    def __init__(self):
        self.token = TELEGRAM["bot_token"]
        self.chat_id = TELEGRAM["chat_id"]
        self.notify_level = TELEGRAM["notify_level"]
        self.enabled = bool(self.token and self.chat_id)
        self._rate_limit: dict = {}  # key -> last_send_time
        self._min_interval = 5  # 同类消息最小间隔(秒)

        if not self.enabled:
            logger.warning("[Telegram] Bot token or chat_id not set, notifications disabled")
        else:
            logger.info(f"[Telegram] Notifications enabled | Level: {self.notify_level}")

    # ==================== 公共接口 ====================

    def startup(self, config_summary: str):
        """启动通知"""
        msg = (
            "🚀 *机器人启动成功*\n"
            f"```\n{config_summary}\n```"
        )
        self._send(msg, level="critical")

    def shutdown(self, reason: str, pnl_summary: str):
        """关闭通知"""
        msg = (
            "🛑 *机器人已停止*\n"
            f"停止原因: {reason}\n"
            f"```\n{pnl_summary}\n```"
        )
        self._send(msg, level="critical")

    def trade_open(self, strategy: str, symbol: str, side: str,
                   amount: float, price: float, extra: str = ""):
        """开仓通知"""
        emoji = "🟢" if side.lower() in ("buy", "long") else "🔴"
        msg = (
            f"{emoji} *开仓通知 | {strategy}*\n"
            f"交易对: `{symbol}`\n"
            f"方向: `{side.upper()}`\n"
            f"数量: `{amount:.6f}`\n"
            f"成交价: `{price:.2f}`"
        )
        if extra:
            msg += f"\n{extra}"
        self._send(msg, level="trade", rate_key=f"open_{strategy}_{symbol}")

    def trade_close(self, strategy: str, symbol: str, side: str,
                    pnl: float, reason: str, extra: str = ""):
        """平仓通知"""
        emoji = "✅" if pnl >= 0 else "❌"
        msg = (
            f"{emoji} *平仓通知 | {strategy}*\n"
            f"交易对: `{symbol}`\n"
            f"方向: `{side.upper()}`\n"
            f"本次盈亏: `{pnl:+.4f} USDT`\n"
            f"触发原因: {reason}"
        )
        if extra:
            msg += f"\n{extra}"
        self._send(msg, level="trade", rate_key=f"close_{strategy}_{symbol}")

    def grid_fill(self, symbol: str, side: str, price: float, grid_profit: float):
        """网格成交通知"""
        emoji = "📗" if side == "buy" else "📕"
        msg = (
            f"{emoji} *网格成交*\n"
            f"交易对: `{symbol}` | {side.upper()} @ `{price:.2f}`\n"
            f"本格利润: `{grid_profit:+.4f} USDT`"
        )
        self._send(msg, level="all", rate_key=f"grid_{side}_{price:.0f}")

    def funding_collected(self, symbol: str, amount: float, rate: float):
        """资金费率收取通知"""
        msg = (
            f"💰 *资金费率收入*\n"
            f"交易对: `{symbol}`\n"
            f"费率: `{rate:.6f}`\n"
            f"收入: `{amount:+.4f} USDT`"
        )
        self._send(msg, level="all", rate_key=f"funding_{symbol}")

    def dca_layer(self, symbol: str, layer: int, total_layers: int,
                  amount: float, price: float, avg_price: float):
        """DCA加仓通知"""
        msg = (
            f"📊 *趋势DCA加仓 [{layer}/{total_layers}]*\n"
            f"交易对: `{symbol}`\n"
            f"加仓数量: `{amount:.6f}` @ `{price:.2f}`\n"
            f"当前均价: `{avg_price:.2f}`"
        )
        self._send(msg, level="trade", rate_key=f"dca_{symbol}_{layer}")

    def risk_alert(self, alert_type: str, details: str, action: str):
        """风控告警"""
        msg = (
            f"⚠️ *风控告警*\n"
            f"告警类型: {alert_type}\n"
            f"详细信息: {details}\n"
            f"处理动作: `{action}`"
        )
        self._send(msg, level="critical")

    def risk_emergency(self, details: str):
        """紧急告警"""
        msg = (
            f"🚨🚨🚨 *紧急止损已触发* 🚨🚨🚨\n"
            f"{details}\n"
            f"所有策略已停止，请立即检查。"
        )
        self._send(msg, level="critical")

    def protection_mode(self, scope: str, reason: str, details: str = ""):
        """保护模式告警"""
        msg = (
            f"🛡️ *保护模式已触发*\n"
            f"范围: `{scope}`\n"
            f"原因: {reason}"
        )
        if details:
            msg += f"\n详情: {details}"
        self._send(msg, level="critical", rate_key=f"protect_{scope}_{reason[:40]}")

    def status_report(self, report: str):
        """定时状态报告"""
        msg = f"📈 *机器人状态报告（30分钟）*\n```\n{report}\n```"
        self._send(msg, level="all", rate_key="status_report")

    def market_state_change(self, old_state: str, new_state: str, adx: float):
        """市场状态变化"""
        state_emoji = {"trend": "📈", "range": "↔️", "transition": "🔄", "unknown": "❓"}
        msg = (
            f"{state_emoji.get(new_state, '❓')} *市场状态变化*\n"
            f"`{old_state}` → `{new_state}`\n"
            f"ADX: `{adx:.1f}`"
        )
        self._send(msg, level="all", rate_key="market_state")

    def error(self, module: str, error_msg: str):
        """错误通知"""
        msg = (
            f"🔥 *模块异常 | {module}*\n"
            f"```\n{error_msg[:500]}\n```"
        )
        self._send(msg, level="critical", rate_key=f"error_{module}")

    # ==================== 内部方法 ====================

    def _should_notify(self, level: str) -> bool:
        """检查是否应该发送此级别的通知"""
        if not self.enabled:
            return False
        levels = {"critical": 0, "trade": 1, "all": 2}
        msg_level = levels.get(level, 2)
        config_level = levels.get(self.notify_level, 2)
        return msg_level <= config_level

    def _rate_limited(self, key: Optional[str]) -> bool:
        """简单限流"""
        if not key:
            return False
        now = time.time()
        last = self._rate_limit.get(key, 0)
        if now - last < self._min_interval:
            return True
        self._rate_limit[key] = now
        return False

    def _send(self, text: str, level: str = "all", rate_key: str = None):
        """发送消息"""
        if not self._should_notify(level):
            return
        if self._rate_limited(rate_key):
            return

        timestamp = datetime.now().strftime("%H:%M:%S")
        text += f"\n\n`发送时间 {timestamp}`"

        try:
            if HAS_AIOHTTP:
                # 尝试获取已有的事件循环
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._send_async(text))
                except RuntimeError:
                    asyncio.run(self._send_async(text))
            else:
                self._send_sync(text)
        except Exception as e:
            logger.error(f"[Telegram] Send failed: {e}")

    async def _send_async(self, text: str):
        """异步发送"""
        url = f"{self.BASE_URL.format(token=self.token)}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(f"[Telegram] API error {resp.status}: {body}")
        except Exception as e:
            logger.error(f"[Telegram] Async send error: {e}")

    def _send_sync(self, text: str):
        """同步发送(无aiohttp时降级)"""
        url = f"{self.BASE_URL.format(token=self.token)}/sendMessage"
        payload = _json.dumps({
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    logger.error(f"[Telegram] API error {resp.status}")
        except Exception as e:
            logger.error(f"[Telegram] Sync send error: {e}")


# 全局单例
notifier = TelegramNotifier()
