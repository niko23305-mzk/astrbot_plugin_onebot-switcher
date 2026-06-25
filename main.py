import asyncio
import websockets
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.api import logger

class MyPlugin(Star):
    def __init__(self, context: Context):
        """初始化插件：读取配置，尝试启动后台连接任务"""
        super().__init__(context)
        config = context.get_config()
        self.ws_url = config.get("ws_url", "ws://localhost:8765")
        self.reconnect_interval = config.get("reconnect_interval", 5.0)
        self.heartbeat_interval = config.get("heartbeat_interval", 30.0)

        self.websocket = None
        self._maintain_task = None
        self._heartbeat_task = None
        self._shutdown = False

        # 若当前有事件循环，立即启动后台连接任务；否则延迟到第一条消息
        try:
            loop = asyncio.get_running_loop()
            self._maintain_task = loop.create_task(self._maintain_connection())
            logger.info("WebSocket 后台连接任务已启动")
        except RuntimeError:
            logger.info("将在收到第一条消息时启动连接任务")

    async def _maintain_connection(self):
        """后台任务：持续维护 WebSocket 连接，断线自动重连"""
        while not self._shutdown:
            try:
                logger.info(f"正在连接 WebSocket: {self.ws_url}")
                async with websockets.connect(self.ws_url) as ws:
                    self.websocket = ws
                    logger.info("WebSocket 连接成功")
                    # 启动心跳
                    if self.heartbeat_interval > 0:
                        self._heartbeat_task = asyncio.create_task(self._heartbeat(ws))
                    await ws.wait_closed()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"WebSocket 连接异常: {e}")
            finally:
                if self._heartbeat_task:
                    self._heartbeat_task.cancel()
                    self._heartbeat_task = None
                self.websocket = None
                if not self._shutdown:
                    logger.info(f"将在 {self.reconnect_interval} 秒后重连...")
                    await asyncio.sleep(self.reconnect_interval)

    async def _heartbeat(self, ws):
        """心跳任务：周期性发送 ping，失败时关闭连接触发重连"""
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                await ws.ping()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"心跳失败: {e}，断开连接")
            if not ws.closed:
                await ws.close()

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent):
        """接收所有消息并转发到 WebSocket，不发送回复"""
        # 若后台任务未启动，则在此启动
        if self._maintain_task is None:
            self._maintain_task = asyncio.create_task(self._maintain_connection())
            logger.info("收到第一条消息，启动 WebSocket 连接任务")

        sender = event.get_sender_name()
        content = event.message_str
        payload = f"{sender}: {content}"

        ws = self.websocket
        if ws and not ws.closed:
            try:
                await ws.send(payload)
                logger.debug(f"已转发: {payload}")
            except Exception as e:
                logger.error(f"发送失败: {e}")
        else:
            logger.warning("WebSocket 未连接，消息丢弃")

    async def terminate(self):
        """插件卸载时清理资源"""
        self._shutdown = True
        if self._maintain_task:
            self._maintain_task.cancel()
            try:
                await self._maintain_task
            except asyncio.CancelledError:
                pass
        if self.websocket and not self.websocket.closed:
            await self.websocket.close()
        logger.info("WebSocket 转发插件已停止")
