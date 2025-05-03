
"""
需要处理复杂异步任务
要求任务状态跟踪
面临性能瓶颈（如高频更新）
"""
class AsyncUIHandler:
    def __init__(self, backup_app):
        self._loop = asyncio.new_event_loop()
        self._queue = asyncio.Queue(loop=self._loop)
        self.backup_app = backup_app
        Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _process_tasks(self):
        while True:
            action, payload, future = await self._queue.get()
            try:
                method = getattr(self.backup_app, action)
                result = method(payload)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)

    def submit(self, action, payload):
        future = self._loop.create_future()
        self._loop.call_soon_threadsafe(
            self._queue.put_nowait, 
            (action, payload, future)
        )
        return future

"""
需要简单优先级控制
希望保持代码简单性
少量并发即可满足需求
"""
class HybridUIHandler:
    def __init__(self, backup_app):
        self.queue = queue.PriorityQueue()  # 带优先级的队列
        self.backup_app = backup_app
        self._running = True
        self._workers = [
            Thread(target=self._worker, daemon=True) 
            for _ in range(2)  # 2个工作线程
        ]
        for w in self._workers:
            w.start()

    def _worker(self):
        while self._running:
            try:
                _, (action, payload) = self.queue.get(timeout=0.5)
                method = getattr(self.backup_app, action)
                method(payload)
            except Exception as e:
                logger.error(f"任务失败: {e}")

    def submit(self, action, payload, priority=0):
        """priority越小优先级越高"""
        if not self._running:
            raise RuntimeError("处理器已关闭")
        self.queue.put((priority, (action, payload)))