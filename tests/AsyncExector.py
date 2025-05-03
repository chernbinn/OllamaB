import asyncio
import threading
import concurrent.futures
from typing import Callable, Any, Dict, Optional, Union
from concurrent.futures import Future, ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
from queue import Queue
import logging, os
import dill  # pip install dill
import logging
import time
from multiprocessing.managers import DictProxy
from multiprocessing import Manager, Lock
import psutil, signal, sys

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProcessTerminator:
    @staticmethod
    def terminate(pid: int):
        if sys.platform == 'win32':
            return ProcessTerminator._windows_terminate(pid)
        else:
            return ProcessTerminator._posix_terminate(pid)

    @staticmethod
    def _windows_terminate(pid: int):
        try:
            import ctypes
            PROCESS_TERMINATE = 1
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_TERMINATE, 0, pid)
            if not handle:
                raise ctypes.WinError()
            ctypes.windll.kernel32.TerminateProcess(handle, -1)
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        except Exception as e:
            logger.error(f"Windows terminate failed: {e}")
            return False

    @staticmethod
    def _posix_terminate(pid: int):
        try:
            os.kill(pid, signal.SIGKILL)
            return True
        except ProcessLookupError:
            return True  # 进程已退出
        except Exception as e:
            logger.error(f"POSIX terminate failed: {e}")
            return False

class SyncAsyncExecutor:
    SHUTDOWN = 0
    LOOP_NOT_READY = 1
    LOOP_STOPED = 2
    TASK_NOT_FOUND = 3
    TASKID_EXITED = 4
    CALLBACK_NOT_QUEUE = 5

    def __init__(self, *, max_workers: int = 6, 
                        max_processes: int = 2,
                        max_queue_size: int = 10, 
                        callback_direct: bool = True):
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._process_pool = ProcessPoolExecutor(max_workers=max_processes)
        self._running_tasks: Dict[str, Future] = {}
        self._queued_tasks: Dict[str, Dict] = {}  # 存储排队中的任务
        self._event_loop_thread: Optional[threading.Thread] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        
        self._callback_direct = callback_direct
        self._max_queue_size = max_queue_size
        self._lock = threading.Lock()
        self._loop_ready = threading.Event()
        self._shutdown_flag = False
        if not self._callback_direct:
            self._callback_queue = Queue()  # 用于主线程回调

        self._manager = Manager()
        self._process_pids = self._manager.dict()  # 共享字典
        self._process_lock = self._manager.Lock()  # 共享锁

        self._start_event_loop()        

    def _start_event_loop(self) -> None:
        """在后台线程启动事件循环"""
        def run_loop():
            try:
                self._event_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._event_loop) 
                self._loop_ready.set()  # 标记事件循环已就绪
                self._event_loop.run_forever()
            except Exception as e:
                logger.error(f"Error in asyncio event loop: {e}")            
            finally:
                if self._event_loop:
                    self._event_loop.close()
                if not self._shutdown_flag:
                    raise RuntimeError("Async event loop stopped", self.LOOP_STOPED)

        self._event_loop_thread = threading.Thread(
            target=run_loop, 
            daemon=False,
            name="AsyncEventLoop"
        )
        self._event_loop_thread.start()
        self._loop_ready.wait(timeout=10)
        if not self._loop_ready.is_set():
            raise RuntimeError("Failed to start event loop")

    def execute_async(
        self,
        task_id: str,
        func: Callable[..., Any],
        *args,
        is_long_task: bool,
        callback: Optional[Callable[[Union[Any, Exception]], None]] = None,
        **kwargs
    ) -> bool:
        """
        同步环境中启动异步任务
        返回: True表示任务已提交，False表示队列已满被拒绝
        """
        logger.debug(f"callback: {callback} -- 1")
        if self._shutdown_flag:
                raise RuntimeError("Executor is shutting down", self.SHUTDOWN)        

        with self._lock:            
            if task_id in self._running_tasks or task_id in self._queued_tasks:
                raise ValueError(f"Task {task_id} already exists", self.TASKID_EXITED)

            if len(self._running_tasks) >= self._thread_pool._max_workers:
                if len(self._queued_tasks) >= self._max_queue_size:
                    return False
                
                # 放入队列等待执行
                self._queued_tasks[task_id] = {
                    'func': func,
                    'args': args,
                    'kwargs': kwargs,
                    'callback': callback,
                    'is_long_task': is_long_task
                }
                return True

            # 立即执行
            logger.debug(f"callback: {callback} -- 11")
            self._submit_task(task_id, func, is_long_task, callback, *args, **kwargs)
            return True

    @staticmethod
    def _run_long_task(tid: str, func: Callable, args: tuple, kwargs: dict,
                      pids: DictProxy, lock: Any) -> Any:
        """静态方法确保可序列化"""
        pid = os.getpid()
        logger.debug(f"Running long task {tid} in process {pid}")
        try:
            with lock:
                pids[tid] = pid
            
            return func(*args, **kwargs)
        except Exception as e:
            raise ChildProcessError(f"Task {tid} failed: {e}")
        finally:
            with lock:
                pids.pop(tid, None)

    def _submit_task(
        self,
        task_id: str,
        func: Callable[..., Any],
        is_long_task: bool,
        callback: Optional[Callable[[Union[Any, Exception]], None]],
        *args,
        **kwargs
    ) -> None:
        if self._shutdown_flag:
            raise RuntimeError("Executor is shutting down", self.SHUTDOWN)
        if not self._event_loop or not self._loop_ready.is_set():
            raise RuntimeError("Event loop not ready", self.LOOP_NOT_READY)
        
    
        executor = self._process_pool if is_long_task else self._thread_pool
        logger.debug(f"Submitting {'long' if is_long_task else 'short'} task {task_id}")

        logger.debug(f"callback: {callback} -- 112")

        async def async_wrapper():
            try:
                if is_long_task:
                    try:
                        dill.dumps((func, args, kwargs))
                    except Exception as e:
                        raise ValueError(f"Task {task_id} not serializable: {e}")
                    result = await self._event_loop.run_in_executor(
                        self._process_pool,
                        self._run_long_task,  # 使用静态方法
                        task_id, func, args, kwargs,
                        self._process_pids, self._process_lock
                    )
                else:
                    result = await self._event_loop.run_in_executor(
                        executor,
                        partial(func, *args, **kwargs)
                    )
                return result
            except Exception as e:
                logger.error(f"Task {task_id} failed: {e}")
                return e  # 返回异常对象

        def done_callback(future: Future):
            if not self._shutdown_flag:
                if callback and isinstance(callback, Callable):
                    try:
                        result = future.result()                    
                    except Exception as e:
                        result = e
                    logger.debug(f"callback: {callback} -- 3")
                    if self._callback_direct:
                        callback(result)
                    else:
                        self._callback_queue.put((callback, result))

                with self._lock:
                    self._process_pids.pop(task_id, None)
                    self._running_tasks.pop(task_id, None)
                    self._process_next_queued_task()

        logger.debug(f"callback: {callback} -- 2")
        if False: #is_long_task:
            # 该逻辑也可以使用
            future = self._process_pool.submit(
                        self._run_long_task,
                        task_id, func, args, kwargs,
                        self._process_pids, self._process_lock
                    )
        else:
            future = asyncio.run_coroutine_threadsafe(
                async_wrapper(),
                self._event_loop
            )
        future.add_done_callback(done_callback)
        self._running_tasks[task_id] = future

    def _process_next_queued_task(self) -> None:
        """从队列中取出下一个任务执行"""
        if not self._queued_tasks or len(self._running_tasks) >= self._thread_pool._max_workers:
            return

        # 获取第一个排队任务
        task_id, task_data = next(iter(self._queued_tasks.items()))
        self._queued_tasks.pop(task_id)
        
        self._submit_task(
            task_id,
            task_data['func'],
            task_data['callback'],
            *task_data['args'],
            **task_data['kwargs']
        )

    def process_callbacks(self) -> None:
        """在主线程中处理回调（需要在主线程定期调用）"""
        if self._shutdown_flag:
            raise RuntimeError("Executor is shutting down", self.SHUTDOWN)
        if self._callback_direct:
            return
        while not self._callback_queue.empty():
            callback, result = self._callback_queue.get()
            try:
                callback(result)
            except Exception as e:
                logger.error(f"Callback error: {e}")

    def cancel_task(self, task_id: str) -> bool:
        """取消任务（包括排队中的任务）"""
        with self._lock:
            # 尝试取消排队中的任务
            if task_id in self._queued_tasks:
                self._queued_tasks.pop(task_id)
                return True

            if task_id in self._process_pids:
                try:
                    ProcessTerminator.terminate(self._process_pids[task_id])
                    logger.debug(f"Sent SIGTERM to process {self._process_pids[task_id]}")
                except ProcessLookupError:
                    pass
                finally:
                    self._process_pids.pop(task_id, None)

            # 尝试取消运行中的任务
            future = self._running_tasks.get(task_id)
            if future and not future.done():
                future.cancel()
                self._running_tasks.pop(task_id)
                self._process_next_queued_task()
                return True
            
            return False

    def has_tasks(self) -> int:
        """检查是否有任务在运行或排队中"""
        with self._lock:
            return (self._running_tasks.__len__() + self._queued_tasks.__len__())

    def shutdown(self) -> None:
        """关闭执行器"""
        if self._shutdown_flag:
            return
        logger.info("Shutting down executor...0")
        self._shutdown_flag = True
        with self._lock:
            self._loop_ready.clear()  # 停止事件循环
            logger.info("Kill process tasks.")
            for tid, pid in self._process_pids.items():
                try:
                    ProcessTerminator.terminate(pid)
                    logger.info(f"Terminated process {pid} (task {tid})")
                except ProcessLookupError:
                    pass
            self._process_pids.clear()
            # 取消所有运行中和排队中的任务
            logger.info("Shutting down executing tasks.")
            for future in self._running_tasks.values():
                if not future.done():
                    future.cancel()
            logger.info("Clear running tasks and queued tasks.")
            self._running_tasks.clear()
            self._queued_tasks.clear()
            if not self._callback_direct:
                while not self._callback_queue.empty():
                    self._callback_queue.get()  # 清空回调队列
        logger.info("Stop envent loop.")
        if self._event_loop:
            for task in asyncio.all_tasks(self._event_loop):
                task.cancel()
            self._event_loop.call_soon_threadsafe(self._event_loop.stop)
        logger.info("Shutting down thread pool.")
        self._thread_pool.shutdown(wait=False)
        self._process_pool.shutdown(wait=False)
        self._manager.shutdown()  # 必须显式关闭Manager
        logger.debug("Shutting down executor over")

    def is_task_active(self, task_id: str) -> bool:
        """检查任务是否在运行或排队中"""
        with self._lock:
            return (task_id in self._running_tasks and not self._running_tasks[task_id].done()) or \
                   (task_id in self._queued_tasks)

    import psutil  # pip install psutil

def check_process_alive(pid: int) -> bool:
    try:
        return psutil.Process(pid).is_running()
    except:
        return False

class LongTask:
    def __init__(self):
        self.test = 1
    # 绑定了类或类对象像的函数，不可以被进程池使用，进程池调用的函数依赖可序列化
    @staticmethod
    def long_running_task(seconds: int, task_name: str):  
        for i in range(seconds):
            logger.debug(f"{task_name} working... {i+1}/{seconds}")
            time.sleep(1)
        return f"{task_name} completed"

    # 可以被进程池调用
    @classmethod
    def long_running_task1(cls, seconds: int, task_name: str):
        for i in range(seconds):
            logger.debug(f"{task_name} working... {i+1}/{seconds}")
            time.sleep(1)
    # 不可以被进程池调用，因为self不是可序列化的。在实际调用中，实际上self无法被传递，因此会导致函数调用参数错误，无法执行
    def long_running_task2(self, seconds: int, task_name: str):  
        logger.info(f"self.test: {self.test}") 
        for i in range(seconds):
            logger.debug(f"{task_name} working... {i+1}/{seconds}")
            time.sleep(1)
        return f"{task_name} completed"

# 可以使用
def long_running_task(seconds: int, task_name: str):  
        for i in range(seconds):
            logger.debug(f"{task_name} working... {i+1}/{seconds}")
            time.sleep(1)
        return f"{task_name} completed"
    
# 使用示例
if __name__ == "__main__":
    def task_callback(result):
        if isinstance(result, Exception):
            logger.info(f"! Task failed: {result}")
        else:
            logger.info(f"√ Task success: {result}")

    # 测试初始化
    logger.debug("=== Testing initialization ===")
    executor = SyncAsyncExecutor(max_workers=2, max_queue_size=3)
    logger.debug("Executor initialized successfully")

    # 测试正常任务
    logger.debug("\n=== Testing normal execution ===")
    #executor.execute_async("task1", long_running_task, 5, "Task1", is_long_task=False, callback=task_callback)
    #executor.execute_async("task2", long_running_task, 3, "Task2", is_long_task=False,callback=task_callback)
    executor.execute_async("task3", LongTask.long_running_task, 5, "Task3", is_long_task=True, callback=task_callback)
    executor.execute_async("task4", LongTask.long_running_task, 30, "Task4", is_long_task=True)
    logger.debug(f"is_task_active(task3): {executor.is_task_active("task3")}")

    start = time.time()
    while time.time() - start < 5:
        logger.debug(f"has_tasks(): {executor.has_tasks()}")
        logger.debug(f"is_task_active(task3): {executor.is_task_active("task3")}")
        if executor.is_task_active("task3"):
            pid = executor._process_pids.get("task3")
            if pid:
                status = "alive" if check_process_alive(pid) else "dead"
                logger.debug(f"Process {pid} is {status}")
        
        # executor.process_callbacks()
        time.sleep(1)

    # 处理回调
    start = time.time()
    try:
        while time.time() - start < 5:
            executor.process_callbacks()
            time.sleep(0.1)
    except Exception as e:
        logger.error(f"Error in main loop: {e}")

    logger.info(f"\nPending tasks:{executor.has_tasks()}")    

    # 测试关闭
    print("\n=== Testing shutdown ===")
    executor.shutdown()
    print("Executor shutdown complete")