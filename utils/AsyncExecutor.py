import asyncio
import threading
import concurrent.futures
from typing import Callable, Any, Dict, Optional, Union
from concurrent.futures import Future, ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
from queue import Queue
import logging
import time
from multiprocessing.managers import DictProxy
from multiprocessing import Manager, Lock
import psutil, signal, sys
import dill
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_config import setup_logging

logger = setup_logging(log_level=logging.DEBUG,b_log_file=False)

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

class RunningTasksContainer:
    def __init__(self):
        self._data: Dict[str, Dict[str, Any]] = {}  # 保持 {task_id: {"future": Future, "is_long_task": bool}} 结构

    def __setitem__(self, task_id: str, task_info: Dict[str, Any]):
        """保持原有赋值方式"""
        if not isinstance(task_info, dict) or "future" not in task_info or "is_long_task" not in task_info:
            raise ValueError("Task info must contain 'future' and 'is_long_task'")
        self._data[task_id] = task_info

    def __getitem__(self, task_id: str) -> Dict[str, Any]:
        """保持原有取值方式"""
        return self._data[task_id]

    def __delitem__(self, task_id: str):
        """保持原有删除方式"""
        del self._data[task_id]

    def __contains__(self, task_id: str) -> bool:
        """保持原有in判断"""
        return task_id in self._data

    def get(self, task_id: str, default=None) -> Optional[Dict[str, Any]]:
        """保持原有get方法"""
        return self._data.get(task_id, default)

    def keys(self):
        """保持keys()方法"""
        return self._data.keys()

    def values(self):
        """保持values()方法"""
        return self._data.values()

    def items(self):
        """保持items()方法"""
        return self._data.items()

    def __len__(self) -> int:
        """保持len()方法"""
        return len(self._data)

    @property
    def long_task_count(self) -> int:
        """获取长任务数量"""
        return sum(1 for task in self._data.values() if task["is_long_task"])

    @property
    def short_task_count(self) -> int:
        """获取短任务数量"""
        return sum(1 for task in self._data.values() if not task["is_long_task"])

    def pop(self, task_id: str, default=None) -> Optional[Dict[str, Any]]:
        """保持pop方法"""
        return self._data.pop(task_id, default)
    
    def clear(self):
        """清空容器"""
        self._data.clear()

class CancellationSignal:
    """用于标记任务被取消的特殊对象"""
    pass

class AsyncExecutor:
    SHUTDOWN = 0
    LOOP_NOT_READY = 1
    LOOP_STOPED = 2
    LOOP_READY_TIMEOUT = 3
    TASK_NOT_FOUND = 4
    TASKID_EXITED = 5
    TASK_QUEUED_FILLED = 6
    CALLBACK_NOT_QUEUE = 7
    CHILD_PROCESS_EXCEPTION = 8
    GET_RESULT_ERROR = 9

    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance    

    def __init__(self, *, max_workers: int = int((os.cpu_count()*2)//3), 
                        max_processes: int = 1,
                        max_queue_size: int = 10, 
                        callback_direct: bool = True):
        if self._initialized:
            return
        self._thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self._process_pool = ProcessPoolExecutor(max_workers=max_processes)
        #self._running_tasks: Dict[str, Dict[Future, bool]] = {}
        self._running_tasks: RunningTasksContainer = RunningTasksContainer()
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

        self._latest_task:Dict = None  # 最近提交的任务ID
        self._notify_processing = None

        self._initialized = True
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
            raise RuntimeError("Failed to start event loop", self.LOOP_READY_TIMEOUT)

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
        logger.info(f"execute_async called with task_id: {task_id}")
        if self._shutdown_flag:
            logger.error("Executor is shutting down")
            return False
        
        with self._lock:            
            if task_id in self._running_tasks or task_id in self._queued_tasks:
                logger.error(f"Task {task_id} already exists")
                return False
            logger.debug(f"_thread_pool._max_workers: {self._thread_pool._max_workers}")
            logger.debug(f"_process_pool._max_workers: {self._process_pool._max_workers}")
            logger.debug(f"_max_queue_size: {self._max_queue_size}")
            logger.debug(f"_running_tasks.short_task_count: {self._running_tasks.short_task_count}")
            logger.debug(f"_running_tasks.long_task_count: {self._running_tasks.long_task_count}")
            if any([
                    (not is_long_task and (self._running_tasks.short_task_count >= self._thread_pool._max_workers)),
                    (is_long_task and (self._running_tasks.long_task_count >= self._process_pool._max_workers))]
                ):
                if len(self._queued_tasks) >= self._max_queue_size:
                    logger.error(f"Task queue is full, _max_queue_size: {self._max_queue_size}")
                    return False
                
                logger.info(f"Task {task_id} queued, current queue size: {len(self._queued_tasks)}")
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
            logger.info(f"Executing task {task_id} immediately")
            try:
                self._submit_task(task_id, func, is_long_task, callback, *args, **kwargs)
            except Exception as e:
                logger.error(f"Failed to submit task {task_id}: {e.args}")
                return False
            return True

    def set_notify_processing(self, notify_func: Callable[[str], None]):
        """设置通知函数"""
        with self._lock:
            self._notify_processing = notify_func

    @staticmethod
    def _run_long_task(tid: str, func: Callable, args: tuple, kwargs: dict,
                      pids: DictProxy, lock: Any) -> Any:
        """静态方法确保可序列化"""
        pid = os.getpid()
        logger.info(f"Running long task {tid} in process {pid}")
        logger.debug(f"---func: {str(func)}")
        try:
            with lock:
                pids[tid] = pid
            
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Task: {tid} failed: {e.args}")
            logger.error(traceback.format_exc())
            return e
        finally:
            with lock:
                pids.pop(tid, None)

    def _done_callback(self, task_id: str, callback: Callable, future: Future):
        current_thread = threading.current_thread()
        logger.debug(f"Callback executing in thread: {current_thread.name}")

        try:
            result = future.result()
            logger.debug(f"normal result: {result}")
        except Exception as e:
            result = e#RuntimeError(e)
        
        result_type = None
        if isinstance(result, Exception):
            if isinstance(result, (concurrent.futures.CancelledError, asyncio.CancelledError)):
                logger.debug(f"Task: {task_id} is cancelled")
                #result_type = CancellationSignal()  # 特殊标记对象
            elif isinstance(result, concurrent.futures.process.BrokenProcessPool):                
                # self._restart_process_pool()
                #result_type = CancellationSignal()
                pass
            result = RuntimeError(result)
            logger.error(f"Task: {task_id} occur exception, args: {result.args}")
        logger.info(f"Task: {task_id} completed with result: {result}")
        
        if not self._shutdown_flag:
            with self._lock:
                if self._latest_task and self._latest_task['task_id'] == task_id:
                    self._latest_task = None
                exist_id = task_id in self._running_tasks
                is_callback = self._callback_direct                
                if not self._callback_direct:
                    self._callback_queue.put((callback, result))
                future = self._running_tasks.get(task_id, {}).get('future')
                if future and future.done():
                    self._cleanup_task(task_id)
                    if not isinstance(result_type, CancellationSignal):
                        logger.info(f"Task: {task_id} completed, precessing next queued task")
                        self._process_next_queued_task()

            if all([callback and isinstance(callback, Callable),
                    exist_id and is_callback
                    ]):
                callback(result)            

    def _cleanup_task(self, task_id: str):
        """清理任务资源的公共方法"""        
        self._process_pids.pop(task_id, None)
        self._running_tasks.pop(task_id, None)

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

        async def async_wrapper():
            try:
                if is_long_task: 
                    logger.debug(f"To run long task {task_id} ")
                    try:
                        result = await self._event_loop.run_in_executor(
                            self._process_pool,
                            self._run_long_task,  # 使用静态方法
                            task_id, func, args, kwargs,
                            self._process_pids, self._process_lock
                        )
                    except concurrent.futures.process.BrokenProcessPool as e:
                        logger.error(f"Task: {task_id} failed.Process pool broken, going to restart ...")
                        #self._restart_process_pool()
                        result = e
                else:
                    result = await self._event_loop.run_in_executor(
                        executor,
                        partial(func, *args, **kwargs)
                    )
                return result
            except Exception as e:
                    logger.error(f"Task: {task_id} failed: {e}")
                    return e

        if False: #is_long_task:
            # 该逻辑也可以使用。回调函数在调用线程中执行
            future = self._process_pool.submit(
                        self._run_long_task,
                        task_id, func, args, kwargs,
                        self._process_pids, self._process_lock
                    )
        else:
            # 使用协程获取future好处：任务执行完之后，回调函数是在协程的队列线程中执行
            future = asyncio.run_coroutine_threadsafe(
                async_wrapper(),
                self._event_loop
            )
        self._latest_task = {
                    'task_id': task_id,
                    'func': func,
                    'args': args,
                    'kwargs': kwargs,
                    'callback': callback,
                    'is_long_task': is_long_task
                    }
        # partial(self._done_callback, task_id, callback) 和使用lambda的效果是一样的
        # 但是使用partial可以避免lambda的闭包问题，同时也可以传递参数
        future.add_done_callback(partial(self._done_callback, task_id, callback))
        self._running_tasks[task_id] = {"future": future, "is_long_task": is_long_task}
        self._notify_processing(task_id)

    def _restart_process_pool(self):
        logger.warning("Restarting process pool due to broken state")
        self._process_pool.shutdown()
        self._process_pool = ProcessPoolExecutor(
            max_workers=self._process_pool._max_workers
        )
        for task_id in list(self._process_pids.keys()):
            self._process_pids.pop(task_id, None)
            self._running_tasks.pop(task_id, None)

    def _process_next_queued_task(self) -> None:
        """ 如果最近提交的任务执行成功，并且队列中还有任务，那么立即执行队列中的任务 
        注意：
            1. 最近提交的任务执行成功，并且队列中还有任务，那么立即执行队列中的任务
            2. 最近提交的任务执行失败，并且队列中还有任务，那么不执行队列中的任务，重新提交最近的任务
        """
        logger.info(f"Processing next queued task, current queue size: {len(self._queued_tasks)}")

        """ 根据实际测试，任务提交失败也会回到执行_done_callback, 所以这里不需要判断任务是否执行成功 """
        """
        task_id = None
        if self._latest_task:
            task_id = self._latest_task.get('task_id', None)
            is_long_task = self._latest_task.get('is_long_task', False)
            if task_id:
                if is_long_task:
                    if self._process_pids.get(task_id, None):
                        self._latest_task = None
                else:
                    self._latest_task = None

        if any([
            not self._queued_tasks,
            len(self._queued_tasks) == 0
        ]) and not self._latest_task:
            logger.info(f"Queue is empty, no task to process")
            return
        
        if not self._latest_task:
            # 获取第一个排队任务
            task_id, task_data = next(iter(self._queued_tasks.items()))
        else:
            task_id = self._latest_task.get('task_id', None)
            task_data = self._latest_task
        """
        if any([
            not self._queued_tasks,
            len(self._queued_tasks) == 0
        ]) and not self._latest_task:
            logger.info(f"Queue is empty, no task to process")
            return
        task_id, task_data = next(iter(self._queued_tasks.items()))

        callback = task_data.get('callback')
        is_long_task = task_data.get('is_long_task')

        if any([
            is_long_task and len(self._process_pids) >= self._process_pool._max_workers,
            not is_long_task and len(self._running_tasks) >= self._thread_pool._max_workers
        ]):
            logger.info(f"Execut unit is full, need to wait for some time")
            return

        if self._process_pool._broken:  # 检查进程池是否健康
            self._restart_process_pool()
        
        try:
            self._submit_task(
                task_id,
                task_data['func'],
                task_data['is_long_task'],
                callback,  # 显式传递callback
                *task_data['args'],
                **task_data['kwargs']
            )
        except Exception as e:
            logger.error(f"Failed to submit queued task {task_id}: {e}")
        # 留作测试ansync中未捕获异常如何输出到日志文件
        # self._queued_tasks.pop(task_id)
        # self._queued_tasks.pop(task_id)
        if task_id in self._queued_tasks:
            self._queued_tasks.pop(task_id, None)

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

    def _future_cancle(self, future: Future, timeout: float) -> bool:
        """取消任务（包括排队中的任务）"""
        cancel_result = {'success': False, 'done': False}
        def _cancel():
            try:
                cancel_result['success'] = future.cancel()
            except Exception as e:
                logger.error(f"Cancel failed for {task_id}: {str(e)}")
            finally:
                cancel_result['done'] = True

        cancel_thread = threading.Thread(target=_cancel, daemon=True)
        cancel_thread.start()
        cancel_thread.join(timeout=timeout)  # 设置超时
        if not cancel_result['done']:  # 超时情况
            logger.warning("Cancel operation timed out")
            return False
            
        return cancel_result['success']

    def cancel_task(self, task_id: str, timeout: float=1.0) -> bool:
        """取消任务（包括排队中的任务）"""
        with self._lock:
            if self._latest_task and self._latest_task.get('task_id') == task_id:
                self._latest_task = None
            # 1. 尝试取消排队中的任务（无论长短任务）
            if task_id in self._queued_tasks:
                logger.info(f"Cancelling queued task: {task_id}")
                is_long = self._queued_tasks[task_id]['is_long_task']
                self._queued_tasks.pop(task_id)
                if is_long:
                    logger.debug(f"Removed long task {task_id} from queue")
                else:
                    logger.debug(f"Removed short task {task_id} from queue")
                return True

            future = self._running_tasks.get(task_id, {}).get('future', None)
            exist_pid = self._process_pids.get(task_id, None)
            is_long_task = self._running_tasks.get(task_id, {}).get('is_long_task', False)
        
        if future is None:
            logger.warning(f"Task {task_id} not found")
            return False
        
        if future.done():
            logger.info(f"Task {task_id} already completed")
            return False

        # 2. 处理进程任务（长任务）
        cancle_result = False
        if exist_pid:
            # 逻辑上分析，进程被直接kill之后，进程future会自动done
            # 如果进程池进入broken状态，自动重启也不会受到影响
            #if future and not future.done():
            #    cancle_result = self._future_cancle(future, timeout)
            
            pid = exist_pid
            logger.info(f"Terminating process task {task_id} (PID: {pid})")
            try_count = 0
            while self.check_process_alive(pid) and try_count < 1:
                try:
                    if ProcessTerminator.terminate(pid):
                        logger.info(f"Process {pid} terminated")
                        """
                        if self._process_pool._broken:  # 检查进程池是否健康
                            self._restart_process_pool()
                        """
                    else:
                        logger.warning(f"Failed to terminate process {pid}")
                except Exception as e:
                    logger.error(f"Termination error: {e}")
                time.sleep(0.3)  # 等待进程终止
                try_count += 1

        # 3. 检查运行中的短任务（线程池）
        cancle_result = future.done()
        if future and not future.done(): 
            logger.info(f"Cancelling task: {task_id}")
            cancle_result = self._future_cancle(future, timeout)

        if cancle_result:
            with self._lock: 
                if future.done():
                    logger.info(f"Task {task_id} completed after cancellation")
                    self._running_tasks.pop(task_id, None)
                    """ cancle成功后，会调用_done_callback,在回调中提交下一个任务 """
                    # self._process_next_queued_task()
                if not self.check_process_alive(exist_pid):
                    logger.info(f"Process {exist_pid} terminated after cancellation")
                    self._process_pids.pop(task_id, None)
                return True
        
        if not is_long_task:
            logger.info(f"Short task must not cancel succeed, task: {task_id}")
            return True

        logger.warning(f"Task {task_id} not found or already completed")
        return False

    def has_tasks(self) -> int:
        """检查是否有任务在运行或排队中"""
        with self._lock:
            return (self._running_tasks.__len__() + self._queued_tasks.__len__())

    def get_task_status(self):
        with self._lock:
            return {
                'running': list(self._running_tasks.keys()),
                'queued': list(self._queued_tasks.keys()),
                'processes': dict(self._process_pids)
            }
    
    def is_queued(self, task_id: str) -> bool:
        """检查任务是否在队列中"""
        with self._lock:
            return task_id in self._queued_tasks
    
    def is_all_tasks_done(self) -> bool:
        with self._lock:
            return (self._running_tasks.__len__() == 0 and self._queued_tasks.__len__() == 0)

    def get_running_process_count(self) -> int:
        """获取当前运行中的进程数量"""
        with self._process_lock:
            return len(self._process_pids)
    
    def get_queued_task_count(self)-> int:
        """获取当前排队中的任务数量"""
        with self._lock:
            return len(self._queued_tasks)

    def set_concurrency(self, max_workers: int, max_processes: int):
        self._thread_pool._max_workers = max_workers
        self._process_pool._max_workers = max_processes

    def shutdown(self) -> None:
        """关闭执行器"""
        logger.debug(f"----pid: {os.getpid()}")
        if self._shutdown_flag:
            return
        logger.info("Shutting down executor...0")
        self._shutdown_flag = True
        
        with self._lock:
            self._latest_task = None
            self._queued_tasks.clear()            
            self._loop_ready.clear()  # 停止事件循环

            logger.info("Kill all processing tasks.")
            for tid, pid in self._process_pids.items():
                try:
                    ProcessTerminator.terminate(pid)
                    logger.info(f"Terminated process {pid} (task {tid})")
                except ProcessLookupError:
                    pass
            # 取消所有运行中和排队中的任务
            logger.info("Cancle executing tasks.")
            for value in self._running_tasks.values():
                future = value.get('future', None)
                if future and not future.done():
                    self._future_cancle(future, 1.0)
            
            self._process_pids.clear()            
            logger.info("Clear running tasks and queued tasks.")
            self._running_tasks.clear()

            logger.info("Stop envent loop.")            
            if self._event_loop:
                for task in asyncio.all_tasks(self._event_loop):
                    task.cancel()
            self._event_loop.call_soon_threadsafe(self._event_loop.stop)
            
            if not self._callback_direct:
                while not self._callback_queue.empty():
                    self._callback_queue.get()  # 清空回调队列
        
        logger.info("Shutting down thread pool.")
        self._thread_pool.shutdown(wait=False)
        logger.info("Shutting down process pool.")
        self._process_pool.shutdown(wait=False)
        logger.info("Shutting down share args.")
        self._manager.shutdown()  # 必须显式关闭Manager
        logger.debug("Shutting down executor over")

    def is_task_active(self, task_id: str) -> bool:
        """检查任务是否在运行或排队中"""
        with self._lock:
            future = self._running_tasks.get(task_id, {}).get('future')
            pid = self._process_pids.get(task_id, None)
            queued = task_id in self._queued_tasks

        if queued:
            return True

        # 检查运行中任务
        if future and not future.done():
            return True
        
        # 检查进程任务
        if pid:
            return self.check_process_alive(pid)

    @staticmethod
    def check_process_alive(pid: int) -> bool:
        try:
            return psutil.Process(pid).is_running()
        except:
            return False

# just for testing,非正式代码
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

# just for testing,非正式代码
# 可以使用
def long_running_task(seconds: int, task_name: str):  
        for i in range(seconds):
            logger.debug(f"{task_name} working... {i+1}/{seconds}")
            time.sleep(1)
        return f"{task_name} completed"
       
def reliable_task(seconds, name, comm_file):
    """使用文件系统通信的任务"""
    # 标记进程启动
    with open(comm_file, 'w') as f:
        logger.info(f"Process {name} started")  # 记录进程启动
        f.write("STARTED")
    
    try:
        for i in range(seconds):
            # 检查停止信号
            if os.path.exists(comm_file + '.stop'):
                logger.info(f"{name} received stop signal")
                return f"{name} terminated early"
            time.sleep(0.1)
        return f"{name} completed"
    finally:
        os.remove(comm_file)  # 清理
    
# 使用示例
if __name__ == "__main__":
    def task_callback(result):
        if isinstance(result, Exception):
            logger.info(f"! Task failed: {result}")
        else:
            logger.info(f"√ Task success: {result}")

    # 测试初始化
    logger.debug("=== Testing initialization ===")
    executor = AsyncExecutor(max_workers=2, max_queue_size=3)
    logger.debug("Executor initialized successfully")

    executor.set_concurrency(max_workers=3, max_processes=2)
    # 测试正常任务
    logger.debug("\n=== Testing normal execution ===")
    #executor.execute_async("task1", long_running_task, 100, "Task1", is_long_task=False, callback=task_callback)
    #executor.execute_async("task2", long_running_task, 100, "Task2", is_long_task=False,callback=task_callback)
    #executor.execute_async("task21", long_running_task, 6, "Task21", is_long_task=False,callback=task_callback)
    #executor.execute_async("task22", long_running_task, 8, "Task22", is_long_task=False,callback=task_callback)
    #executor.execute_async("task3", LongTask.long_running_task, 5, "Task3", is_long_task=True, callback=task_callback)
    executor.execute_async("task4", LongTask.long_running_task, 100, "Task4", is_long_task=True, callback=task_callback)
    executor.execute_async("task5", LongTask.long_running_task, 100, "Task5", is_long_task=True)
    executor.execute_async("task6", LongTask.long_running_task, 100, "Task6", is_long_task=True, callback=task_callback)
    #logger.debug(f"is_task_active(task3): {executor.is_task_active("task3")}")

     # 提交任务
    comm_file = os.path.join(os.getcwd(), "proc_comm_test.txt")
    #success = executor.execute_async("to_terminate", reliable_task, 10, "Terminate Test", comm_file, is_long_task=True)
    #logger.debug(f"submit task success: {success}")

    target_task_id = "task4"
    start = time.time()
    while time.time() - start < 5:
        logger.debug(f"has_tasks(): {executor.has_tasks()}")
        logger.debug(f"is_task_active({target_task_id}): {executor.is_task_active(target_task_id)}")
        if executor.is_task_active(target_task_id):
            pid = executor._process_pids.get(target_task_id)
            if pid:
                status = "alive" if executor.check_process_alive(pid) else "dead"
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
    executor.cancel_task("task4")

    time.sleep(30)

    # 测试关闭
    logger.debug("\n=== Testing shutdown ===")
    executor.shutdown()
    logger.debug("Executor shutdown complete")

# 模块级别的代码包含直接创建多进程的代码（如Manager()），就会导致递归创建进程
# 因此，以下代码不可以在模块级别的代码中使用，否则会导致递归创建进程
# AsyncExecutor(max_workers=int((os.cpu_count()*2)//3), max_processes=1, max_queue_size=10)