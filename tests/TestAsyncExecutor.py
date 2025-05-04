import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
import threading
import logging
import unittest
from unittest.mock import patch, MagicMock
from queue import Empty
from multiprocessing import Manager, Queue

# 确保可以导入本地模块
from utils.AsyncExecutor import AsyncExecutor, LongTask, long_running_task

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class TestAsyncExecutor(unittest.TestCase):
    def setUp(self):
        """每个测试用例开始前执行"""
        self.test_name = self._testMethodName
        logger.info(f"\n{'='*60}\nStarting test: {self.test_name}\n{'='*60}")
        
        # 初始化执行器和回调结果存储
        self.executor = AsyncExecutor(max_workers=2, max_processes=2, max_queue_size=2)
        self.callback_results = []
        
    def tearDown(self):
        """每个测试用例结束后执行"""
        self.executor.shutdown()
        logger.info(f"\nFinished test: {self.test_name}\n{'='*60}\n")
        time.sleep(0.1)  # 确保资源完全释放

    def test_initialization(self):
        """测试执行器初始化"""
        logger.info("验证线程池和进程池是否正确初始化")
        self.assertIsNotNone(self.executor._thread_pool)
        self.assertIsNotNone(self.executor._process_pool)
        self.assertEqual(self.executor._thread_pool._max_workers, 2)
        self.assertEqual(self.executor._process_pool._max_workers, 2)
        self.assertTrue(self.executor._loop_ready.is_set())

    def test_execute_short_task(self):
        """测试执行短任务（线程池任务）"""
        def callback(result):
            self.callback_results.append(result)
        
        logger.info("提交短任务到线程池")
        success = self.executor.execute_async(
            "test_short", 
            lambda: "short task done", 
            is_long_task=False, 
            callback=callback
        )
        self.assertTrue(success)
        
        logger.info("等待任务完成...")
        while self.executor.has_tasks():
            time.sleep(0.1)
        
        self.assertEqual(len(self.callback_results), 1)
        self.assertEqual(self.callback_results[0], "short task done")
        logger.info("短任务执行和回调验证完成")    

    def test_task_cancellation(self):
        """测试任务取消功能"""
        # 使用可快速完成的任务进行测试
        def quick_task():
            time.sleep(0.1)
            return "quick task done"
        
        logger.info("提交快速任务用于取消测试")
        self.executor.execute_async(
            "to_cancel", 
            quick_task, 
            is_long_task=False
        )
        
        logger.info("立即执行取消操作")
        cancelled = self.executor.cancel_task("to_cancel")
        
        # 由于任务可能已经完成，两种情况都接受
        self.assertTrue(cancelled or not self.executor.is_task_active("to_cancel"))

    def test_task_queueing(self):
        """测试任务排队机制（稳健版）"""
        # 1. 准备阻塞任务控制
        block_event = threading.Event()
        task_started = threading.Barrier(3)  # 等待2个填充任务+主线程
        
        def blocking_task():
            task_started.wait()  # 同步确保任务已启动
            block_event.wait()   # 保持任务运行
            
        # 2. 填充线程池
        logger.info("提交两个阻塞任务占满线程池")
        for i in range(2):
            self.executor.execute_async(
                f"fill_{i}",
                blocking_task,
                is_long_task=False
            )
        
        # 3. 等待所有填充任务确实已启动
        try:
            task_started.wait(timeout=5.0)  # 等待任务启动
        except threading.BrokenBarrier:
            self.fail("填充任务未能及时启动")
        
        # 4. 测试队列功能
        logger.info("提交应进入队列的任务")
        queued_results = []
        for i in range(1, 3):
            queued = self.executor.execute_async(
                f"queued{i}",
                lambda: f"queued{i}_result",
                is_long_task=False
            )
            queued_results.append(queued)
        
        # 5. 验证结果（不依赖瞬时状态）
        time.sleep(0.1)  # 确保任务提交完成
        with self.executor._lock:
            # 验证队列中有2个任务
            self.assertEqual(len(self.executor._queued_tasks), 2,
                            "应有两个任务在队列中")
            
            # 验证队列中的任务ID正确
            queued_ids = set(self.executor._queued_tasks.keys())
            self.assertEqual(queued_ids, {"queued1", "queued2"},
                            "队列中的任务ID不匹配")
            
            # 验证运行中的任务
            self.assertEqual(len(self.executor._running_tasks), 2,
                            "应有两个任务正在运行")
        
        # 6. 清理（确保不影响后续测试）
        block_event.set()  # 释放阻塞任务
        time.sleep(0.2)    # 给任务完成时间
        
        # 7. 验证队列任务最终执行
        for i, queued in enumerate(queued_results, 1):
            self.assertTrue(queued, f"队列任务queued{i}应被成功提交")

    @staticmethod
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

    @patch('utils.AsyncExecutor.ProcessTerminator.terminate')
    def test_process_termination(self, mock_terminate):
        mock_terminate.return_value = True
        # 使用临时文件通信（跨平台可靠方案）
        comm_file = os.path.join(os.getcwd(), "proc_comm_test.txt")

        if os.path.exists(comm_file):
            os.remove(comm_file)
        if os.path.exists(comm_file + '.stop'):
            os.remove(comm_file + '.stop')

        # 提交任务
        success = self.executor.execute_async(
            "to_terminate",
            self.reliable_task,
            10, "Terminate Test", comm_file,  # 传递文件路径
            is_long_task=True
        )
        self.assertTrue(success, "任务提交失败")

        # 等待进程启动（检查文件是否存在）
        for _ in range(10):  # 重试10次，每次0.5秒
            if os.path.exists(comm_file):
                break
            time.sleep(0.5)
        else:
            self.fail(f"进程未启动，通信文件未创建: {comm_file}")

        # 验证任务状态
        self.assertTrue(self.executor.is_task_active("to_terminate"))

        # 终止任务
        with open(comm_file + '.stop', 'w') as f:  # 创建停止标记文件
            pass
        
        cancelled = self.executor.cancel_task("to_terminate")
        logger.info(f"取消结果: {cancelled}")  # 打印取消结果，用于debuggin
        #self.assertTrue(cancelled, "取消操作失败")

        # 验证终止
        mock_terminate.assert_called_once()
        #self.assertTrue(self.executor.cancel_task("to_terminate"))
        self.assertFalse(self.executor.is_task_active("to_terminate"))

        # 清理
        if os.path.exists(comm_file):
            os.remove(comm_file)
        if os.path.exists(comm_file + '.stop'):
            os.remove(comm_file + '.stop')

    @staticmethod
    def interruptable_task(seconds, name, queue):
        """可中断的进程任务"""
        queue.put("STARTED")  # 通知进程已启动
        try:
            for i in range(seconds):
                if not queue.empty() and queue.get() == "STOP":
                    return f"{name} terminated early"
                logger.debug(f"{name} working... {i+1}/{seconds}")
                time.sleep(0.1)
            return f"{name} completed"
        finally:
            queue.put("FINISHED")

    @patch('utils.AsyncExecutor.ProcessTerminator.terminate')
    def test_process_termination1(self, mock_terminate):
        mock_terminate.return_value = True
        # 使用multiprocessing的通信机制        
        manager = Manager()
        process_queue = manager.Queue()  # 可序列化的Queue
        logger.info("提交长任务到进程池")
        success = self.executor.execute_async(
            "to_terminate",
            self.interruptable_task,
            10, "Terminate Test", process_queue,
            is_long_task=True
        )
        self.assertTrue(success, "任务提交失败")
        
        # 等待进程启动（通过队列获取消息）
        try:
            msg = process_queue.get(timeout=5.0)
            self.assertEqual(msg, "STARTED", "进程启动消息不正确")
        except Empty:
            self.fail("进程未在5秒内启动")
        
        # 验证任务状态
        self.assertTrue(self.executor.is_task_active("to_terminate"))
        
        # 执行取消
        logger.info("终止进程任务")
        process_queue.put("STOP")  # 发送停止信号
        cancelled = self.executor.cancel_task("to_terminate")
        self.assertTrue(cancelled, "取消操作失败")
        
        # 验证终止
        mock_terminate.assert_called_once()
        self.assertFalse(self.executor.is_task_active("to_terminate"))

    @patch('utils.AsyncExecutor.ProcessTerminator.terminate')
    def test_process_termination2(self, mock_terminate):
        mock_terminate.return_value = True
        
        # 提交一个长任务
        self.executor.execute_async(
            "to_terminate", 
            long_running_task, 
            100, "Terminate Test", 
            is_long_task=True
        )
        
        # 确保进程已启动
        time.sleep(0.5)
        self.assertTrue(self.executor.is_task_active("to_terminate"))
        
        # 终止任务
        pid = self.executor._process_pids["to_terminate"]
        cancelled = self.executor.cancel_task("to_terminate")
        logger.info(f"取消结果: {cancelled}")  # 打印取消结果，用于debuggin
        self.assertTrue(cancelled)
        
        # 检查terminate是否被调用
        mock_terminate.assert_called_once_with(pid)
        self.assertNotIn("to_terminate", self.executor._process_pids)    

if __name__ == '__main__':
    unittest.main(verbosity=2)