import unittest
import time
import logging
from typing import Callable, Any
from concurrent.futures import Future
from unittest.mock import patch, MagicMock
from AsyncExecutor import AsyncExecutor, LongTask, long_running_task

class TestAsyncExecutor(unittest.TestCase):
    def setUp(self):
        # 设置一个基础的执行器
        self.executor = AsyncExecutor(max_workers=2, max_processes=2, max_queue_size=2)
        self.callback_results = []
        
    def tearDown(self):
        self.executor.shutdown()
        time.sleep(0.1)  # 确保资源完全释放

    def test_initialization(self):
        """测试执行器初始化"""
        self.assertIsNotNone(self.executor._thread_pool)
        self.assertIsNotNone(self.executor._process_pool)
        self.assertEqual(self.executor._thread_pool._max_workers, 2)
        self.assertEqual(self.executor._process_pool._max_workers, 2)
        self.assertTrue(self.executor._loop_ready.is_set())

    def test_execute_short_task(self):
        """测试执行短任务"""
        def callback(result):
            self.callback_results.append(result)
        
        success = self.executor.execute_async(
            "test_short", 
            lambda: "short task done", 
            is_long_task=False, 
            callback=callback
        )
        self.assertTrue(success)
        
        # 等待任务完成
        while self.executor.has_tasks():
            time.sleep(0.1)
        
        self.assertEqual(len(self.callback_results), 1)
        self.assertEqual(self.callback_results[0], "short task done")

    def test_execute_long_task(self):
        """测试执行长任务"""
        def callback(result):
            self.callback_results.append(result)
        
        success = self.executor.execute_async(
            "test_long", 
            long_running_task, 
            1, "Test Long Task", 
            is_long_task=True, 
            callback=callback
        )
        self.assertTrue(success)
        
        # 检查进程PID是否被记录
        time.sleep(0.5)
        self.assertIn("test_long", self.executor._process_pids)
        
        # 等待任务完成
        while self.executor.has_tasks():
            time.sleep(0.1)
        
        self.assertEqual(len(self.callback_results), 1)
        self.assertIn("Test Long Task completed", self.callback_results[0])

    def test_task_queueing(self):
        """测试任务排队机制"""
        # 填充线程池
        for i in range(2):
            self.executor.execute_async(
                f"fill_{i}", 
                lambda: time.sleep(0.5), 
                is_long_task=False, 
                callback=None
            )
        
        # 这些任务应该进入队列
        queued1 = self.executor.execute_async(
            "queued1", 
            lambda: "queued1", 
            is_long_task=False, 
            callback=None
        )
        queued2 = self.executor.execute_async(
            "queued2", 
            lambda: "queued2", 
            is_long_task=False, 
            callback=None
        )
        
        self.assertTrue(queued1)
        self.assertTrue(queued2)
        self.assertEqual(len(self.executor._queued_tasks), 2)
        
        # 这个任务应该被拒绝，因为队列已满
        rejected = self.executor.execute_async(
            "rejected", 
            lambda: "rejected", 
            is_long_task=False, 
            callback=None
        )
        self.assertFalse(rejected)

    def test_task_cancellation(self):
        """测试任务取消"""
        # 提交一个长时间运行的任务
        self.executor.execute_async(
            "to_cancel", 
            lambda: time.sleep(10), 
            is_long_task=True, 
            callback=None
        )
        
        # 确保任务已启动
        time.sleep(0.5)
        self.assertTrue(self.executor.is_task_active("to_cancel"))
        
        # 取消任务
        cancelled = self.executor.cancel_task("to_cancel")
        self.assertTrue(cancelled)
        self.assertFalse(self.executor.is_task_active("to_cancel"))

    def test_process_callbacks(self):
        """测试回调处理"""
        def callback(result):
            self.callback_results.append(result)
        
        # 使用间接回调模式
        executor = AsyncExecutor(callback_direct=False)
        
        executor.execute_async(
            "callback_test", 
            lambda: "callback data", 
            is_long_task=False, 
            callback=callback
        )
        
        # 等待任务完成
        while executor.has_tasks():
            time.sleep(0.1)
        
        # 处理回调
        executor.process_callbacks()
        self.assertEqual(len(self.callback_results), 1)
        self.assertEqual(self.callback_results[0], "callback data")
        
        executor.shutdown()

    def test_shutdown(self):
        """测试关闭执行器"""
        # 提交一些任务
        for i in range(3):
            self.executor.execute_async(
                f"shutdown_test_{i}", 
                lambda: time.sleep(1), 
                is_long_task=(i % 2 == 0), 
                callback=None
            )
        
        # 立即关闭
        self.executor.shutdown()
        
        # 检查所有任务是否已清理
        self.assertEqual(self.executor.has_tasks(), 0)
        self.assertEqual(len(self.executor._process_pids), 0)

    def test_serialization_error(self):
        """测试不可序列化任务的错误处理"""
        def callback(result):
            self.callback_results.append(result)
        
        # 创建一个不可序列化的lambda
        bad_func = lambda x: x + 1
        bad_func.__name__ = "<lambda>"
        
        success = self.executor.execute_async(
            "bad_task", 
            bad_func, 
            1, 
            is_long_task=True,  # 长任务需要可序列化
            callback=callback
        )
        self.assertTrue(success)
        
        # 等待错误发生
        while not self.callback_results:
            time.sleep(0.1)
        
        self.assertIsInstance(self.callback_results[0], Exception)
        self.assertIn("not serializable", str(self.callback_results[0]))

    def test_task_status(self):
        """测试获取任务状态"""
        # 提交一些任务
        self.executor.execute_async("status1", lambda: time.sleep(0.1), is_long_task=False)
        self.executor.execute_async("status2", lambda: time.sleep(0.1), is_long_task=False)
        self.executor.execute_async("status3", lambda: time.sleep(0.1), is_long_task=False)  # 应该排队
        
        status = self.executor.get_task_status()
        self.assertEqual(len(status['running']), 2)
        self.assertEqual(len(status['queued']), 1)
        
        # 等待任务完成
        while self.executor.has_tasks():
            time.sleep(0.1)
        
        status = self.executor.get_task_status()
        self.assertEqual(len(status['running']), 0)
        self.assertEqual(len(status['queued']), 0)

    def test_class_method_task(self):
        """测试类方法作为任务"""
        def callback(result):
            self.callback_results.append(result)
        
        success = self.executor.execute_async(
            "class_method", 
            LongTask.long_running_task1, 
            1, "Class Method Task", 
            is_long_task=True, 
            callback=callback
        )
        self.assertTrue(success)
        
        # 等待任务完成
        while not self.callback_results:
            time.sleep(0.1)
        
        self.assertEqual(len(self.callback_results), 1)

    @patch('AsyncExecutor.ProcessTerminator.terminate')
    def test_process_termination(self, mock_terminate):
        """测试进程终止"""
        mock_terminate.return_value = True
        
        # 提交一个长任务
        self.executor.execute_async(
            "to_terminate", 
            long_running_task, 
            10, "Terminate Test", 
            is_long_task=True
        )
        
        # 确保进程已启动
        time.sleep(0.5)
        self.assertTrue(self.executor.is_task_active("to_terminate"))
        
        # 终止任务
        pid = self.executor._process_pids["to_terminate"]
        cancelled = self.executor.cancel_task("to_terminate")
        self.assertTrue(cancelled)
        
        # 检查terminate是否被调用
        mock_terminate.assert_called_once_with(pid)
        self.assertNotIn("to_terminate", self.executor._process_pids)

if __name__ == '__main__':
    unittest.main()