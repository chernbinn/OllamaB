import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.AsyncExecutor import AsyncExecutor, LongTask, long_running_task

import time
from queue import Empty
from multiprocessing import Manager, Queue

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SpecialAsyncTest:
    def __init__(self):
        self.executor = AsyncExecutor(max_workers=2, max_processes=1, max_queue_size=5)
    
    def terminated(self):
        self.executor.shutdown()

    @staticmethod
    def reliable_task(seconds, name, comm_file):
            """使用文件系统通信的任务"""
            # 标记进程启动
            with open(comm_file, 'w') as f:
                logger.debug(f"Process {name} started")  # 记录进程启动
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

    def test_process_termination1(self):
            
        # 使用临时文件通信（跨平台可靠方案）
        comm_file = os.path.join(os.getcwd(), "proc_comm_test.txt")  
        # 提交任务
        success = self.executor.execute_async(
            "to_terminate",
            #SpecialAsyncTest.reliable_task,
            self.reliable_task,
            10, "Terminate Test", comm_file,  # 传递文件路径
            is_long_task=True
        )
        assert success, "任务提交失败"

        # 等待进程启动（检查文件是否存在）
        for _ in range(20):  # 重试10次，每次0.5秒
            if os.path.exists(comm_file):
                break
            time.sleep(0.5)
        else:
            assert os.path.exists(comm_file), f"进程未启动，通信文件未创建: {comm_file}"

        # 验证任务状态
        assert self.executor.is_task_active("to_terminate"), "任务未处于活跃状态"

        # 终止任务
        with open(comm_file + '.stop', 'w') as f:  # 创建停止标记文件
            pass
        
        cancelled = self.executor.cancel_task("to_terminate")
        assert cancelled, "取消操作失败"

        # 验证终止
        #mock_terminate.assert_called_once()
        #self.assertTrue(self.executor.cancel_task("to_terminate"))
        assert not self.executor.is_task_active("to_terminate"), "任务仍处于活跃状态"

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

    def test_process_termination(self):    
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
        assert success, "任务提交失败"
        
        # 等待进程启动（通过队列获取消息）
        try:
            msg = process_queue.get(timeout=5.0)
            assert msg == "STARTED", "进程启动消息不正确"
        except Empty:
            assert msg == "STARTED", "进程未在5秒内启动"
        
        # 验证任务状态
        assert self.executor.is_task_active("to_terminate"), "任务未处于活跃状态"
        
        # 执行取消
        logger.info("终止进程任务")
        process_queue.put("STOP")  # 发送停止信号
        cancelled = self.executor.cancel_task("to_terminate")
        assert cancelled, "取消操作失败"
        
        # 验证终止
        #mock_terminate.assert_called_once()
        assert not self.executor.is_task_active("to_terminate"), "任务仍处于活跃状态"

    def test_process_termination2(self):
        
        # 提交一个长任务
        self.executor.execute_async(
            "to_terminate", 
            long_running_task, 
            100, "Terminate Test", 
            is_long_task=True
        )
        
        # 确保进程已启动
        time.sleep(0.5)
        assert self.executor.is_task_active("to_terminate"), "任务未处于活跃状态"
        
        # 终止任务
        pid = self.executor._process_pids["to_terminate"]
        cancelled = self.executor.cancel_task("to_terminate")
        logger.info(f"取消结果: {cancelled}")  # 打印取消结果，用于debuggin
        assert cancelled, "取消操作失败"
        
        # 检查terminate是否被调用
        #mock_terminate.assert_called_once_with(pid)
        assert not self.executor.is_task_active("to_terminate"), "任务仍处于活跃状态"

if __name__ == '__main__':
    specialTest = SpecialAsyncTest()
    specialTest.test_process_termination()
    specialTest.test_process_termination1()
    specialTest.test_process_termination2()
    specialTest.terminated()