import logging
from typing import List, Dict, Optional, Protocol, runtime_checkable
from pydantic import BaseModel
import copy
import logging
from logging_config import setup_logging
from threading import Lock
from enum import Enum

# 初始化日志配置
logger = setup_logging(log_level=logging.DEBUG, log_tag="models")

class ModelBackupStatus(BaseModel):
    model_name: str
    backup_path: str|None = None
    backup_status: bool
    zip_file: str|None = None
    zip_md5: str|None = None

class LLMModel(BaseModel):
    model_path: str
    name: str
    description: str    
    llm: str
    version: str
    manifest: str
    blobs: List[str]
    bk_status: Optional[ModelBackupStatus] = None

class ProcessEvent(Enum):
    WINDOW_INFO = 1
    WINDOW_ERR = 2
    WINDOW_WAR = 3
    BAR_INFO = 4
    BAR_ERR = 5
    BAR_WAR = 6
    PROGRESS = 7

class ProcessStatus(BaseModel):
    event: ProcessEvent|None = None
    message: str|int|None = None

@runtime_checkable
class ModelObserver(Protocol):
    def notify_add_model(self, model: LLMModel) -> None: ...
    def notify_delete_model(self, model: LLMModel) -> None: ...
    def notify_update_model(self, model: LLMModel) -> None: ...
    def notify_update_backup_status(self, status: ModelBackupStatus) -> None:...
    def notify_initialized(self, initialized: bool) -> None: ...
    def notify_process_status(self, status: ProcessStatus) -> None:...

class ModelData:
    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, batch_size: int = 5):
        self._models: Dict = {}
        self._observers: List[ModelObserver] = []
        self._initialized: bool = False
        self._lock = Lock()  # 添加锁对象
        self._process_event: ProcessEvent = ProcessStatus(event=None, message="就绪")

    def add_observer(self, observer: ModelObserver) -> None:
        logger.debug(f"添加观察者: {observer}")  # 调试日志，确保正确添加观察器
        """添加观察者"""
        with self._lock:  # 获取锁
            if not isinstance(observer, ModelObserver):
                logger.warning(f"添加的观察者未实现ModelObserver协议: {observer}")
            self._observers.append(observer)

    def remove_observer(self, observer: ModelObserver) -> None:
        logger.debug(f"移除观察者: {observer}")  # 调试日志，确保正确移除观察器
        """移除观察者"""
        with self._lock:  # 获取锁
            try:
                self._observers.remove(observer)
            except ValueError:
                logger.warning(f"尝试移除未注册的观察者: {observer}")

    def _notify_observers(self, method_name: str, *args, **kwargs) -> None:
        """通知所有观察者的指定方法""" 
        with self._lock:  # 同样的锁
            current_observers = list(self._observers)  # 创建副本避免长时间持有锁
            logger.debug(f"通知观察者: {method_name} 观察者数量：{len(current_observers)}")  # 调试日志，确保正确调用方法

        for observer in current_observers:
            try:
                method = getattr(observer, method_name)
                logger.debug(f"通知观察者的方法 {method_name}")  # 调试日志，确保正确调用方法
                method(*args, **kwargs)
            except AttributeError:
                logger.error(f"观察者 {observer} 缺少方法 {method_name}")
            except Exception as e:
                logger.error(f"通知观察者时出错: {e}")

    def add_model(self, model: LLMModel) -> None:
        logger.debug(f"添加模型: {model.name}")
        """添加模型并通知观察者"""
        self._models[model.name] = model
        self._notify_observers("notify_add_model", copy.deepcopy(model))

    def delete_model(self, model: LLMModel) -> None:
        """删除模型并通知观察者"""
        if self._models.pop(model.name, None):
            self._notify_observers("notify_delete_model", model)
        else:
            logger.warning(f"尝试删除不存在的模型: {model.name}")

    def update_model(self, model: LLMModel) -> None:
        """更新模型并通知观察者"""
        self._models[model.name] = model
        self._notify_observers("notify_update_model", copy.deepcopy(model))

    def update_backup_status(self, status: ModelBackupStatus) -> None:
        """更新备份状态并通知观察者"""
        logger.debug(f"更新备份状态: {status}")  # 调试日志，确保正确更新备份状态
        model_name = status.model_name
        if status.backup_status:
            zip_file = status.zip_file
            zip_md5 = zip_file.split('_')[-1].split('.')[0]
            status.zip_md5 = zip_md5
        
        self._models[model_name].bk_status = status
        self._notify_observers("notify_update_backup_status", copy.deepcopy(status))

    def get_backup_status(self, model_name: str) -> Optional[str]:
        """获取模型备份状态"""
        return self._models.get(model_name, {}).get("bk_status", None)

    @property
    def models(self) -> List[LLMModel]:
        """获取所有模型"""        
        return copy.deepcopy(self._models.values())  # 使用深拷贝返回副本防止外部修改
    
    def get_model(self, model_name: str) -> Optional[LLMModel]:
        """获取指定模型"""
        return self._models.get(model_name, None)

    @property
    def initialized(self) -> bool:
        """是否已完成初始化"""
        return self._initialized

    @initialized.setter
    def initialized(self, value: bool) -> None:
        """设置初始化状态"""
        self._initialized = value
        self._notify_observers("notify_initialized", value)

    @property
    def process_event(self) -> ProcessEvent:
        return self._process_event

    @process_event.setter
    def process_event(self, value: ProcessEvent) -> None:
        """设置初始化状态"""
        self._process_event = value
        self._notify_observers("notify_process_status", value)