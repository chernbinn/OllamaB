import logging
from typing import List, Dict, Optional, Protocol, runtime_checkable
from pydantic import BaseModel
import copy
import logging
from utils.logging_config import setup_logging
from threading import Lock
from enum import Enum
from functools import wraps

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

class Blobs(BaseModel):
    sha256: str
    size: int
    path: str
    md5: str

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

def call_once(func):
    lock = Lock()
    @wraps(func) # 保留原函数func的元数据信息，如名称、文档字符串等。便于调试和错误追踪。
    def wrapper(self, *args, **kwargs):
        if not hasattr(self, '_called'):
            with lock:
                if not hasattr(self, '_called'):
                    setattr(self, '_called', True)
                    return func(self, *args, **kwargs)
        logger.warning(f"{func.__name__} already called")
    return wrapper

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
                    cls._instance._instance_initialized = False
                    cls._instance._init(*args, **kwargs)
        return cls._instance

    def __init__(self):
        # __new__和__init__是分离执行的，多线程情况下存在__new__返回实例后，调用函数，
        # 但是__init__还没有执行的情况，所以需要加锁，确保只有一个实例在初始化。
        # 其实，在多线程下，即使加锁也存在属性未被初始化情况，好的方式是使用工厂模式或者初始化放在__new__中
        # 现在该方法可以留空且不需要_instance_initialized
        # 非单例模式下，__new__和__init__是一个原子操作，因此不存在初始化问题。
        # 单例模式下，本质上__new__和__init__是一体执行的，只是多线程情况下存在__new__返回实例后，调用函数
        pass
    
    @call_once
    def _init(self) -> None:
        """ 只可以被__new__调用一次，用于初始化单例模式下的属性，可以根据需求添加客制化参数 """
        self._models: Dict = {}
        self._observers: List[ModelObserver] = []
        self._blobs: Dict[str, Blobs] = {}                
        self._initialized: bool = False
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
        if status.backup_status and status.zip_file:
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
    
    @property
    def blobs(self) -> Dict[str, Blobs]:
        return self._blobs
    def add_blob(self, blob: Blobs) -> None:
        """添加 Blob 信息"""
        self._blobs[blob.sha256] = blob
    def get_blob(self, sha256: str) -> Optional[Blobs]:
        """获取 Blob 信息"""
        return self._blobs.get(sha256, None)