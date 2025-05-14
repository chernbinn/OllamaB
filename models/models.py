import logging
from typing import List, Dict, Optional, Protocol, runtime_checkable, Union
from pydantic import BaseModel, Field
import copy, os
import logging
from utils.logging_config import setup_logging
from threading import Lock
from enum import Enum
from functools import wraps

# 初始化日志配置
logger = setup_logging(log_level=logging.INFO, log_tag="models")

class ModelBackupStatus(BaseModel):
    model_name: str
    backup_path: str|None = None
    backup_status: bool
    zip_file: str|None = None
    zip_md5: str|None = None
    size: int|None = None

class Blob(BaseModel):
    name: str
    size: int|None = None
    md5: str|None = None
    path: str|None = None
    models: List[str] = Field(default_factory=list)

    def append_model(self, model_name: str) -> None:
        if model_name not in self.models:
            self.models.append(model_name)
        #return self
    
    def remove_model(self, model_name: str) -> None:
        if model_name in self.models:
            self.models.remove(model_name)

class LLMModel(BaseModel):
    model_path: str|None = None
    name: str
    description: str    
    llm: str
    version: str
    manifest: str|None = None
    blobs: List[str]|None = None
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
    def notify_set_model(self, model: LLMModel) -> None: ...
    def notify_delete_model(self, model: LLMModel) -> None: ...
    def notify_set_blob(self, blob: Blob) -> None:...
    def notify_set_backup_status(self, status: ModelBackupStatus) -> None:...
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
        self._blobs: Dict[str, Blob] = {}                
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

    def set_model(self, model: LLMModel) -> None:
        logger.debug(f"添加模型: {model.name}")
        """添加模型并通知观察者"""
        with self._lock:  # 获取锁
            if model.name in self._models:
                bk_status = self._models[model.name].bk_status if model.bk_status is None else model.bk_status
                model.bk_status = bk_status
            self._models[model.name] = model
            for blob in model.blobs:
                if blob not in self._blobs:
                    #logger.debug(f"添加blob: {blob}")
                    self._blobs[blob] = Blob(name=blob, size=None, md5=None, path=None)
                    self._blobs[blob].append_model(model.name)
                else:
                    #logger.debug(f"blob已存在: {blob}")
                    self._blobs[blob].append_model(model.name)
        self._notify_observers("notify_set_model", copy.deepcopy(model))

    def delete_model(self, model: LLMModel) -> None:
        """删除模型并通知观察者"""
        with self._lock:
            if not self._models.pop(model.name, None):
                logger.warning(f"尝试删除不存在的模型: {model.name}")
        self._notify_observers("notify_delete_model", model)

    def set_backup_status(self, status: ModelBackupStatus) -> None:
        """更新备份状态并通知观察者"""
        logger.debug(f"更新备份状态: {status}")  # 调试日志，确保正确更新备份状态
        model_name = status.model_name
        if status.zip_file and os.path.exists(status.zip_file):
            zip_file = status.zip_file
            status.size = os.path.getsize(zip_file)
            if status.backup_status and status.zip_md5 is None:
                zip_md5 = zip_file.split('_')[-1].split('.')[0]
                status.zip_md5 = zip_md5
            
        with self._lock:
            exist = True
            if model_name not in self._models:
                model = LLMModel(
                    model_path=None,
                    name=model_name,
                    description=model_name,
                    llm=model_name.split(':')[0] if ':' in model_name else model_name,
                    version=model_name.split(':')[-1] if ':' in model_name else 'latest',
                    manifest=None,
                    blobs=None,
                    bk_status=status
                )
                exist = False
                self._models[model_name] = model
            self._models[model_name].bk_status = status
        if exist:
            self._notify_observers("notify_set_backup_status", copy.deepcopy(status))
        else:
            self._notify_observers("notify_set_model", copy.deepcopy(model))

    def get_backup_status(self, model_name: str) -> Optional[ModelBackupStatus]:
        """获取模型备份状态"""
        with self._lock:
            try:
                return copy.deepcopy(self._models.get(model_name, {}).get("bk_status", None))
            except Exception as e:
                logger.error(f"获取备份状态时出错: {e}", exc_info=True)
                return None
                
    def exist_model_backup(self, model_name: str) -> bool:
        """检查模型是否存在备份"""
        with self._lock:
            try:
                return self._models.get(model_name, {}).get("bk_status", None) is not None
            except Exception as e:
                logger.error(f"检查备份状态时出错: {e}", exc_info=True)
                return False

    @property
    def models(self) -> List[LLMModel]:
        """获取所有模型"""
        with self._lock:
            try:
                return copy.deepcopy(self._models.values())  # 使用深拷贝返回副本防止外部修改
            except Exception as e:
                logger.error(f"获取模型列表时出错: {e}", exc_info=True)
                return []
    
    def get_model(self, model_name: str) -> Optional[LLMModel]:
        """获取指定模型"""
        with self._lock:
            try:
                return copy.deepcopy(self._models.get(model_name, None))  # 使用深拷贝返回副本防止外部修改
            except Exception as e:
                logger.error(f"获取模型时出错: {e}", exc_info=True)
                return None

    @property
    def initialized(self) -> bool:
        """是否已完成初始化"""
        with self._lock:
            return self._initialized

    @initialized.setter
    def initialized(self, value: bool) -> None:
        """设置初始化状态"""
        with self._lock:
            self._initialized = value
        self._notify_observers("notify_initialized", value)

    @property
    def process_event(self) -> ProcessEvent:
        with self._lock:
            return self._process_event

    @process_event.setter
    def process_event(self, value: ProcessEvent) -> None:
        """设置初始化状态"""
        with self._lock:
            self._process_event = value
        self._notify_observers("notify_process_status", value)
    
    @property
    def blobs(self) -> Dict[str, Blob]:
        with self._lock:
            return copy.deepcopy(self._blobs)
    def set_blob(self, blob: Blob) -> None:
        """添加 Blob 信息"""
        with self._lock:
            if blob.name in self._blobs:
                logger.debug(f"setblob-更新blob: {blob.name}")
                self._blobs[blob.name].size = blob.size
                self._blobs[blob.name].md5 = blob.md5
                self._blobs[blob.name].path = blob.path
            else:
                logger.debug(f"set-blob添加blob: {blob.name}")
                self._blobs[blob.name] = blob
        self._notify_observers("notify_set_blob", copy.deepcopy(blob))
    def get_blob(self, name: str) -> Optional[Blob]:
        """获取 Blob 信息"""
        with self._lock:
            #logger.debug(f"获取blob: {name}")
            return copy.deepcopy(self._blobs.get(name, None))
    def get_blob_size(self, name: str, b_human: bool=False) -> str|int:
        """获取 Blob 大小"""
        blob = self.get_blob(name)
        if not blob or not blob.size:
            logger.debug(f"blob不存在或大小为0: {name}")
            return 0 if not b_human else "加载中..." #"0B"
        if b_human:
            return self._human_readable_size(blob.size) if blob else ""
        return blob.size

    @staticmethod
    def _human_readable_size(size_bytes: Union[int, float]) -> str:
        """将字节大小转换为带单位的可读字符串    
        Args:
            size_bytes: 字节大小        
        Returns:
            格式化后的字符串，自动选择最佳单位
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if abs(size_bytes) < 1024.0:
                return f"{size_bytes:.2f} {unit}" if unit != 'B' else f"{int(size_bytes)} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    
    @staticmethod
    def _humansize_to_bytes(size_str: str) -> int:
        """将带单位的字符串转换为字节大小
        Args:
            size_str: 带单位的字符串，如 '10MB', '2GB'
        Returns:
            字节大小
        Raises:
            ValueError: 如果字符串格式不正确
        """
        size_str = size_str.upper()
        logger.debug(f"输入字符串: {size_str}")
        units = {'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4, 'B': 1}
        try:
            for unit in units:
                #logger.debug(f"检查单位: {unit}")
                if size_str.endswith(unit):
                    return int(float(size_str[:-len(unit)].strip()) * units[unit])
            return int(size_str)  # 如果没有单位，直接返回整数大小
        except Exception as e:
            logger.error(f"无法解析大小字符串: {size_str}, exception: {e}", exc_info=True)
            return 0