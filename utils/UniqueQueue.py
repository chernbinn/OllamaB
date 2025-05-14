from queue import Queue
from typing import Any, Optional
import hashlib
import weakref
import threading

class UniqueQueue(Queue):
    """支持不可哈希类型的唯一值队列"""
    
    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        self._seen = set()  # 存储元素的哈希值
    
    def _put(self, item: Any):
        """重写放入元素的内部方法"""
        item_hash = self._get_hash(item)
        if item_hash not in self._seen:
            self._seen.add(item_hash)
            super()._put(item)
    
    def _get(self) -> Any:
        """重写获取元素的内部方法"""
        item = super()._get()
        item_hash = self._get_hash(item)
        self._seen.remove(item_hash)
        return item
    
    def _get_hash(self, item: Any) -> str:
        """生成元素的唯一哈希值"""
        if isinstance(item, (str, int, float, bool)):
            return str(item)
        try:
            return hashlib.md5(str(item).encode()).hexdigest()
        except:
            return str(id(item))  # 最后手段，使用对象ID
    
    def __contains__(self, item: Any) -> bool:
        """检查元素是否在队列中"""
        return self._get_hash(item) in self._seen

class SafeUniqueQueue(Queue):
    """支持不可哈希类型的唯一值队列"""
    
    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        self._seen = set()  # 存储元素的哈希值
        self._lock = threading.Lock()  # 额外细粒度锁
    
    def _put(self, item: Any):
        """重写放入元素的内部方法"""
        with self._lock:
            item_hash = self._get_hash(item)
            if item_hash not in self._seen:
                self._seen.add(item_hash)
                super()._put(item)
    
    def _get(self) -> Any:
        """重写获取元素的内部方法"""
        with self._lock:
            item = super()._get()
            item_hash = self._get_hash(item)
            self._seen.remove(item_hash)
            return item
    
    def _get_hash(self, item: Any) -> str:
        """生成元素的唯一哈希值"""
        if isinstance(item, (str, int, float, bool)):
            return str(item)
        try:
            return hashlib.md5(str(item).encode()).hexdigest()
        except:
            return str(id(item))  # 最后手段，使用对象ID
    
    def __contains__(self, item: Any) -> bool:
        """检查元素是否在队列中"""
        with self._lock:
            return self._get_hash(item) in self._seen

"""
以下是使用id判重，只能保证队列中的元素是为可以的不重复的实例，不能保证内容是不重复的。
如果要保证内容不重复，一定需要实现对对象进行hash
"""
class UniversalUniqueQueue(Queue):
    """通用唯一队列，支持几乎所有对象类型"""
    
    def __init__(self, maxsize=0):
        super().__init__(maxsize)
        self._seen = weakref.WeakSet() # 隐式依赖存储对象的id，以此支持几乎所有对象类型，包括不可序列化对象
        self._lock = threading.Lock()  # 额外细粒度锁
    
    def _put(self, item):
        with self._lock:
            if item not in self._seen:
                self._seen.add(item)
                super()._put(item)
    
    def _get(self):
        with self._lock:
            item = super()._get()
            try:
                self._seen.remove(item)
            except KeyError:
                pass
            return item
    
    def __contains__(self, item):
        with self._lock:
            return item in self._seen

class ObjectIDUniqueQueue(Queue):
    """显式使用对象ID的唯一队列，支持不可哈希/不可序列化对象"""
    
    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        self._seen_ids = set()  # 存储对象ID
        self._lock = threading.Lock()  # 细粒度锁
    
    def _put(self, item: Any):
        """使用id(item)作为唯一标识"""
        with self._lock:
            item_id = id(item)
            if item_id not in self._seen_ids:
                self._seen_ids.add(item_id)
                super()._put(item)
    
    def _get(self) -> Any:
        """获取时移除对象ID"""
        with self._lock:
            item = super()._get()
            self._seen_ids.discard(id(item))  # 使用discard避免KeyError
            return item
    
    def __contains__(self, item: Any) -> bool:
        """检查对象ID是否存在"""
        with self._lock:
            return id(item) in self._seen_ids
    
    def _qsize(self) -> int:
        """队列大小需要同步"""
        with self._lock:
            return super()._qsize()