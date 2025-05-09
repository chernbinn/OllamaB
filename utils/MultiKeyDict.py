from collections import defaultdict
from typing import Any, Union, List, Tuple, Iterator

class MultiKeyDict:
    """支持同名键的高效字典实现"""
    
    def __init__(self):
        # 使用双层结构：
        # _keys: {key: [id1, id2]} 记录每个key对应的所有值的唯一ID
        # _values: {id: (key, value)} 存储实际数据
        self._keys = defaultdict(list)
        self._values = {}
        self._counter = 0  # 用于生成唯一ID

    # 下标方式赋值
    def __setitem__(self, key: str, value: Any):
        """添加键值对（允许重复key）"""
        self._counter += 1
        self._values[self._counter] = (key, value)
        self._keys[key].append(self._counter)

    # 下标访问取值
    def __getitem__(self, key: Union[str, int]) -> Any:
        """通过key或唯一ID访问值"""
        if isinstance(key, int):  # 按ID直接访问
            return self._values[key][1]
        if key in self._keys:     # 按key访问最新值
            return self._values[self._keys[key][-1]][1]
        raise KeyError(key)

    def get_all(self, key: str) -> List[Any]:
        """获取某个key对应的所有值"""
        return [self._values[id][1] for id in self._keys.get(key, [])]

    def get_by_index(self, key: str, index: int) -> Any:
        """获取key的指定序号值"""
        ids = self._keys.get(key, [])
        if not ids or index >= len(ids):
            raise IndexError(f"Index {index} out of range for key '{key}'")
        return self._values[ids[index]][1]

    def latest(self, key: str) -> Any:
        """获取key的最新值"""
        if key not in self._keys:
            raise KeyError(key)
        return self._values[self._keys[key][-1]][1]

    def oldest(self, key: str) -> Any:
        """获取key的最旧值"""
        if key not in self._keys:
            raise KeyError(key)
        return self._values[self._keys[key][0]][1]
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取key的最新值，如果不存在则返回默认值"""
        try:
            return self.latest(key)
        except KeyError:
            return default

    # if key in dict: 检查key或ID是否存在
    def __contains__(self, key: Union[str, int]) -> bool:
        """检查key或ID是否存在"""
        if isinstance(key, int):
            return key in self._values
        return key in self._keys

    # for key, value in dict: 迭代所有键值对（按插入顺序）
    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        """迭代所有键值对（按插入顺序）"""
        for id in sorted(self._values):
            yield self._values[id]

    # len(dict) 返回唯一键的数量
    def __len__(self) -> int:
        """返回唯一键的数量"""
        return len(self._keys)

    def size_key(self, key: str) -> int:
        """返回某个key的条目数"""
        return len(self._keys.get(key, []))

    def total_entries(self) -> int:
        """返回总条目数（包括重复key）"""
        return len(self._values)

    def remove(self, key: str, index: int = -1):
        """删除指定key的某个值（默认删除最新）"""
        ids = self._keys.get(key, [])
        if not ids:
            raise KeyError(key)
        
        target_id = ids.pop(index) if index != -1 else ids.pop()
        del self._values[target_id]
        
        if not ids:  # 如果该key没有更多值，清理空列表
            del self._keys[key]

    def clear(self):
        """清空所有数据"""
        self._keys.clear()
        self._values.clear()