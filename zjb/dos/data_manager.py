from abc import abstractmethod
from typing import Any

from traits.has_traits import ABCMetaHasTraits, HasPrivateTraits

from .data import Data
from .wrapper import DataRef, dumps, loads


class DataManager(HasPrivateTraits, metaclass=ABCMetaHasTraits):
    """数据管理器,提供统一的数据库接口"""

    def bind(self, data: Data):
        """将数据持久化并绑定到当前数据管理器"""
        if data._manager:
            raise ValueError("data must be unbound")
        self._bind(data)
        data._manager = self

    @abstractmethod
    def _bind(self, data: Data): ...

    @abstractmethod
    def _get_data_trait(self, data: Data, name: str) -> Any:
        """获取数据特征"""

    @abstractmethod
    def _set_data_trait(self, data: Data, name: str, value):
        """设置数据特征"""

    def _dumps(self, value: Any) -> bytes:
        """序列化"""
        return dumps(value, self._to_ref)

    def _loads(self, wrapped: bytes) -> Any:
        """反序列化"""
        return loads(wrapped, self._to_data)

    def _to_ref(self, data: Data) -> DataRef:
        """处理数据并转引用"""
        if data._manager and data._manager is not self:
            raise ValueError(
                f"{data} bound to {data._manager} can no longer be bound to {self}")
        if not data._manager:
            self.bind(data)
        return DataRef.from_data(data)

    def _to_data(self, ref: DataRef) -> Data:
        """解析引用"""
        return ref.type(ref.gid, self)
