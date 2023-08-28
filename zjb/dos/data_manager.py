from abc import abstractmethod
from typing import Any

from traits.has_traits import ABCMetaHasTraits, HasPrivateTraits

from .data import Data


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
        """获取数据的的特征"""

    @abstractmethod
    def _set_data_trait(self, data: Data, name: str, value):
        """设置数据特征"""
