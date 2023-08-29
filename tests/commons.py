from typing import Any, Callable

from traits.trait_types import Bool, Dict, Float, Int, Str

from zjb.dos.data import Data
from zjb.dos.data_manager import DataManager

DataFactory = Callable[[], Data]
TraitTuple = tuple[str, Any]
TraitsDict = dict[str, Any]


class DictDataManager(DataManager):
    """用于测试数据接口的简单数据管理器"""

    dict = Dict(Str)

    def _bind(self, data: Data):
        for name in data.store_traits:
            self._set_data_trait(data, name, getattr(data, name))

    def _get_data_trait(self, data: Data, name: str) -> Any:
        return self.dict[data._gid.str + name]

    def _set_data_trait(self, data: Data, name: str, value):
        self.dict[data._gid.str + name] = value


class Book(Data):

    name = Str("?")

    page = Int(99)

    price = Float(33.0)

    sold = Bool(False)
