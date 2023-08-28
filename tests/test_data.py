import logging
from typing import Any, Callable

from pytest import mark
from traits.trait_types import Bool, Dict, Float, Int, Str

from zjb.dos.data import Data
from zjb.dos.data_manager import DataManager

logger = logging.getLogger(__name__)


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


class TestDataInterface:
    """测试数据接口"""

    parametrize = mark.parametrize('data_maker, kvs', [
        (Book, [
            ('name', 'Nature'),
            ('page', 1111),
            ('price', 22),
            ('sold', True)
        ]),
    ])

    @parametrize
    def test_set(self, data_maker: Callable[[], Data], kvs: list[tuple[str, Any]]):
        """测试特征赋值"""
        dm = DictDataManager()
        data = data_maker()
        dm.bind(data)
        for name, value in kvs:
            setattr(data, name, value)
            assert dm._get_data_trait(data, name) == value

    @parametrize
    def test_get(self, data_maker: Callable[[], Data], kvs: list[tuple[str, Any]]):
        """测试特征获取"""
        dm = DictDataManager()
        data = data_maker()
        dm.bind(data)
        for name, value in kvs:
            dm._set_data_trait(data, name, value)
            assert getattr(data, name) == value
