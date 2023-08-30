from typing import Any

from pytest import mark
from traits.trait_types import Any as TraitAny
from traits.trait_types import Dict, Str

from zjb.dos.data import Data
from zjb.dos.data_manager import DataManager

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


class _TestData(Data):

    test_ = TraitAny()


traits_parametrize = mark.parametrize('traits', [
    {
        'test_none': None,
        'test_int': 1111,
        'test_float': 22.0,
        'test_complex': 1.+1.j,
        'test_str': 'Nature',
        'test_bool': True,
    }
])

trait_parametrize = mark.parametrize('trait', [
    ('test_none', None),
    ('test_int', 1111),
    ('test_float', 22.0),
    ('test_complex', 1.+1.j),
    ('test_str', 'Nature'),
    ('test_bool', True),
    ('test_tuple', ('apple', 'banana', 'orange')),
    ('test_list', ['apple', 'banana', 'orange']),
    ('test_dict', {'apple': 10, 'banana': 8.8, 'orange': None}),
    ('test_set', {'apple', 'banana', 'orange'}),
    ('test_frozenset', frozenset(['apple', 'banana', 'orange'])),
    ('test_data', _TestData(test_str='Nature', test_int=1111))
])

performance_parametrize = mark.parametrize('trait', [
    ('test_base', '123'),
])
