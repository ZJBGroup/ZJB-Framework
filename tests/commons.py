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

    ('test_data', _TestData(test_str='Nature', test_int=1111)),

    ('test_nested_tuple', ((1, 2), (3, 4))),
    ('test_nested_list', [[1, 2], [3, 4]]),
    ('test_nested_dict', {'a': {'a': 1}, 'b': {'b', 1}}),
    ('test_compound_tuple', (
        1, 'a', (1, 2), [1, 2], {1, 2}, {'a': 1})),
    ('test_compound_list', [
        1, 'a', (1, 2), [1, 2], {1, 2}, {'a': 1}]),
    ('test_compound_dict', {
        'int': 1, 'str': 'a', 'tuple': (1, 2), 'list': [1, 2],
        'set': {1, 2}, 'dict': {'a': 1}
    }),


    ('test_data_tuple', (_TestData(test_str='Nature', test_int=1111),)),
    ('test_data_in_list', [_TestData(test_str='Nature', test_int=1111)]),
    ('test_data_in_dict', {
        'data': _TestData(test_str='Nature', test_int=1111)
    }),
    ('test_data_in_dict_key', {
        _TestData(test_str='Nature', test_int=1111): 1
    }),
    ('test_data_in_set', {_TestData(test_str='Nature', test_int=1111)}),
    ('test_data_in_frozenset', frozenset(
        [_TestData(test_str='Nature', test_int=1111)])),

    ('test_data_in_data', _TestData(
        test_data=_TestData(test_str='Nature', test_int=1111))),

])

performance_parametrize = mark.parametrize('trait', [
    ('test_base', '123'),
])
