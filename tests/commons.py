from typing import Any, Iterator

import ulid
from pytest import mark
from traits.trait_types import Any as TraitAny
from traits.trait_types import Bytes, Dict
from ulid import ULID

from zjb.dos.data import Data
from zjb.dos.data_manager import DataManager, DataRef, PackageDict

TraitTuple = tuple[str, Any]
TraitsDict = dict[str, Any]


class DictDataManager(DataManager):
    """用于测试数据接口的简单数据管理器"""

    dict = Dict(Bytes, Bytes)

    def _get(self, key: bytes) -> "bytes | None":
        return self.dict.get(key)

    def _put(self, packages: PackageDict):
        for _, (_, _, traits) in packages.items():
            for key, value in traits:
                self.dict[key] = value

    def _delete(self, gid: ULID):
        key_prefix = gid.bytes
        keys = [
            key
            for key in self.dict.keys()
            if key[:16] == key_prefix
        ]
        for key in keys:
            del self.dict[key]

    def _iter(self) -> Iterator[DataRef]:
        for key, value in self.dict.items():
            if len(key) == 16:
                yield DataRef(ulid.from_bytes(key), self._loads(value))


class _TestData(Data):

    test_ = TraitAny()


def create_self_reference_data():
    data = _TestData(test_self=None)
    data.test_self = data
    return data


def create_circular_reference_data():
    a = _TestData(test_b=None)
    b = _TestData(test_a=a)
    a.test_b = b
    return a


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

    ('test_self_reference_data', create_self_reference_data()),
    ('test_circular_reference_data', create_circular_reference_data()),
])

performance_parametrize = mark.parametrize('trait', [
    ('test_base', '123'),
])
