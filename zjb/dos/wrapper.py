"""
    包装器, 提供DOS专用的序列化与反序列化接口
"""
from pickle import dumps as pdumps
from pickle import loads as ploads
from typing import Any, Callable, NamedTuple

from ulid import ULID

from .data import Data


class DataRef(NamedTuple):
    gid: ULID
    type: type[Data]

    @classmethod
    def from_data(cls, data: Data):
        return cls(data._gid, type(data))


def dumps(value: Any, to_ref: Callable[[Data], DataRef]) -> bytes:
    """序列化"""
    return pdumps(_dumps(value, to_ref))


def loads(wrapped: bytes, to_data: Callable[[DataRef], Data]) -> Any:
    """反序列化"""
    return _loads(ploads(wrapped), to_data)


def _dumps(value: Any, to_ref: Callable[[Data], DataRef]) -> Any:
    """递归处理序列化前的数据以处理容器中的数据"""
    if isinstance(value, Data):
        return to_ref(value)
    if isinstance(value, tuple):
        return tuple(_dumps(v, to_ref) for v in value)
    if isinstance(value, list):
        return [_dumps(v, to_ref) for v in value]
    if isinstance(value, dict):
        return {_dumps(k, to_ref): _dumps(v, to_ref) for k, v in value.items()}
    if isinstance(value, set):
        return {_dumps(v, to_ref) for v in value}
    if isinstance(value, frozenset):
        return frozenset(_dumps(v, to_ref) for v in value)
    return value


def _loads(value: Any, to_data: Callable[[DataRef], Data]) -> Any:
    """递归处理反序列化后的数据以解析容器中的数据引用"""
    if isinstance(value, DataRef):
        return to_data(value)
    if isinstance(value, tuple):
        return tuple(_loads(v, to_data) for v in value)
    if isinstance(value, list):
        return [_loads(v, to_data) for v in value]
    if isinstance(value, dict):
        return {_loads(k, to_data): _loads(v, to_data) for k, v in value.items()}
    if isinstance(value, set):
        return {_loads(v, to_data) for v in value}
    if isinstance(value, frozenset):
        return frozenset(_loads(v, to_data) for v in value)
    return value
