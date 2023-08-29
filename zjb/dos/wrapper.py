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
    return pdumps(value)


def loads(wrapped: bytes, to_data: Callable[[DataRef], Data]) -> Any:
    """反序列化"""
    return ploads(wrapped)
