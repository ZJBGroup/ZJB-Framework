import io
import random
import sys
from abc import abstractmethod
from pickle import Pickler, Unpickler
from time import sleep
from typing import Any, Generic, Iterator, NamedTuple, TypeVar
from weakref import WeakValueDictionary, ref

from traits.has_traits import (
    ABCMetaHasTraits,
    HasPrivateTraits,
    HasRequiredTraits,
    Property,
    cached_property,
)
from traits.trait_dict_object import TraitDictObject
from traits.trait_list_object import TraitListObject
from traits.trait_set_object import TraitSetObject
from traits.trait_types import Bool, Bytes, Dict, Str
from ulid import ULID

from .._traits.types import Instance
from .data import Data

T = TypeVar("T", bound=Data)

if sys.version_info >= (3, 9) and sys.version_info < (3, 11):
    # Python3.9/10中NamedTuple不支持泛型
    # see: https://github.com/python/cpython/issues/88089
    class DataRef(NamedTuple):
        gid: ULID
        type: type[Data]

        @classmethod
        def from_data(cls, data: Data):
            return cls(data._gid, type(data))

else:

    class DataRef(NamedTuple, Generic[T]):
        gid: ULID
        type: type[T]

        @classmethod
        def from_data(cls, data: Data):
            return cls(data._gid, type(data))


class TraitItem(NamedTuple):
    key: bytes
    value: bytes


class Package(NamedTuple):
    ref: DataRef
    data: Data
    traits: list[TraitItem]


PackageDict = dict[ULID, Package]


class _Pickler(Pickler):
    def __init__(self, manager: "DataManager"):
        _bytes = io.BytesIO()
        super().__init__(_bytes)
        self.bytes = _bytes
        self.manager = manager
        # 记录所包含的未管理数据
        self.unmanagered: dict[ULID, Data] = {}

    def persistent_id(self, obj: Any) -> Any:
        if not isinstance(obj, Data):
            return None

        ref = DataRef.from_data(obj)

        # 检查data是否属于其他管理器
        manager = obj._manager
        if manager and manager is not self.manager:
            raise ValueError(
                f"Can not pack {obj} belongs to {manager} in {self.manager}"
            )

        # 记录未管理数据
        if not manager:
            self.unmanagered[ref.gid] = obj

        return ref


class _Unpickler(Unpickler):
    def __init__(self, bytes, manager: "DataManager"):
        super().__init__(io.BytesIO(bytes))
        self.manager = manager

    def persistent_load(self, pid: Any) -> Any:
        return self.manager._unpack_ref(pid)


class DataManager(HasPrivateTraits, metaclass=ABCMetaHasTraits):
    """数据管理器,提供统一的数据库接口"""

    # 已管理数据的弱引用
    _refs: WeakValueDictionary[ULID, Data] = Instance(WeakValueDictionary, args=(), transient=True)  # type: ignore

    # 已打包或正在打包的数据
    _packages: PackageDict = Dict(transient=True)  # type: ignore

    def bind(self, data: Data):
        """将数据持久化并绑定到当前数据管理器"""
        if data._manager:
            raise ValueError("data must be unbound")

        packages = {}
        self._dumps(data, packages)
        self._put(packages)
        self._finish(packages)

    def unbind(self, data: Data):
        """解除数据与数据管理器的绑定,并从数据库中删除数据"""
        if data._manager is not self:
            raise ValueError(f"{self} can not unbind {data} that is not bound to self")

        gid = data._gid
        self._delete(gid)
        del self._refs[gid]
        data._manager = None

    def iter(self) -> Iterator[Data]:
        """遍历数据管理器中的所有数据"""
        for ref in self._iter():
            yield self._unpack_ref(ref)

    def allocate_lock(self, data: Data, name: "str | None" = None):
        if name:
            return TraitLock(data=data, name=name, manager=self)
        return DataLock(data=data, manager=self)

    @abstractmethod
    def _get(self, key: bytes) -> "bytes | None":
        """从数据库中读特定数据特征"""

    @abstractmethod
    def _put(self, packages: PackageDict):
        """将数据包保存到数据库"""

    @abstractmethod
    def _delete(self, gid: ULID):
        """从数据库中删除数据"""

    @abstractmethod
    def _iter(self) -> Iterator[DataRef]:
        """遍历数据管理器中的所有数据(引用)"""

    def _lock(self, key: bytes, secret: bytes) -> bool:
        """使用secret锁定key
        如果提供的key未锁定或已锁定且提供了正确的secret(重入),
        返回True表示成功锁定key, 否则返回False表示锁定失败
        """
        raise NotImplementedError

    def _unlock(self, key: bytes, secret: bytes):
        """使用secret解锁key
        如果提供的key已锁定且secret一致,_unlock正常退出
        否则抛出异常
        """
        raise NotImplementedError

    def _get_data_trait(self, data: Data, name: str) -> Any:
        """获取数据特征"""
        key = data._gid.bytes + name.encode()
        buffer = self._get(key)
        if not buffer:
            raise ValueError("`%s` of %s not in %s" % (name, data, self))
        value = self._loads(buffer)
        if isinstance(value, (TraitListObject, TraitDictObject, TraitSetObject)):
            value.__dict__ |= {
                "object": ref(data),
                "trait": data.trait(name).handler,
            }
        return value

    def _set_data_trait(self, data: Data, name: str, value):
        """设置数据特征"""
        gid = data._gid
        ref = DataRef.from_data(data)
        packages: PackageDict = {}
        _bytes = self._dumps(value, packages)
        packages[gid] = Package(
            ref, data, [TraitItem(gid.bytes + name.encode(), _bytes)]
        )
        self._put(packages)
        self._finish(packages)

    def _finish(self, packages: PackageDict):
        """更新数据库后以后清理packages"""
        for _, data, _ in packages.values():
            if not data._manager:
                self._refs[data._gid] = data
                data._manager = self

    def _dumps(self, obj: Any, packages: PackageDict) -> bytes:
        pickler = _Pickler(self)
        pickler.dump(obj)
        res = pickler.bytes.getvalue()
        for gid, data in pickler.unmanagered.items():
            if gid in packages:
                continue
            packages[gid] = None  # type: ignore
            key_prefix = gid.bytes
            traits = [TraitItem(key_prefix, self._dumps(type(data), packages))] + [
                TraitItem(
                    key_prefix + name.encode(),
                    self._dumps(getattr(data, name), packages),
                )
                for name in data.store_traits
            ]
            packages[gid] = Package(DataRef.from_data(data), data, traits)
        return res

    def _loads(self, buffer: bytes) -> Any:
        res = _Unpickler(buffer, self).load()
        return res

    def _unpack_ref(self, ref: "DataRef[T]") -> T:
        gid, cls = ref
        data = self._refs.get(gid, None)
        if not data:
            data = self._refs[gid] = cls.from_manager(self, gid)
        return data  # type: ignore


class _Lock(HasPrivateTraits, HasRequiredTraits):
    manager = Instance(DataManager, required=True)

    key = Bytes(required=True)

    secret: bytes = Property()

    locked = Bool()

    @cached_property
    def _get_secret(self):
        return random.randbytes(16)

    def acquire(self, block=True):
        if self.locked:
            return True
        while True:
            self.locked = locked = self.manager._lock(self.key, self.secret)
            if locked or not block:
                break
            sleep(0.01)
        return locked

    __enter__ = acquire

    def release(self):
        if not self.locked:
            raise RuntimeError("cannot release un-acquired lock")
        self.manager._unlock(self.key, self.secret)
        self.locked = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class DataLock(_Lock):
    key: bytes = Property()

    data = Instance(Data, required=True)

    @cached_property
    def _get_key(self):
        return self.data._gid.bytes


class TraitLock(DataLock):
    name = Str(require=True)

    @cached_property
    def _get_key(self):
        return self.data._gid.bytes + self.name.encode()
