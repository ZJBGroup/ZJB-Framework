from abc import abstractmethod
from pickle import dumps, loads
from typing import Any, Iterator, NamedTuple
from weakref import WeakValueDictionary

from traits.has_traits import ABCMetaHasTraits, HasPrivateTraits
from ulid import ULID

from .data import Data


class DataRef(NamedTuple):
    gid: ULID
    type: type[Data]

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


class DataManager(HasPrivateTraits, metaclass=ABCMetaHasTraits):
    """数据管理器,提供统一的数据库接口"""

    # 已管理数据的弱引用
    _refs: WeakValueDictionary[ULID, Data]

    # 已打包或正在打包的数据
    _packages: PackageDict

    def __init__(self, **traits):
        super().__init__(**traits)
        self._refs = WeakValueDictionary()
        self._packages = {}

    def bind(self, data: Data):
        """将数据持久化并绑定到当前数据管理器"""
        if data._manager:
            raise ValueError("data must be unbound")

        packages = {}
        self._pack_data(data, packages)
        self._put(packages)
        self._finish(packages)

    def unbind(self, data: Data):
        """解除数据与数据管理器的绑定,并从数据库中删除数据"""
        if data._manager is not self:
            raise ValueError(
                f"{self} can not unbind {data} that is not bound to self")

        gid = data._gid
        self._delete(gid)
        del self._refs[gid]
        data._manager = None

    def iter(self) -> Iterator[Data]:
        """遍历数据管理器中的所有数据"""
        for ref in self._iter():
            yield self._unpack_ref(ref)

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

    def _get_data_trait(self, data: Data, name: str) -> Any:
        """获取数据特征"""
        key = data._gid.bytes + name.encode()
        buffer = self._get(key)
        if not buffer:
            raise ValueError("`%s` of %s not in %s" % (name, data, self))
        return self._unpack(self._loads(buffer))

    def _set_data_trait(self, data: Data, name: str, value):
        """设置数据特征"""
        gid = data._gid
        ref = DataRef.from_data(data)
        traits: list[TraitItem] = []
        packages: PackageDict = {
            gid: Package(ref, data, traits)
        }
        package = self._pack(value, packages)
        traits.append(TraitItem(
            gid.bytes + name.encode(), self._dumps(package)))
        self._put(packages)
        self._finish(packages)

    def _finish(self, packages: PackageDict):
        """更新数据库后以后清理packages"""
        for _, data, _ in packages.values():
            if not data._manager:
                self._refs[data._gid] = data
                data._manager = self

    def _dumps(self, package: Any) -> bytes:
        """序列化已打包的数据"""
        return dumps(package)

    def _loads(self, buffer: bytes) -> Any:
        """反序列化"""
        return loads(buffer)

    def _pack_data(self, data: Data, packages: PackageDict) -> DataRef:
        """打包Data"""
        gid = data._gid

        # 检查packages中是否包含该数据, 避免循环引用导致的无限递归
        item = packages.get(gid, None)
        if item:
            return item.ref

        # 检查data是否属于其他管理器
        manager = data._manager
        if manager and manager is not self:
            raise ValueError(
                f"Can not pack {data} belongs to {manager} in {self}")

        # 标记data正在打包
        traits: list[TraitItem] = []
        ref = DataRef.from_data(data)
        packages[gid] = Package(ref, data, traits)

        # 递归打包未管理的数据的特征
        if not manager:
            key_prefix = gid.bytes
            traits.append(TraitItem(key_prefix, self._dumps(ref.type)))
            traits += [
                TraitItem(
                    key_prefix + name.encode(),
                    self._dumps(self._pack(getattr(data, name), packages))
                )
                for name in data.store_traits
            ]

        return ref

    def _pack(self, obj, packages: PackageDict):
        """打包数据"""
        # 数据=>数据引用
        if isinstance(obj, Data):
            return self._pack_data(obj, packages)

        # 容器类型
        if isinstance(obj, tuple):
            return tuple(self._pack(v, packages) for v in obj)
        if isinstance(obj, list):
            return [self._pack(v, packages) for v in obj]
        if isinstance(obj, dict):
            return {self._pack(k, packages): self._pack(v, packages) for k, v in obj.items()}
        if isinstance(obj, set):
            return {self._pack(v, packages) for v in obj}
        if isinstance(obj, frozenset):
            return frozenset(self._pack(v, packages) for v in obj)

        # 其他
        return obj

    def _unpack_ref(self, ref: DataRef) -> Data:
        gid, cls = ref
        data = self._refs.get(gid, None)
        if not data:
            data = self._refs[gid] = cls(gid, self)
        return data

    def _unpack(self, obj):
        """解包数据"""
        # 数据引用=>数据
        if isinstance(obj, DataRef):
            return self._unpack_ref(obj)

        # 容器类型
        if isinstance(obj, tuple):
            return tuple(self._unpack(v) for v in obj)
        if isinstance(obj, list):
            return [self._unpack(v) for v in obj]
        if isinstance(obj, dict):
            return {self._unpack(k): self._unpack(v) for k, v in obj.items()}
        if isinstance(obj, set):
            return {self._unpack(v) for v in obj}
        if isinstance(obj, frozenset):
            return frozenset(self._unpack(v) for v in obj)

        # 其他
        return obj
