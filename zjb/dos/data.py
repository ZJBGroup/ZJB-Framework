from typing import TYPE_CHECKING

import ulid
from traits.has_traits import HasPrivateTraits, HasRequiredTraits
from traits.trait_types import Set, Str

if TYPE_CHECKING:
    from .data_manager import DataManager


def is_not_true(value):
    return value is not True


class Data(HasPrivateTraits, HasRequiredTraits):
    _manager: "DataManager | None"

    store_traits = Set(Str, transient=True)

    def __init__(self, **traits):
        super().__init__(**traits)
        self._gid = ulid.new()
        self.store_traits = set(self.trait_names(transient=is_not_true))

    @classmethod
    def from_manager(cls, manager: "DataManager", gid: ulid.ULID):
        data = cls.__new__(cls)
        HasPrivateTraits.__init__(data)
        data._gid = gid
        data._manager = manager
        data.store_traits = set(data.trait_names(transient=is_not_true))
        return data

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name not in self.store_traits:
            return
        if not self._manager:
            return

        # 获取经过验证的特征值并更新到管理器
        value = super().__getattribute__(name)
        self._manager._set_data_trait(self, name, value)

    def __getattribute__(self, name):
        if name == "store_traits" or name not in self.store_traits:
            return super().__getattribute__(name)

        if not self._manager:
            return super().__getattribute__(name)

        return self._manager._get_data_trait(self, name)

    def __enter__(self):
        if self._manager:
            self._lock = self._manager.allocate_lock(self)
            return self._lock.acquire()
        return True

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._lock:
            self._lock.release()
            self._lock = None

    def clone_traits(self, traits=None, memo=None, copy=None, **metadata):
        cloned = super().clone_traits(traits, memo, copy, **metadata)
        cloned._gid = ulid.new()
        return cloned

    def unbind(self):
        if self._manager:
            self._manager.unbind(self)
