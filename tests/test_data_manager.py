import random
from tempfile import TemporaryDirectory

from pytest import fixture, raises

from tests.commons import (
    TraitsDict,
    TraitTuple,
    _TestData,
    deepcopy,
    trait_parametrize,
    traits_parametrize,
)
from zjb.dos.data_manager import DataManager
from zjb.dos.lmdb_data_manager import LMDBDataManager


class _TestDataManager:
    """测试数据管理器"""

    @traits_parametrize
    def test_bind(self, dm: DataManager, traits: TraitsDict):
        """测试数据管理器bind接口
        - 构造一个新的数据
        - 记录数据的store特征字典
        - 数据绑定到数据管理器
        - 数据的_manager属性应当为数据管理器
        - 从数据管理器获取的store特征应当与记录的字典中的值一致
        """
        data = _TestData(**deepcopy(traits))
        assert data._manager is None

        dm.bind(data)
        assert data._manager is dm

        for name, value in traits.items():
            assert dm._get_data_trait(data, name) == value

    @traits_parametrize
    def test_unbind(self, dm: DataManager, traits: TraitsDict):
        """测试数据管理器的unbind接口
        - 构造一个新的数据
        - 将数据绑定到数据管理器(`dm.bind`)
        - 将数据从数据管理器解绑定(`dm.unbind`)
        - 从数据管理器获取任意`store_traits`中的特征都会引发ValueError
        - 数据的`_manager`属性为`None`
        """
        data = _TestData(**deepcopy(traits))
        dm.bind(data)

        dm.unbind(data)
        for name in traits.keys():
            with raises(ValueError):
                dm._get_data_trait(data, name)
        assert data._manager is None

    @traits_parametrize
    def test_iter(self, dm: DataManager, traits: TraitsDict):
        """测试数据管理器的iter接口"""
        data = [_TestData(**{name: deepcopy(value)}) for name, value in traits.items()]
        for d in data:
            dm.bind(d)

        for _data in dm.iter():
            data.remove(_data)
        assert data == []

    def test_allocate_lock(self, dm: DataManager):
        """测试数据管理器的allocate_lock接口"""
        data = _TestData()

        # 数据锁
        lock1 = dm.allocate_lock(data)
        lock2 = dm.allocate_lock(data)
        with lock1:
            # 互斥
            assert lock2.acquire(False) == False
            # 可重入
            assert lock1.acquire(False)
            # 安全
            with raises(RuntimeError):
                lock2.release()
        # 特征锁
        lock1 = dm.allocate_lock(data, "test_lock")
        lock2 = dm.allocate_lock(data, "test_lock")
        with lock1:
            # 互斥
            assert lock2.acquire(False) == False
            # 可重入
            assert lock1.acquire(False)
            # 安全
            with raises(RuntimeError):
                lock2.release()

    @trait_parametrize
    def test_set_get_data_trait(self, dm: DataManager, trait: TraitTuple):
        """测试数据管理器的_set_data_trait与_get_data_trait接口
        - 使用_set_data_trait设置特征为目标值
        - 再使用_get_data_trait得到值应当目标值相同
        """
        # 占位用的空数据
        data = _TestData()
        name, value = deepcopy(trait)

        # FIXME: 这里没有(可能无法做到)测试是否所有内部数据都被正确添加到管理器
        # 例如, 嵌套的数据没有被绑定到管理器时也能够通过验证, 不过可能需要外部提供所有包含的数据
        dm._set_data_trait(data, name, value)
        assert dm._get_data_trait(data, name) == value

    @trait_parametrize
    def test_delete(self, dm: DataManager, trait: TraitTuple):
        """测试数据管理器的_delete接口
        - 使用_set_data_trait设置特征为目标值
        - 调用_delete
        - 再使用_get_data_trait获取特征时会触发ValueError
        """
        data = _TestData()
        name, value = deepcopy(trait)
        dm._set_data_trait(data, name, value)

        dm._delete(data._gid)
        with raises(ValueError):
            dm._get_data_trait(data, name)

    def test_lock_unlock(self, dm: DataManager):
        """测试数据管理器的_lock接口和_unlock接口"""
        key = random.randbytes(16)
        secret = random.randbytes(16)
        secret2 = random.randbytes(16)

        # 锁定
        assert dm._lock(key, secret)
        # 互斥
        assert dm._lock(key, secret2) == False
        # 可重入
        assert dm._lock(key, secret)
        # 安全
        with raises(RuntimeError):
            assert dm._unlock(key, secret2)
        # 解锁
        dm._unlock(key, secret)
        # 未锁定不可解锁
        with raises(RuntimeError):
            assert dm._unlock(key, secret)
        # 锁定2
        assert dm._lock(key, secret2)
        # 解锁2
        dm._unlock(key, secret2)
        # 未锁定不可解锁
        with raises(RuntimeError):
            assert dm._unlock(key, secret2)


class TestLMDBDataManager(_TestDataManager):
    @fixture
    def dm(self):
        with TemporaryDirectory() as tmpdir:
            yield LMDBDataManager(path=tmpdir)
