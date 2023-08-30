from tempfile import TemporaryDirectory

from pytest import fixture

from tests.commons import (TraitsDict, TraitTuple, _TestData,
                           trait_parametrize, traits_parametrize)
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
        data = _TestData(**traits)
        assert data._manager is None

        dm.bind(data)
        assert data._manager is dm

        for name, value in traits.items():
            assert dm._get_data_trait(data, name) == value

    @trait_parametrize
    def test_set_get_data_trait(self, dm: DataManager, trait: TraitTuple):
        """测试数据管理器的_set_data_trait与_get_data_trait接口
            - 使用_set_data_trait设置特征为目标值
            - 再使用_get_data_trait得到值应当目标值相同
        """
        # 占位用的空数据
        data = _TestData()
        name, value = trait

        dm._set_data_trait(data, name, value)
        assert dm._get_data_trait(data, name) == value


class TestLMDBDataManager(_TestDataManager):

    @fixture
    def dm(self):
        with TemporaryDirectory() as tmpdir:
            yield LMDBDataManager(path=tmpdir)
