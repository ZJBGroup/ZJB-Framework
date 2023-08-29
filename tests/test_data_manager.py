from tempfile import TemporaryDirectory

from pytest import fixture, mark

from tests.commons import Book, DataFactory, TraitsDict, TraitTuple
from zjb.dos.data import Data
from zjb.dos.data_manager import DataManager
from zjb.dos.lmdb_data_manager import LMDBDataManager


class _TestDataManager:
    """测试数据管理器"""

    @mark.parametrize('data_factory', [Book,])
    def test_bind(self, dm: DataManager, data_factory: DataFactory):
        """测试数据管理器bind接口
            - 构造一个新的数据
            - 记录数据的store特征字典
            - 数据绑定到数据管理器
            - 数据的_manager属性应当为数据管理器
            - 从数据管理器获取的store特征应当与记录的字典中的值一致
        """
        data = data_factory()
        assert data._manager is None
        traits: TraitsDict = data.trait_get(*data.store_traits)

        dm.bind(data)
        assert data._manager is dm

        for name, value in traits.items():
            assert dm._get_data_trait(data, name) == value

    @mark.parametrize('trait', [
        ('name', 'Nature'),  # 字符串
        ('page', 1111),  # 整数
        ('price', 22.0),  # 浮点数
        ('sold', True)  # 布尔值
    ])
    def test_set_get_data_trait(self, dm: DataManager, trait: TraitTuple):
        """测试数据管理器的_set_data_trait与_get_data_trait接口
            - 使用_set_data_trait设置特征为目标值
            - 再使用_get_data_trait得到值应当目标值相同
        """
        # 占位用的空数据
        data = Data()
        name, value = trait

        dm._set_data_trait(data, name, value)
        assert dm._get_data_trait(data, name) == value


class TestLMDBDataManager(_TestDataManager):

    @fixture
    def dm(self):
        with TemporaryDirectory() as tmpdir:
            yield LMDBDataManager(path=tmpdir)
