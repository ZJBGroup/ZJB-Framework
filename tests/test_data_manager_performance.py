import logging
from tempfile import TemporaryDirectory
from timeit import Timer

from pytest import fixture, mark

from tests.commons import TraitTuple
from zjb.dos.data import Data
from zjb.dos.data_manager import DataManager
from zjb.dos.lmdb_data_manager import LMDBDataManager


class _TestDataManagerPerformance:
    """测试数据管理器性能"""

    performance_parametrize = mark.parametrize('trait', [
        ('name', 'Nature'),  # 字符串
        ('page', 1111),  # 整数
        ('price', 22.0),  # 浮点数
        ('sold', True)  # 布尔值
    ])

    @performance_parametrize
    def test_set_data_trait(self, dm: DataManager, trait: TraitTuple):
        """测试数据管理器_set_data_trait的性能"""
        data = Data()
        name, value = trait

        timer = Timer('dm._set_data_trait(data, name, value)',
                      globals=locals())
        number, costs = timer.autorange()
        logging.info(
            f"{dm.__class__.__name__} set `{name}={value}`"
            f" {number} times in {costs:.3}s")

    @performance_parametrize
    def test_get_data_trait_performance(self, dm: DataManager, trait: TraitTuple):
        """测试数据管理器_get_data_trait的性能"""
        data = Data()
        name, value = trait

        timer = Timer('dm._get_data_trait(data, name)',
                      'dm._set_data_trait(data, name, value)',
                      globals=locals())
        number, costs = timer.autorange()
        logging.info(
            f"{dm.__class__.__name__} get `{name}={value}`"
            f" {number} times in {costs:.3}s")


class TestLMDBDataManagerPerformance(_TestDataManagerPerformance):

    @fixture
    def dm(self):
        with TemporaryDirectory() as tmpdir:
            yield LMDBDataManager(path=tmpdir)
