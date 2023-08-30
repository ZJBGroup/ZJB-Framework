import logging
from tempfile import TemporaryDirectory
from timeit import Timer

from pytest import fixture

from tests.commons import TraitTuple, _TestData, performance_parametrize
from zjb.dos.data_manager import DataManager
from zjb.dos.lmdb_data_manager import LMDBDataManager


class _TestDataManagerPerformance:
    """测试数据管理器性能"""

    @performance_parametrize
    def test_set_data_trait(self, dm: DataManager, trait: TraitTuple):
        """测试`data.{name}={value}`与数据管理器`_set_data_trait`的性能"""
        data = _TestData(manager=dm)
        name, value = trait

        # 将特征名添加到store_traits
        data.store_traits.add(name)

        dm_name = dm.__class__.__name__

        timer = Timer(f'data.{name} = value',
                      globals=locals())
        number, costs = timer.autorange()
        logging.info(
            f"call `data.{name}=...` {number} times in {costs:.3}s for {dm_name}")

        timer = Timer('dm._set_data_trait(data, name, value)',
                      globals=locals())
        number, costs = timer.autorange()
        logging.info(
            f"call `{dm_name}._set_data_trait({name}, ...)` {number} times in {costs:.3}s")

    @performance_parametrize
    def test_get_data_trait_performance(self, dm: DataManager, trait: TraitTuple):
        """测试`data.{name}`与数据管理器`_get_data_trait`的性能"""
        data = _TestData(manager=dm)
        name, value = trait
        # 将特征名添加到store_traits
        data.store_traits.add(name)
        setattr(data, name, value)

        dm_name = dm.__class__.__name__

        timer = Timer(f'data.{name}',
                      globals=locals())
        number, costs = timer.autorange()
        logging.info(
            f"call `data.{name}` {number} times in {costs:.3}s for {dm_name}")

        timer = Timer('dm._get_data_trait(data, name)',
                      globals=locals())
        number, costs = timer.autorange()
        logging.info(
            f"call `{dm_name}._get_data_trait({name})` {number} times in {costs:.3}s")


class TestLMDBDataManagerPerformance(_TestDataManagerPerformance):

    @fixture
    def dm(self):
        with TemporaryDirectory() as tmpdir:
            yield LMDBDataManager(path=tmpdir)
