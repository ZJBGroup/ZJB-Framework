import logging

from pytest import mark

from tests.commons import Book, DataFactory, DictDataManager, TraitsDict

logger = logging.getLogger(__name__)


class TestDataInterface:
    """测试数据接口"""

    parametrize = mark.parametrize('data_factory, traits', [
        (
            Book,
            {
                'name': 'Nature',
                'page': 1111,
                'price': 22,
                'sold': True
            }
        ),
    ])

    @parametrize
    def test_set(self, data_factory: DataFactory, traits: TraitsDict):
        """测试特征赋值"""
        dm = DictDataManager()
        data = data_factory()
        dm.bind(data)
        for name, value in traits.items():
            setattr(data, name, value)
            assert dm._get_data_trait(data, name) == value

    @parametrize
    def test_get(self, data_factory: DataFactory, traits: TraitsDict):
        """测试特征获取"""
        dm = DictDataManager()
        data = data_factory()
        dm.bind(data)
        for name, value in traits.items():
            dm._set_data_trait(data, name, value)
            assert getattr(data, name) == value
