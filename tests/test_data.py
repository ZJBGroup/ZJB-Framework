from tests.commons import (DictDataManager, TraitsDict, _TestData,
                           traits_parametrize)


class TestDataInterface:
    """测试数据接口"""

    @traits_parametrize
    def test_set(self, traits: TraitsDict):
        """测试特征赋值
            - 使用setattr对数据特征赋值
            - 使用数据管理器接口获取的数据特征值应当于赋值一致
        由于为使用特征获取,该测试不受其影响
        """
        dm = DictDataManager()
        data = _TestData(manager=dm)

        # 将traits中的特征添加到store_traits
        data.store_traits |= traits.keys()

        for name, value in traits.items():
            setattr(data, name, value)
            assert dm._get_data_trait(data, name) == value

    @traits_parametrize
    def test_get(self, traits: TraitsDict):
        """测试特征获取
            - 直接通过数据管理器接口设置特征值
            - 使用getattr获取特征值应当与设置值一致
        由于未使用特征赋值,该测试不受其影响
        """
        dm = DictDataManager()
        data = _TestData(manager=dm)

        # 将traits中的特征添加到store_traits
        data.store_traits |= traits.keys()

        for name, value in traits.items():
            dm._set_data_trait(data, name, value)
            assert getattr(data, name) == value

    def test_unbind(self):
        """测试解绑定"""

        dm = DictDataManager()
        data = _TestData(manager=dm, test_anything="anything")

        assert data._manager is dm

        data.unbind()

        assert data._manager is None
