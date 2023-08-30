from tests.commons import TraitTuple, trait_parametrize
from zjb.dos.data import Data
from zjb.dos.wrapper import DataRef, dumps, loads


class TestWrapper:
    """测试包装器"""

    @trait_parametrize
    def test(self, trait: TraitTuple):
        """测试
            - 序列化特征项
            - 反序列化的结果应当与原特征项相同
        """

        pool = {}

        def to_ref(data: Data):
            pool[data._gid] = data
            return DataRef.from_data(data)

        def to_data(ref: DataRef):
            return pool[ref.gid]

        wrapped = dumps(trait, to_ref)
        assert loads(wrapped, to_data) == trait
