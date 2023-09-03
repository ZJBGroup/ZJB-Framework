from enum import IntEnum

from traits.trait_types import Any, Dict
from traits.trait_types import Enum as TraitEnum
from traits.trait_types import Str, Tuple

from .._traits.types import Callable, OptionalInstance
from ..dos.data import Data


class JobState(IntEnum):
    NEW = 0
    PENDING = 1
    RUNNING = 11
    DONE = 21
    ERROR = -1


class Job(Data):

    func = Callable(required=True)

    args = Tuple()

    kwargs = Dict(Str, Any)

    out = Any()

    err = OptionalInstance(Exception)

    state = TraitEnum(JobState)

    def __call__(self):
        try:
            self.out = self.func(*self.args, **self.kwargs)
        except Exception as ex:
            self.err = ex
            self.state = JobState.ERROR
            return
        self.state = JobState.DONE
