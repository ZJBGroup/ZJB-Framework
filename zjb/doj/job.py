from enum import IntEnum

from traits.trait_types import Dict, Str, Tuple

from .._traits.types import OptionalInstance, TraitAny, TraitCallable, TraitEnum
from ..dos.data import Data


class JobState(IntEnum):
    NEW = 0
    PENDING = 1
    RUNNING = 11
    DONE = 21
    ERROR = -1


class Job(Data):
    func = TraitCallable(required=True)

    args = Tuple()

    kwargs = Dict(Str, TraitAny)

    out = TraitAny()

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
