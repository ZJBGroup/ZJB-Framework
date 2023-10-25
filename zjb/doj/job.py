from enum import IntEnum
from reprlib import recursive_repr
from time import sleep
from typing import TYPE_CHECKING, Callable, Generator, Generic, ParamSpec, TypeVar

from traits.trait_types import Dict, List, Str, Tuple

from .._traits.types import (
    Instance,
    OptionalInstance,
    TraitAny,
    TraitEnum,
    TypedCallable,
    TypedInstance,
)
from ..dos.data import Data

if TYPE_CHECKING:
    from .job_manager import JobManager

P = ParamSpec("P")
R = TypeVar("R")


class JobState(IntEnum):
    NEW = 0
    PENDING = 1
    RUNNING = 11
    WAITTING = 12  # 仅用于生成器作业, 表明生成器正在等待子作业完成
    DONE = 21
    ERROR = -1


class Job(Data, Generic[P, R]):
    func = TypedCallable[P, R](required=True)

    args = Tuple()

    kwargs = Dict(Str, TraitAny)

    out: "R | None" = TraitAny()  # type: ignore

    err = OptionalInstance(Exception)

    state = TraitEnum(JobState)

    parent = TypedInstance["GeneratorJob | None"]("GeneratorJob", module=__name__)

    def __init__(self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs):
        super().__init__(func=func, args=args, kwargs=kwargs)

    def __call__(self):
        try:
            self.out = self.func(*self.args, **self.kwargs)
        except Exception as ex:
            self.err = ex
            self.state = JobState.ERROR
        else:
            self.state = JobState.DONE
        # 存在父作业时, 通知其该作业已完成
        if self.parent:
            self.parent.notify(self)

    @recursive_repr()
    def __repr__(self):
        qualname = type(self).__qualname__
        args = [repr(x) for x in self.args]
        args.extend(f"{k}={v!r}" for (k, v) in self.kwargs.items())
        return (
            f"<{qualname} {self.func.__qualname__}({', '.join(args)})"
            f" {JobState(self.state).name}>"
        )

    @property
    def done(self):
        state = self.state
        return (state == JobState.DONE) or (state == JobState.ERROR)

    def join(self, interval=1):
        """阻塞线程至至作业完成

        Parameters
        ----------
        interval : float, optional
            轮询间隔, by default 1
        """
        while not self.done:
            sleep(interval)

    def submit(self, manager: "JobManager"):
        """提交该作业到作业管理器"""
        manager.bind(self)


class JobRuntimeError(Exception):
    """作业执行异常"""

    def __init__(self, job: Job) -> None:
        super().__init__(f"error occurred while executing {job}")


class GeneratorJob(Job[P, R]):
    func: "Callable[P, Generator[Job, None, Job[..., R] | R]]"

    children = List(Instance(Job))

    _return = OptionalInstance(Job)

    def __init__(
        self,
        func: "Callable[P, Generator[Job, None, Job[..., R] | R]]",
        *args: P.args,
        **kwargs: P.kwargs,
    ):
        super().__init__(func, *args, **kwargs)

    def __call__(self):
        try:
            gen = self.func(*self.args, **self.kwargs)
            for job in self._handle_return(gen):
                # 作业已经失败则不继续执行
                if self.state == JobState.ERROR:
                    return
                job.parent = self
                self.children += [job]  # 子作业被保存到管理器
                job.state = JobState.PENDING  # 开始调度子作业
        except Exception as ex:
            self.err = ex
            self.state = JobState.ERROR
            return
        with self:
            self.state = JobState.WAITTING
            self._check_and_done()
        # FIXME: 在Data中添加接口或从外部传入参数来判断作业是否与任务管理器绑定
        # 如果Job未提交到任务管理器则顺序执行子作业
        if not self._manager:
            for child in self.children:
                child()

    def _handle_return(self, gen: "Generator[Job, None, Job[..., R] | R]"):
        _return = yield from gen
        if isinstance(_return, Job):
            self._return = _return
        else:
            self.out = _return

    def notify(self, job: Job):
        """子作业执行完成通知, 并在该作业及所有子作业完成后执行_return作业"""
        with self:
            # 子作业错误时, 该作业也被设置为错误状态
            if job.state == JobState.ERROR:
                self.err = JobRuntimeError(job)
                self.state = JobState.ERROR
                return
            # 该作业仍在运行时直接返回
            if self.state != JobState.WAITTING:
                return
            self._check_and_done()

    def _check_and_done(self):
        """检查作业是否完成并更新状态,
        如果所有子已完成且_return为作业, 则执行_return以获取输出"""
        # 还有子作业未完成时直接返回
        for child in self.children:
            if not child.done:
                return
        # 所有子作业已完成时执行_return作业
        _return = self._return
        if not _return:
            self.state = JobState.DONE
            return
        try:
            self.out = _return.func(*_return.args, **_return.kwargs)
        except Exception as ex:
            self.err = ex
            self.state = JobState.ERROR
        else:
            self.state = JobState.DONE
        # 存在父作业时, 通知其该作业已完成
        if self.parent:
            self.parent.notify(self)
