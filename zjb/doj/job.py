from enum import IntEnum
from functools import partial, wraps
from reprlib import recursive_repr
from time import sleep
from typing import TYPE_CHECKING, Any, Callable, Generator, Generic, ParamSpec, TypeVar

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
    func: "GenJobFuncType[P, R]"

    children = List(Instance(Job))

    _return = OptionalInstance(Job)

    def __init__(
        self,
        func: "GenJobFuncType[P, R]",
        *args: P.args,
        **kwargs: P.kwargs,
    ):
        super().__init__(func, *args, **kwargs)

    def __call__(self):
        try:
            func = self.func
            if hasattr(func, "__job_wrapped__"):  # 处理装饰过的函数
                if hasattr(func, "__self__"):  # bound method
                    func = partial(func.__job_wrapped__, func.__self__)
                else:
                    func = func.__job_wrapped__
            gen: "GenJobGeneratorType[R]" = func(*self.args, **self.kwargs)
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

    def _handle_return(self, gen: "GenJobGeneratorType[R]"):
        _return = yield from gen
        if isinstance(_return, Job):
            self._return = _return
        else:
            self.out = _return

    # FIXME: Worker在子作业调用该方法时中断可能会导致父作业无法完成
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


if TYPE_CHECKING:
    from typing import Protocol, Self, overload

    from .._typings import Method

    O = TypeVar("O")
    O_co = TypeVar("O_co", contravariant=True)
    CR_co = TypeVar("CR_co", covariant=True)
    JR_co = TypeVar("JR_co", covariant=True)

    class JobWrapped(Protocol[P, CR_co, JR_co]):
        __self__: Any  # just for type check

        def __job_wrapped__(self, *args: P.args, **kwds: P.kwargs) -> JR_co:
            ...

        def __call__(self, *args: P.args, **kwds: P.kwargs) -> CR_co:
            ...

    class JobWrappedMethod(Method[O_co, P, CR_co], Protocol[O_co, P, CR_co, JR_co]):
        def __job_wrapped__(
            _self, self: O_co, *args: P.args, **kwds: P.kwargs
        ) -> JR_co:
            ...

        def __call__(_self, self: O_co, *args: P.args, **kwds: P.kwargs) -> CR_co:
            ...

        @overload
        def __get__(_self, self: None, cls: type[O_co]) -> Self:
            ...

        @overload
        def __get__(_self, self: O_co, cls: type[O_co]) -> JobWrapped[P, CR_co, JR_co]:
            ...

        def __get__(_self, self: Any, cls: Any) -> Any:
            ...

    GenJobGeneratorType = Generator[Job[..., Any], None, Job[..., R] | R]
    GenJobFuncType = (
        Callable[P, GenJobGeneratorType[R]] | JobWrapped[P, R, GenJobGeneratorType[R]]
    )

    @overload
    def generator_job_wrap(
        wrapped: Method[O, P, GenJobGeneratorType[R]],
    ) -> JobWrappedMethod[O, P, R, GenJobGeneratorType[R]]:
        ...

    @overload
    def generator_job_wrap(
        wrapped: Callable[P, GenJobGeneratorType[R]],
    ) -> JobWrapped[P, R, GenJobGeneratorType[R]]:
        ...


def generator_job_wrap(
    wrapped: Any,
) -> Any:
    """装饰一个生成器作业实现的函数, 使其可以直接调用得到结果"""

    @wraps(wrapped)
    def wrapper(*args, **kwargs):
        job = GeneratorJob(wrapped, *args, **kwargs)
        job()
        if job.err:
            raise job.err
        return job.out

    # 使用__job_wrapped__存储用于产生生成器的函数
    setattr(wrapper, "__job_wrapped__", wrapped)

    return wrapper
