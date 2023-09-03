from typing import Iterator

from zjb.dos.data import Data

from ..dos.data_manager import DataManager, DataRef
from .job import Job, JobState


class JobManager(DataManager):
    """在数据管理器的基础上, 提供额外的作业管理接口"""

    def bind(self, data: Data):
        is_job = isinstance(data, Job)
        if is_job:
            if data.state != JobState.NEW:
                raise RuntimeError(f"cannot bind non-NEW job")

        super().bind(data)

        if isinstance(data, Job):
            with data:
                data.state = JobState.PENDING

    def request(self) -> "Job | None":
        """请求一个PENDING状态的作业, 并置为RUNNING状态"""
        for ref in self._jobiter():
            job = self._unpack_ref(ref)
            if job.state != JobState.PENDING:
                continue
            with job:
                if job.state == JobState.PENDING:
                    job.state = JobState.RUNNING
                    return job

    def jobiter(self) -> Iterator[Job]:
        """遍历所有作业"""
        for ref in self._jobiter():
            yield self._unpack_ref(ref)

    def _jobiter(self) -> Iterator[DataRef[Job]]:
        """遍历所有作业(引用)"""
        for ref in self._iter():
            if issubclass(ref.type, Job):
                yield ref
