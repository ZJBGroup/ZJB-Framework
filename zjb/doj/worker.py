import logging
from multiprocessing import Process, Semaphore
from time import sleep

from traits.has_traits import HasPrivateTraits, HasRequiredTraits
from traits.trait_types import Any as TraitAny
from traits.trait_types import Float

from .._traits.types import Instance
from .job_manager import JobManager

logger = logging.getLogger(__name__)


class Worker(HasPrivateTraits, HasRequiredTraits):
    manager = Instance(JobManager, required=True)

    polling_interval = Float(0.1)

    process = Instance(Process)

    # 用于判断Worker是否空闲的信号量
    sem = TraitAny()

    def run(self):
        while True:
            with self.sem:
                job = self.manager.request()
                if job:
                    job()
            sleep(self.polling_interval)

    def start(self):
        self.process.start()

    def terminate(self, force=False) -> bool:
        """尝试终止Worker, 发送SIGTERM到子进程并等待进程终止

        Parameters
        ----------
        force : bool, optional
            当为True时, 总是终止Worker
            反之仅在Worker空闲时终止, by default True

        Returns
        -------
        bool
            子进程被终止时返回True, 否则返回False
        """
        if force:
            self.process.terminate()
            self.process.join()
            return True
        lock = self.sem.acquire(False)
        if lock:
            self.process.terminate()
            self.process.join()
            self.sem.release()
        return lock

    def is_idle(self) -> bool:
        if not self.sem:
            return True
        idle = self.sem.acquire(False)
        if idle:
            self.sem.release()
        return idle

    def _process_default(self):
        self.sem = Semaphore(1)
        return Process(target=self.run, daemon=True)
