import logging
from multiprocessing import Process
from time import sleep

from traits.has_traits import HasPrivateTraits, HasRequiredTraits
from traits.trait_types import Float

from .._traits.types import Instance
from .job_manager import JobManager

logger = logging.getLogger(__name__)


class Worker(HasPrivateTraits, HasRequiredTraits):

    manager = Instance(JobManager, required=True)

    polling_interval = Float(0.1)

    process = Instance(Process)

    def run(self):
        while True:
            while True:
                # 先sleep是为了可以在一个job完成后立即处理中断
                sleep(self.polling_interval)
                job = self.manager.request()
                if job:
                    break
            job()

    def start(self):
        self.process.start()

    def terminal(self):
        self.process.terminate()

    def kill(self):
        self.process.kill()

    def _process_default(self):
        return Process(target=self.run, daemon=True)
