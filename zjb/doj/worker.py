import logging
from time import sleep

from traits.has_traits import HasPrivateTraits, HasRequiredTraits
from traits.trait_types import Float

from .._traits.types import Instance
from .job_manager import JobManager

logger = logging.getLogger(__name__)


class Worker(HasPrivateTraits, HasRequiredTraits):

    manager = Instance(JobManager, required=True)

    polling_interval = Float(0.1)

    def run(self):
        while True:
            while True:
                job = self.manager.request()
                if job:
                    break
                sleep(self.polling_interval)
            job()
