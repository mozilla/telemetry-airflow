from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import time

class SleepOperator(BaseOperator):
    @apply_defaults
    def __init__(self, sleep_time=30, *args, **kwargs):
        super(SleepOperator, self).__init__(*args, **kwargs)
        self.sleep_time=sleep_time

    def execute(self, context):
        logging.info("Sleeping for {} seconds".format(self.sleep_time))
        time.sleep(self.sleep_time)
        logging.info("Done sleeping for {} seconds".format(self.sleep_time))
