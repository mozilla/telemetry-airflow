from airflow.models import BaseOperator
import logging
import time

class SleepOperator(BaseOperator):
    def __init__(self, sleep_time=30, *args, **kwargs):
        super(SleepOperator, self).__init__(*args, **kwargs)
        self.sleep_time=sleep_time

    def execute(self, context):
        logging.info("Sleeping for {} seconds".format(self.sleep_time))
        time.sleep(self.sleep_time)
        logging.info("Done sleeping for {} seconds".format(self.sleep_time))
