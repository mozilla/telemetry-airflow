from airflow.models import BaseOperator

import subprocess
import uuid
import os
from airflow import configuration as conf
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class MergeParquetOperator(BaseOperator):
    """
    Merge parquet files
    :param directory: the directory or partition of the files to merge
    :param directory: string
    :param new_directory: the directory you want to move the old unmerged files to
    :param new_directory: string


    """

    @apply_defaults
    def __init__(self, directory, new_directory, *args, **kwargs):

        super(MergeParquetOperator, self).__init__(*args, **kwargs)
        self.directory = directory
        self.file_list = []
        self.new_directory = new_directory

    def execute(self, context):
        for root, dirs, files in os.walk(self.directory):
            for file in files:
                if os.path.splitext(file)[1] == '.parquet':
                    self.file_list.append(file)

        self.file_length = len(self.file_list)
        while self.file_length > 1:
            self.file_size = os.stat(os.path.join(self.directory, (self.file_list[0]))).st_size

            self.file_size = self.file_size + os.stat(os.path.join(self.directory, (self.file_list[1]))).st_size

            if self.file_size < 200000000:

                bash_command1 = 'parquet-tools merge '

                file1 = os.path.join(self.directory, self.file_list[0])
                file2 = os.path.join(self.directory, self.file_list[1])
                self.uuid_id = str(uuid.uuid1())

                bash_command = bash_command1 + file1 + " " + file2 + " " + self.directory + self.uuid_id+'.parquet'
                self.file = subprocess.call(['bash', '-c', bash_command])

                os.rename(file1, os.path.join(self.new_directory, self.file_list[0]))
                os.rename(file2, os.path.join(self.new_directory, self.file_list[1]))

                self.file_list.pop(0)
                self.file_list.pop(0)
                self.file_list.insert(0, self.uuid_id+".parquet")

                self.file_length = len(self.file_list)

            else:
                self.file_list.pop(0)
                self.file_length = len(self.file_list)

            if self.file_length < 1:
                break
