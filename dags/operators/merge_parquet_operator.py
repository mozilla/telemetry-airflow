from airflow.models import BaseOperator
import uuid
import os
from airflow.utils.decorators import apply_defaults
import pyarrow.parquet as pq


class MergeParquetOperator(BaseOperator):
    """
    Merge parquet files to create a new file of a specified size
    :param directory: the directory or partition of the files to merge
    :type directory: string
    :param new_directory: the directory you want to write the merged files to
    :type new_directory: string
    :param file_size: the desired size of the aggregated parquet file in bytes
    :type file_size: integer
    """
    @apply_defaults
    def __init__(self, directory, new_directory, file_size, *args, **kwargs):

        super(MergeParquetOperator, self).__init__(*args, **kwargs)
        self.directory = directory
        self.file_list = []
        self.parquet_list = []
        self.new_directory = new_directory
        self.aggregated_file_size = int(file_size)

    #def execute(self, context):
        for root, dirs, files in os.walk(self.directory):
            for file in files:
                if os.path.splitext(file)[1] == '.parquet':
                    self.file_list.append(file)
        self.file_length = len(self.file_list)
        while self.file_length >= 1:
            self.additive_size = os.stat(os.path.join(self.directory, (self.file_list[0]))).st_size
            if self.additive_size < self.aggregated_file_size:
                file1 = os.path.join(self.directory, self.file_list[0])
                self.parquet_list.append(file1)
                self.file_list.pop(0)
                self.additive_size = self.additive_size + os.stat(os.path.join(self.directory, (self.file_list[0]))).st_size
                self.file_length = len(self.file_list)
            if self.additive_size >= self.aggregated_file_size:
                if len(self.parquet_list) >= 0:
                    df = pq.ParquetDataset(self.parquet_list)
                    self.uuid_id = str(uuid.uuid1())
                    table = df.read()
                    filepath = os.path.join(self.new_directory, self.uuid_id+'.parquet')
                    pq.write_table(table, filepath, flavor="spark")
                    self.parquet_list = []
                else:
                    self.file_list.pop(0)
                    self.file_length = len(self.file_list)

            if self.file_length <= 1:
                if len(self.parquet_list) >=0:
                    df = pq.ParquetDataset(self.parquet_list)
                    self.uuid_id = str(uuid.uuid1())
                    table = df.read()
                    filepath = os.path.join(self.new_directory, self.uuid_id+'.parquet')
                    pq.write_table(table, filepath)
                for i in range(self.file_length):
                    file = os.path.join(self.directory, self.file_list[i])
                    os.rename(file, os.path.join(self.new_directory, self.file_list[0]))

                break



