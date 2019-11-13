from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.contrib.operators.dataproc_operator import DataProcHadoopOperator
from airflow.utils.decorators import apply_defaults


class DataProcHadoopOperatorWithAws(DataProcHadoopOperator):
    @apply_defaults
    def __init__(self, aws_conn_id=None, **kwargs):
        super(DataProcHadoopOperatorWithAws, self).__init__(**kwargs)
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        # add s3 credentials to the job without exposing them in the airflow UI
        properties = {
            "fs.s3a." + key: value
            for key, value in zip(
                ("access.key", "secret.key", "session.token"),
                AwsHook(aws_conn_id=self.aws_conn_id).get_credentials(),
            )
            if value is not None
        }
        if self.dataproc_properties is not None:
            properties.update(self.dataproc_properties)

        # fork super().execute to reference combined properties. forked from:
        # https://github.com/apache/airflow/blob/1.10.2/airflow/contrib/operators/dataproc_operator.py#L1181-L1197
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        job = hook.create_job_template(
            self.task_id, self.cluster_name, "hadoopJob", properties
        )

        job.set_main(self.main_jar, self.main_class)
        job.add_args(self.arguments)
        job.add_jar_file_uris(self.dataproc_jars)
        job.add_archive_uris(self.archives)
        job.add_file_uris(self.files)
        job.set_job_name(self.job_name)

        job_to_submit = job.build()
        self.dataproc_job_id = job_to_submit["job"]["reference"]["jobId"]

        hook.submit(hook.project_id, job_to_submit, self.region, self.job_error_states)
