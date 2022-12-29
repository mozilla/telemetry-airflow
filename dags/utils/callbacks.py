from typing import Optional

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import clear_task_instances
from airflow.utils.context import Context
from airflow.utils.db import provide_session
from sqlalchemy.orm.session import Session


@provide_session
def retry_tasks_callback(context: Context, session: Optional[Session] = None) -> None:
    """Callback to clear tasks specified by the `retry_tasks` task param.
    
    Intended to be used to as an `on_retry_callback` to also retry other tasks when a task fails.
    """
    retry_task_ids: list[str] = context['params'].get('retry_tasks', [])
    if isinstance(retry_task_ids, str):
        retry_task_ids = [retry_task_ids]
    dag_run: DagRun = context['dag_run']
    retry_task_instances = [
        task_instance
        for task_instance in dag_run.get_task_instances(session=session)
        if task_instance.task_id in retry_task_ids
    ]
    if retry_task_instances:
        clear_task_instances(retry_task_instances, session=session)
