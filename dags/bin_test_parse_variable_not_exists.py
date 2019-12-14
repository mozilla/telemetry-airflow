from airflow import DAG
from airflow.models import Variable
Variable.get('non_existent_variable')
