# telemetry-airflow
Airflow is a platform to programmatically author, schedule and monitor workflows.

When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

### Deployment

Airflow can be deployed to an environment, e.g. `test`, with:
```bash
ansible-playbook ansible/deploy.yml -e '@ansible/envs/test.yml'
```

### Testing

A single task, e.g. `spark`, of an airflow dag, e.g. `example`, can be run with an execution date, e.g. `2015-06-02`, in the `test` environment with:
```bash
ansible-playbook ansible/test.yml --extra-vars "@ansible/envs/test.yml" --extra-vars "dag=example task=spark date=2015-06-02"
```
