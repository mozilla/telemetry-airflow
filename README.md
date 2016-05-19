# telemetry-airflow
Airflow is a platform to programmatically author, schedule and monitor workflows.

When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

### Local Deployment

```bash
ansible-playbook ansible/deploy_local.yml -e '@ansible/envs/test.yml'
```

### Remote Deployment

In order to deploy Airflow to e.g. the test environment, an ECS cluster has to be created first with at least one container instance:
```bash
ansible-playbook ansible/provision_aws.yml -e '@ansible/envs/test.yml'
```

Once the ECS cluster is up and running, Airflow can be (re)deployed with:
```bash
ansible-playbook ansible/deploy_aws.yml -e '@ansible/envs/test.yml'
```

### Testing

A single task, e.g. `spark`, of an airflow dag, e.g. `example`, can be run with an execution date, e.g. `2015-06-02`, in the `test` environment with:
```bash
ansible-playbook ansible/test.yml --extra-vars "@ansible/envs/test.yml" --extra-vars "dag=example task=spark date=2015-06-02"
```
