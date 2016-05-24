# telemetry-airflow
Airflow is a platform to programmatically author, schedule and monitor workflows.

When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

### Build Container

An Airflow container can be built with 

```bash
docker build -t vitillo/telemetry-airflow .
```

and pushed to Docker hub with
```bash
docker push vitillo/telemetry-airflow
```

### Testing

A single task, e.g. `spark`, of an Airflow dag, e.g. `example`, can be run with an execution date, e.g. `2016-01-01`, in the `test` environment with:
```bash
AWS_SECRET_ACCESS_KEY=... AWS_ACCESS_KEY_ID=... \
ansible-playbook ansible/test.yml -e '@ansible/envs/test.yml' -e "dag=example task=spark date=20160101"
```

The container will run the desired task to completion (or failure). Note that if the container is stopped during the execution of a task, the task will
be aborted. In the example's case, the Spark job will be terminated. 

The logs of the task can be inspected in real-time with:
```bash
docker logs -f files_scheduler_1
```

### Local Deployment

Assuming you are on OS X, first create a docker machine with a sufficient amount of memory with e.g.:
```bash
docker-machine create --d virtualbox --virtualbox-memory 4096 default
```

To deploy the Airflow container on the docker engine, with its required dependencies, run:
```bash
AWS_SECRET_ACCESS_KEY=... AWS_ACCESS_KEY_ID=... \
ansible-playbook ansible/deploy_local.yml -e '@ansible/envs/test.yml'
```

### Remote Deployment

In order to deploy Airflow to e.g. the `test` environment, an ECS cluster has to be created first with at least one container instance:
```bash
ansible-playbook ansible/provision_aws.yml -e '@ansible/envs/test.yml'
```

Once the ECS cluster is up and running, Airflow can be (re)deployed with:
```bash
ansible-playbook ansible/deploy_aws.yml -e '@ansible/envs/test.yml'
```
