[![CircleCi](https://circleci.com/gh/mozilla/telemetry-airflow.svg?style=shield&circle-token=62f4c1be98e5c9f36bd667edb7545fa736eed3ae)](https://circleci.com/gh/mozilla/telemetry-airflow)

# Telemetry-Airflow
Airflow is a platform to programmatically author, schedule and monitor workflows.

When workflows are defined as code, they become more maintainable, versionable,
testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks.
The Airflow scheduler executes your tasks on an array of workers while following
the specified dependencies. Rich command line utilities make performing complex
surgeries on DAGs a snap. The rich user interface makes it easy to visualize
pipelines running in production, monitor progress, and troubleshoot issues when
needed.

### Prerequisites

This app is built and deployed with
[docker](https://docs.docker.com/) and
[docker-compose](https://docs.docker.com/compose/).

### Dependencies

Most Airflow jobs are thin wrappers that spin up an EMR cluster for running
the job. Be aware that the configuration of the created EMR clusters depends
on finding scripts in an S3 location configured by the `SPARK_BUCKET` variable.
Those scripts are maintained in
[emr-bootstrap-spark](https://github.com/mozilla/emr-bootstrap-spark/)
and are deployed independently of this repository.
Changes in behavior of Airflow jobs not explained by changes in the source of the
Spark jobs or by changes in this repository
could be due to changes in the bootstrap scripts.

### Build Container

An Airflow container can be built with

```bash
make build
```

You should then run the database migrations to complete the container initialization with

```bash
make migrate
```

## Testing

A single task, e.g. `spark`, of an Airflow dag, e.g. `example`, can be run with an execution date, e.g. `2018-01-01`, in the `dev` environment with:
```bash
export DEV_USERNAME=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ACCESS_KEY_ID=...
make run COMMAND="test example spark 20180101"
```

The `DEV_USERNAME` is a short string used to identify your EMR instances.
This should be set to something like your IRC or Slack handle. 

The container will run the desired task to completion (or failure).
Note that if the container is stopped during the execution of a task,
the task will be aborted. In the example's case, the Spark job will be
terminated.

The logs of the task can be inspected in real-time with:
```bash
docker logs -f telemetryairflow_scheduler_1
```

You can see task logs and see cluster status on
[the EMR console](https://us-west-2.console.aws.amazon.com/elasticmapreduce/home?region=us-west-2)

By default, the results will end up in the `telemetry-test-bucket` in S3.
If your desired task depends on other views, it will expect to be able to find those results
in `telemetry-test-bucket` too. It's your responsibility to run the tasks in correct
order of their dependencies.

### Testing main_summary

`main_summary` can be a good test case for any large changes to telemetry-batch-view, launch in dev as:

```bash
export DEV_USERNAME=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ACCESS_KEY_ID=...
make run COMMAND="test main_summary main_summary 20180803"
```

See the next section for info on how to configure a full DAG run,
though this should only be needed to significant changes affecting many view definitions.

### Local Deployment

Assuming you're using macOS and Docker for macOS, start the docker service,
click the docker icon in the menu bar, click on preferences and change the
available memory to 4GB.

To deploy the Airflow container on the docker engine, with its required dependencies, run:
```bash
make up
```

You can now connect to your local Airflow web console at
`http://localhost:8000/`.

All DAGs are paused by default for local instances and our staging instance of Airflow.
In order to submit a DAG via the UI, you'll need to toggle the DAG from "Off" to "On".
You'll likely want to toggle the DAG back to "Off" as soon as your desired task starts running.


#### Workaround for permission issues

Users on Linux distributions will encounter permission issues with `docker-compose`.
This is because the local application folder is mounted as a volume into the running container.
The Airflow user and group in the container is set to `10001`.

To work around this, replace all instances of `10001` in `Dockerfile.dev` with the host user id.

```bash
sed -i "s/10001/$(id -u)/g" Dockerfile.dev

```

### Testing Databricks Jobs

There are a few caveats when using the `MozDatabricksRunSubmit` operator.
For local testing, the token-based authentication requires setting json in the connection's `Extra` field.

Read through [this comment](https://github.com/mozilla/telemetry-airflow/pull/337#issuecomment-413383009) for context and modify the script for your application.
The instructions will do the following:

1. Set up a single local instance of Airflow
2. Set the connection string with the host and token
3. Execute the job

There may be issues running this particular operator directly via `make run` and the underlying `docker-compose run`.

### Testing Dev Changes

*Note: This only works for `telemetry-batch-view` and `telemetry-streaming` jobs*

A dev changes can be run by simply changing the `DEPLOY_TAG` environment variable
to whichever upstream branch you've pushed your local changes to.

Afterwards, you're going to need to rebuild: `make build && make migrate`

From there, you can either set the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the 
Dockerfile and run `make up` to get a local UI and run from there, or you can follow the
testing instructions above and use `make run`.

### Production Setup

Note: the canonical reference for production environment variables lives
in [a private repository](https://github.com/mozilla-services/cloudops-deployment/blob/master/projects/data/puppet/yaml/app/data.prod.wtmo.yaml).

When deploying to production make sure to set up the following environment
variables:

- `AWS_ACCESS_KEY_ID` -- The AWS access key ID to spin up the Spark clusters
- `AWS_SECRET_ACCESS_KEY` -- The AWS secret access key
- `SPARK_BUCKET` -- The AWS S3 bucket where Spark related files are stored,
  e.g. `telemetry-spark-emr-2`
- `AIRFLOW_BUCKET` -- The AWS S3 bucket where airflow specific files are stored,
  e.g. `telemetry-airflow`
- `PUBLIC_OUTPUT_BUCKET` -- The AWS S3 bucket where public job results are
  stored in, e.g. `telemetry-public-analysis-2`
- `PRIVATE_OUTPUT_BUCKET` -- The AWS S3 bucket where private job results are
  stored in, e.g. `telemetry-parquet`
- `AIRFLOW_DATABASE_URL` -- The connection URI for the Airflow database, e.g.
  `postgres://username:password@hostname:port/password`
- `AIRFLOW_BROKER_URL` -- The connection URI for the Airflow worker queue, e.g.
  `redis://hostname:6379/0`
- `AIRFLOW_BROKER_URL` -- The connection URI for the Airflow result backend, e.g.
  `redis://hostname:6379/1`
- `AIRFLOW_GOOGLE_CLIENT_ID` -- The Google Auth client id used for
  authentication.
- `AIRFLOW_GOOGLE_CLIENT_SECRET` -- The Google Auth client secret used for
  authentication.
- `AIRFLOW_GOOGLE_APPS_DOMAIN` -- The domain(s) to restrict Google Auth login
  to e.g. `mozilla.com`
- `AIRFLOW_SMTP_HOST` -- The SMTP server to use to send emails e.g.
  `email-smtp.us-west-2.amazonaws.com`
- `AIRFLOW_SMTP_USER` -- The SMTP user name
- `AIRFLOW_SMTP_PASSWORD` --  The SMTP password
- `AIRFLOW_SMTP_FROM` -- The email address to send emails from e.g.
  `telemetry-alerts@workflow.telemetry.mozilla.org`
- `URL` -- The base URL of the website e.g.
  `https://workflow.telemetry.mozilla.org`
- `DEPLOY_ENVIRONMENT` -- The environment currently running, e.g.
  `stage` or `prod`
- `DEPLOY_TAG` -- The tag or branch to retrieve the JAR from, e.g.
  `master` or `tags`. You can specify the tag or travis build exactly as well, e.g.
  `master/42.1` or `tags/v2.2.1`. Not specifying the exact tag or build will
  use the latest from that branch, or the latest tag.
- `ARTIFACTS_BUCKET` -- The s3 bucket where the build artifacts can be found, e.g.
  `net-mozaws-data-us-west-2-ops-ci-artifacts`

Also, please set

- `AIRFLOW_SECRET_KEY` -- A secret key for Airflow's Flask based webserver
- `AIRFLOW__CORE__FERNET_KEY` -- A secret key to saving connection passwords in the DB

Both values should be set by using the cryptography module's fernet tool that
we've wrapped in a docker-compose call:

    make secret

Run this for each key config variable, and **don't use the same for both!**

### Debugging

Some useful docker tricks for development and debugging:

```bash
# Stop all docker containers:
docker stop $(docker ps -aq)

# Remove any leftover docker volumes:
docker volume rm $(docker volume ls -qf dangling=true)
```

### Triggering a task to re-run within the Airflow UI

- Check if the task / run you want to re-run is visible in the DAG's Tree View UI
  - For example, [the `main_summary` DAG tree view](http://workflow.telemetry.mozilla.org/admin/airflow/tree?num_runs=25&root=&dag_id=main_summary).
  - Hover over the little squares to find the scheduled dag run you're looking for.
- If the dag run is not showing in the Dag Tree View UI (maybe deleted)
  - Browse -> Dag Runs
  - Create (you can look at another dag run of the same dag for example values too)
    - Dag Id: the name of the dag, for example, `main_summary` or `crash_aggregates`
    - Execution Date: The date the dag should have run, for example, `2018-05-14 00:00:00`
    - Start Date: Some date between the execution date and "now", for example, `2018-05-20 00:00:05`
    - End Date: Leave it blank
    - State: success
    - Run Id: `scheduled__2018-05-14T00:00:00`
    - External Trigger: unchecked
  - Click Save
  - Click on the Graph view for the dag in question. From the main DAGs view, click the name of the DAG
  - Select the "Run Id" you just entered from the drop-down list
  - Click "Go"
  - Click each element of the DAG and "Mark Success"
  - The tasks should now show in the Tree View UI
- If the dag run is showing in the DAG's Tree View UI
  - Click on the small square for the task you want to re-run
  - **Uncheck** the "Downstream" toggle
  - Click the "Clear" button
  - Confirm that you want to clear it
  - The task should be scheduled to run again straight away.

### Triggering backfill tasks using the CLI

- SSH into the ECS container instance
- List docker containers using `docker ps`
- Log in to one of the docker containers using `docker exec -it <container_id> bash`. The web server instance is a good choice.
- Run the desired backfill command, something like `$ airflow backfill main_summary -s 2018-05-20 -e 2018-05-26`

### CircleCI

- Commits to forked repo PRs will trigger CircleCI builds that build the docker container and test python dag compilation. This should pass prior to merging.
- Every commit to master or tag will trigger a CircleCI build that will build and push the container to dockerhub
