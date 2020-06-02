# Telemetry-Airflow

[![CircleCi](https://circleci.com/gh/mozilla/telemetry-airflow.svg?style=shield&circle-token=62f4c1be98e5c9f36bd667edb7545fa736eed3ae)](https://circleci.com/gh/mozilla/telemetry-airflow)

Airflow is a platform to programmatically author, schedule and monitor workflows.

When workflows are defined as code, they become more maintainable, versionable,
testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks.
The Airflow scheduler executes your tasks on an array of workers while following
the specified dependencies. Rich command line utilities make performing complex
surgeries on DAGs a snap. The rich user interface makes it easy to visualize
pipelines running in production, monitor progress, and troubleshoot issues Øhen
needed.

## Prerequisites

This app is built and deployed with
[docker](https://docs.docker.com/) and
[docker-compose](https://docs.docker.com/compose/).

### Updating Python dependencies

Add new Python dependencies into `requirements.in`. Run the following commands with the same Python
version specified by the Dockerfile.

```bash
# As of time of writing, python3.7
pip install pip-tools
pip-compile
```

### Build Container

An Airflow container can be built with

```bash
make build
```

### Export Credentials

For now, DAGs that use the Databricks operator won't parse until the following environment variables are set (see issue #501):

```
AWS_SECRET_ACCESS_KEY
AWS_ACCESS_KEY_ID
DB_TOKEN
```

### Migrate Database

Airflow database migration is no longer a separate step for dev but is run by the web container if necessary on first run. That means, however, that you should run the web container (and the database container, of course) and wait for the database migrations to complete before running individual test commands per below. The easiest way to do this is to run `make up` and let it run until the migrations complete.

## Testing

A single task, e.g. `spark`, of an Airflow dag, e.g. `example`, can be run with an execution date, e.g. `2018-01-01`, in the `dev` environment with:

```bash
make run COMMAND="test example spark 20180101"
```

```bash
docker logs -f telemetryairflow_scheduler_1
```

### Adding dummy credentials

Tasks often require credentials to access external credentials. For example, one may choose to store
API keys in an Airflow connection or variable. These variables are sure to exist in production but
are often not mirrored locally for logistical reasons. Providing a dummy variable is the preferred
way to keep the local development environment up to date.

In `bin/run`, please update the `init_connections` and `init_variables` with appropriate strings to
prevent broken workflows. To test this, run `bin/test-parse` to check for errors. You may manually
test this by restarting the orchestrated containers and checking for error messages within the main
administration UI at `localhost:8000`.

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

To run a job running on Databricks, run `make up` in the background. Follow
[this guide on generating a
token](https://docs.databricks.com/api/latest/authentication.html#generate-a-token)
and save this to a secure location. Export the token to a an environment
variable:

```bash
export DB_TOKEN=<TOKEN>
```

Finally, run the testing command using docker-compose directly:

```bash
docker-compose exec web airflow test example spark 20180101
```

### Testing GKE Jobs (including BigQuery-etl changes)

For now, follow the steps outlined here to create a service account: https://bugzilla.mozilla.org/show_bug.cgi?id=1553559#c1.

Enable that service account in Airflow with the following:

```
make build && make up
./bin/add_gcp_creds $GOOGLE_APPLICATION_CREDENTIALS
```

From there, [connect to Airflow](localhost:8000) and enable your job.

### Testing Dataproc Jobs

Dataproc jobs run on a self-contained Dataproc cluster, created by Airflow.

To test these, jobs, you'll need a sandbox account and corresponding service account.
For information on creating that, see "Testing GKE Jobs". Your service account
will need Dataproc and GCS permissions (and BigQuery, if you're connecting to it). _Note_: Dataproc requires "Dataproc/Dataproc Worker"
as well as Compute Admin permissions.
You'll need to ensure that the Dataproc API is [enabled in your sandbox project.](https://console.developers.google.com/apis/api/dataproc.googleapis.com)

Ensure that your dataproc job has a configurable project to write to.
Set the project in the DAG entry to be configured based on development environment;
see the `ltv.py` job for an example of that.

From there, run the following:

```bash
make build && make up
./bin/add_gcp_creds $GOOGLE_APPLICATION_CREDENTIALS google_cloud_airflow_dataproc
```

You can then connect to Airflow [locally](localhost:8000). Enable your DAG and see that it runs correctly.

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
  `mysql://username:password@hostname:port/database`
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
- `AIRFLOW_SMTP_PASSWORD` -- The SMTP password
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

```bash
make secret
```

Run this for each key config variable, and **don't use the same for both!**

### Debugging

Some useful docker tricks for development and debugging:

```bash
# Stop all docker containers:
docker stop $(docker ps -aq)

# Remove any leftover docker volumes:
docker volume rm $(docker volume ls -qf dangling=true)

# Purge docker volumes (helps with mysql container failing to start)
# Careful as this will purge all local volumes not used by at least one container.
docker volume prune
```

Failing CircleCI 'test-environment' check:

```bash
# These commands are from the bin/test-parse script (get_errors_in_listing)
# If --detach is unavailable,  make sure you are running the latest version of docker-compose
docker-compose up --detach

docker-compose logs --follow --tail 0 | sed -n '/\[testing_stage_0\]/q'

# Don't pipe to grep to see the full output including your errors
docker-compose exec web airflow list_dags
```

### Triggering a task to re-run within the Airflow UI

- Check if the task / run you want to re-run is visible in the DAG's Tree View UI
  - For example, [the `main_summary` DAG tree view](http://workflow.telemetry.mozilla.org/admin/airflow/tree?num_runs=25&root=&dag_id=main_summary).
  - Hover over the little squares to find the scheduled dag run you're looking for.
- If the dag run is not showing in the Dag Tree View UI (maybe deleted)
  - Browse -> Dag Runs
  - Create (you can look at another dag run of the same dag for example values too)
    - Dag Id: the name of the dag, for example, `main_summary`
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
