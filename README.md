# Telemetry-Airflow

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/mozilla/telemetry-airflow/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/mozilla/telemetry-airflow/tree/main)
[![Python 3.11.8](https://img.shields.io/badge/python-3.11.8-blue)](https://www.python.org/)
[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v0.json)](https://github.com/charliermarsh/ruff)

[Apache Airflow](https://airflow.apache.org/) is a platform to programmatically
author, schedule and monitor workflows.

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Telemetry-Airflow](#telemetry-airflow)
  - [Contributing](#contributing)
  - [Writing DAGs](#writing-dags)
  - [Prerequisites](#prerequisites)
    - [Installing dependencies locally](#installing-dependencies-locally)
    - [Updating Python dependencies](#updating-python-dependencies)
    - [Build Container](#build-container)
      - [macOS](#macos)
  - [Testing](#testing)
    - [Local Deployment](#local-deployment)
      - [Adding dummy credentials](#adding-dummy-credentials)
      - [Usage](#usage)
      - [Testing GKE Jobs (including BigQuery-etl changes)](#testing-gke-jobs-including-bigquery-etl-changes)
      - [Testing Dataproc Jobs](#testing-dataproc-jobs)
      - [Debugging](#debugging)
  - [Production Setup](#production-setup)
  - [Production Deployments](#production-deployments)
  - [Dev and Stage Deployments](#dev-and-stage-deployments)

<!-- TOC end -->

This repository codifies the Airflow cluster that is deployed at
[workflow.telemetry.mozilla.org](https://workflow.telemetry.mozilla.org)
(behind SSO) and commonly referred to as "WTMO" or simply "Airflow".

Some links relevant to users and developers of WTMO:

- The `dags` directory in this repository contains some custom DAG definitions
- Many of the DAGs registered with WTMO don't live in this repository, but are
  instead generated from ETL task definitions in
  [bigquery-etl](https://github.com/mozilla/bigquery-etl)
- The Data SRE team maintains a
  [WTMO Developer Guide](https://mana.mozilla.org/wiki/display/DOPS/WTMO+Developer+Guide)
  (behind SSO)

## Contributing
[Forking workflow](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) 
is required **ONLY** for **contributors without write access**.

This repo enforces [Conventional Commit style](https://www.conventionalcommits.org/en/v1.0.0/) via the Github action [Semantic PRs](https://github.com/marketplace/semantic-prs).

## Writing DAGs
See the Airflow's [Best Practices guide](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#best-practices) to help you write DAGs.

**⚠ Warning: Do not import resources from the `dags` directory in DAGs definition files ⚠**

As an example, if you have `dags/dag_a.py` and `dags/dag_b.py` and want to use a helper
function in both DAG definition files, define the helper function in the `utils` directory
such as:

`utils/helper.py`
```python
def helper_function():
    return "Help"
```

`dags/dag_a.py`
```python
from airflow import DAG

from utils.helper import helper_function

with DAG("dag_a", ...):
    ...
```

`dags/dag_b.py`
```python
from airflow import DAG

from utils.helper import helper_function

with DAG("dag_b", ...):
    ...
```

WTMO deployments use git-sync sidecars to synchronize DAG files from multiple
repositories via [telemetry-airflow-dags](https://github.com/mozilla/telemetry-airflow-dags/)
using git submodules. Git-sync sidecar pattern results in the following directory structure
once deployed.
```
airflow
├─ dags
│  └── repo
│      └── telemetry-airflow-dags
│          ├── <submodule repo_a>
│          │    └── dags
│          │        └── <dag files>
│          ├── <submodule repo_b>
│          │    └── dags
│          │        └── <dag files>
│          └── <submodule repo_c>
│               └── dags
│                   └── <dag files>
├─ utils
│  └── ...
└─ plugins
   └── ...
```
Hence, defining `helper_function()` in `dags/dag_a.py` and
importing the function in `dags/dag_b.py` as `from dags.dag_a import helper_function`
**will not work after deployment** because of the directory structured required for
git-sync sidecars.


## Prerequisites

This app is built and deployed with
[docker](https://docs.docker.com/) and
[docker-compose](https://docs.docker.com/compose/).
Dependencies are managed with
[pip-tools](https://pypi.org/project/pip-tools/) `pip-compile`.

You'll also need to install PostgreSQL to build the database container.

### Installing dependencies locally
**_⚠ Make sure you use the right Python version. Refer to Dockerfile for current supported Python Version ⚠_**

You can install the project dependencies locally to run tests with Pytest. We use the
[official Airflow constraints](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#constraints-files) file to simplify
Airflow dependency management. Install dependencies locally using the following command:

```bash
make pip-install-local
```

### Updating Python dependencies

Add new Python dependencies into `requirements.in` or `requirements-dev.in` then execute the following commands:

```bash
make pip-compile
make pip-install-local
```

### Build Container

Build Airflow image with

```bash
make build
```

#### macOS
Assuming you're using Docker for Docker Desktop for macOS, start the docker service,
click the docker icon in the menu bar, click on preferences and change the
available memory to 4GB.

## Testing
### Local Deployment
To deploy the Airflow container on the docker engine, with its required dependencies, run:

```bash
make build
make up
```

#### Adding dummy credentials

Tasks often require credentials to access external credentials. For example, one may choose to store
API keys in an Airflow connection or variable. These variables are sure to exist in production but
are often not mirrored locally for logistical reasons. Providing a dummy variable is the preferred
way to keep the local development environment up to date.

Update the `resources/dev_variables.env` and `resources/dev_connections.env` with appropriate strings to
prevent broken workflows.


#### Usage

You can now connect to your local Airflow web console at
`http://localhost:8080/`.

All DAGs are paused by default for local instances and our staging instance of Airflow.
In order to submit a DAG via the UI, you'll need to toggle the DAG from "Off" to "On".
You'll likely want to toggle the DAG back to "Off" as soon as your desired task starts running.


#### Testing GKE Jobs (including BigQuery-etl changes)

See https://go.corp.mozilla.com/wtmodev for more details.

```
make build && make up
make gke

When done:
make clean-gke
```

From there, [connect to Airflow](localhost:8080) and enable your job.

#### Testing Dataproc Jobs

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

You can then connect to Airflow [locally](localhost:8080). Enable your DAG and see that it runs correctly.

#### Debugging

Some useful docker tricks for development and debugging:

```bash
make clean

# Remove any leftover docker volumes:
docker volume rm $(docker volume ls -qf dangling=true)

# Purge docker volumes (helps with postgres container failing to start)
# Careful as this will purge all local volumes not used by at least one container.
docker volume prune
```

## Production Setup
This repository was structured to be deployed using the [offical Airflow Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html).
See the [Production Guide](https://airflow.apache.org/docs/helm-chart/stable/production-guide.html) for best practices.

## Production Deployments
Production deployments are automatically triggered for every commit merged in the `main` branch.
Docker images are tagged using the git commit short SHA and automatically deployed.

Refer to the CI configuration for more details!

## Dev and Stage Deployments
Non-production deployments are automatically triggered for every commit merged in the `main` branch.
Dev and Stage deployments use the `latest` image tag for deployments.

It is also possible to manually trigger the image building and pushing workflow `manual-publish`
by manually tagging a commit. This can be achieved using git on your local machine, or by creating a
[pre-release GitHub Release](https://github.com/mozilla/telemetry-airflow/releases/new) with a tag
prefixed by `dev-` on a non-main branch commit.

Refer to the CI configuration and [deployment repository](https://github.com/mozilla-sre-deploy/deploy-telemetry-airflow/)
for more details!
