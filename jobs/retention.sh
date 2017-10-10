#!/bin/bash

# Bug 1381840 - 1-Day Firefox Retention runner
# 
# This script runs the corresponding mozetl and telemetry-batch-view jobs.
# These two jobs could be individual nodes in the airflow DAG, however the jobs
# uses local cluster memory to pass intermediate data to each other. All dag
# nodes (as of this revision) run on independent machines. 


set -eou pipefail
set -x

MOZETL_COMMAND=${MOZETL_COMMAND:-retention}

# The environment variables are passed into this script according to the Click
# `auto_envvar_prefix` convention.
export MOZETL_RETENTION_START_DATE=${MOZETL_RETENTION_START_DATE?"start_date must be set"}
export MOZETL_RETENTION_BUCKET=${MOZETL_RETENTION_BUCKET?"output bucket must be set"}
export MOZETL_RETENTION_PATH="intermediate_path"

# NOTE: the development branch is under acmiyaguchi/<repo>:bug-1381840-retention
MOZETL_GIT_BRANCH=master
MOZETL_GIT_PATH=https://github.com/mozilla/python_mozetl.git
TBV_GIT_PATH=https://github.com/mozilla/telemetry-batch-view.git 

# Variables that are set in the prepare methods
MOZETL_RUNNER_PATH=
MOZETL_EGG_PATH=
TBV_JAR_PATH=


prepare_mozetl () {
    # fetch and build mozetl by generating the egg
    git clone ${MOZETL_GIT_PATH} --branch ${MOZETL_GIT_BRANCH}
    cd python_mozetl
    pip install .
    python setup.py bdist_egg
    local rel_egg_path=`ls dist/*.egg`
    MOZETL_EGG_PATH=`realpath ${rel_egg_path}`
}

prepare_tbv () {
    # fetch and build tbv
    git clone ${TBV_GIT_PATH} --branch ${MOZETL_GIT_BRANCH}
    cd telemetry-batch-view
    sbt assembly
    local rel_jar_path=`ls target/scala-*/telemetry-batch-view-*.jar`
    TBV_JAR_PATH=`realpath ${rel_jar_path}`
}

run_mozetl () {
    unset PYSPARK_DRIVER_PYTHON
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --py-files ${MOZETL_EGG_PATH} \
        ${MOZETL_RUNNER_PATH} ${MOZETL_COMMAND}
}

run_tbv () {
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --class com.mozilla.telemetry.views.RetentionView \
        ${TBV_JAR_PATH}  \
        --date ${MOZETL_RETENTION_START_DATE} \
        --input ${MOZETL_RETENTION_PATH} \
        --bucket ${MOZETL_RETENTION_BUCKET}
}

main () {
    # create a temporary directory for work
    workdir=$(mktemp -d -t tmp.XXXXXXXXXX)
    cleanup () { rm -rf "$workdir" ; }
    trap cleanup EXIT

    MOZETL_RUNNER_PATH=${workdir}/runner.py
    cat <<EOT >> $MOZETL_RUNNER_PATH
from mozetl import cli
cli.entry_point(auto_envvar_prefix="MOZETL")
EOT

    # reset directory after each setup call 
    cd ${workdir} && prepare_mozetl
    cd ${workdir} && prepare_tbv
    cd ${workdir}

    local start_date=${1:-${MOZETL_RETENTION_START_DATE}}
    local end_date=${2:-${start_date}}
    end_date=`date -d "$end_date + 1 day" +%Y%m%d`

    local current_date=${start_date}
    while [ "$current_date" != "$end_date" ]; do
        echo Running job for $current_date
        export MOZETL_RETENTION_START_DATE=$current_date
        run_mozetl
        run_tbv
        current_date=`date -d "$current_date + 1 day" +%Y%m%d`
    done
}

main "$@"
