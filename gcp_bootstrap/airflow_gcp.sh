#!/usr/bin/env bash
# to be copied to gs://moz-fx-data-prod-airflow-artifacts/airflow_gcp.sh

set -exo pipefail

# Error message
error_msg ()
{
    echo 1>&2 "Error: $1"
}

# Parse arguments
while [ $# -gt 0 ]; do
    case "$1" in
        --job-name)
            shift
            job_name=$1
            ;;
        --uri)
            shift
            uri=$1
            ;;
        --arguments)
            shift
            args=$1
            ;;
        --environment)
            shift
            environment=$1
            ;;
         -*)
            # do not exit out, just note failure
            error_msg "unrecognized option: $1"
            ;;
          *)
            break;
            ;;
    esac
    shift
done

if [ -z "$job_name" ] || [ -z "$uri" ]; then
    error_msg "missing argument(s)"
    exit 1
fi

wd=/mnt/analyses
mkdir -p $wd && cd $wd
mkdir -p output

urldecode() {
    local url_encoded="${1//+/ }"
    printf '%b' "${url_encoded//%/\\x}"
}

# Download file
if [[ $uri =~ ^gs.*$ ]]; then
    gsutil cp "$uri" .
elif [[ $uri =~ ^https?.*$ ]]; then
    uri=$(urldecode $uri)
    wget -N "$uri"
fi

# Run job
job="${uri##*/}"

if [[ $uri == *.jar ]]; then
    time env $environment spark-submit --master yarn "./$job" $args
elif [[ $uri == *.ipynb ]]; then
    echo "We are no longer supporting running ipynb's via GCP dataproc."
    exit 1
elif [[ $uri == *.py ]]; then
    time env $environment \
    PYSPARK_DRIVER_PYTHON=/opt/conda/default/bin/python PYSPARK_DRIVER_PYTHON_OPTS="" spark-submit \
    --master yarn "./$job" $args
else
    chmod +x "./$job"
    time env $environment "./$job" $args
fi
