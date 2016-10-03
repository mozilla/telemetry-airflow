set -o verbose

HOME=/home/hadoop
source $HOME/.bashrc

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
        --user)
            shift
            user=$1
            ;;
        --uri)
            shift
            uri=$1
            ;;
        --arguments)
            shift
            args=$1
            ;;
        --runner-arguments)
            shift
            runner_args=$1
            ;;
        --data-bucket)
            shift
            data_bucket=$1
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

if [ -z "$job_name" ] || [ -z "$user" ] || [ -z "$uri" ] || [ -z "$data_bucket" ]; then
    error_msg "missing argument(s)"
    exit -1
fi

s3_base="s3://$data_bucket/data/$user/$job_name"

# Wait for Parquet datasets to be loaded
while pgrep -f hive_config.sh | grep -v grep > /dev/null; do sleep 1; done

wd=/mnt/analyses
mkdir -p $wd && cd $wd
mkdir -p output

urldecode() {
    local url_encoded="${1//+/ }"
    printf '%b' "${url_encoded//%/\\x}"
}

# Download file
if [[ $uri == s3://* ]]; then
    aws s3 cp "$uri" .
elif [[ $uri =~ ^https?.*$ ]]; then
    uri=$(urldecode $uri)
    wget -N "$uri"
fi

# Run job
job="${uri##*/}"
cd $wd
if [[ $uri == *.jar ]]; then
    time env $environment spark-submit $runner_args --master yarn-client "./$job" $args
    rc=$?
elif [[ $uri == *.ipynb ]]; then
    time env $environment \
    PYSPARK_DRIVER_PYTHON=jupyter \
    PYSPARK_DRIVER_PYTHON_OPTS="nbconvert --ExecutePreprocessor.timeout=-1 --to notebook --log-level=10 --execute \"./${job}\" --allow-errors --output \"./output/${job}\"" \
    pyspark
    rc=$?
    # When nbconvert is called with --allow-errors there's no way to detect if a cell raised an exception.
    # Grepping the output notebook looking for a 'error' output type does the trick.
    if [ $rc != 0 ] || [ "`grep  '\"output_type\": \"error\"' ./output/${job}`" ] ;then
        # If an error is detected, print out the notebook in a human readable (markdown) format.
        PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS="nbconvert --to markdown --stdout \"./output/${job}\"" pyspark
        rc=1
    fi
elif [[ $uri == *.py ]]; then
    time env $environment \
    PYSPARK_DRIVER_PYTHON=/home/hadoop/anaconda2/bin/python PYSPARK_DRIVER_PYTHON_OPTS= spark-submit \
    $runner_args --master yarn-client "./$job" $args
    rc=$?
else
    chmod +x "./$job"
    time env $environment "./$job" $args
    rc=$?
fi

# Upload output files
cd $wd/output
find . -iname "*" -type f | while read f
do
    # Remove the leading "./"
    f=$(sed -e "s/^\.\///" <<< $f)
    echo $f

    upload_cmd="aws s3 cp './$f' '$s3_base/$f'"

    if [[ "$f" == *.gz ]]; then
        upload_cmd="$upload_cmd --content-encoding gzip"
    fi

    eval $upload_cmd
done

if [[ $rc != 0 ]]; then
    exit $rc;
fi
