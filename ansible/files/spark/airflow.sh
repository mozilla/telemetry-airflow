set -o verbose

# Log everything
sudo chmod o+w "/mnt/var/log/spark/"
exec > >(tee -i "/mnt/var/log/spark/job_$(date +%Y%m%d%H%M%S).log")
exec 2>&1

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
            JOB_NAME=$1
            ;;
        --user)
            shift
            USER=$1
            ;;
        --notebook)
            shift
            NOTEBOOK=$1
            ;;
        --jar)
            shift
            JAR=$1
            ;;
        --spark-submit-args)
            shift
            ARGS=$1
            ;;
        --data-bucket)
            shift
            DATA_BUCKET=$1
            ;;
        --environment)
            shift
            ENVIRONMENT=$1
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

if [ -z "$JOB_NAME" ] || [ -z "$USER" ] || ([ -z "$NOTEBOOK" ] && [ -z "$JAR" ]) || ([ -n "$NOTEBOOK" ] && [ -n "$JAR" ]) || [ -z "$DATA_BUCKET" ]; then
    exit -1
fi

S3_BASE="s3://$DATA_BUCKET/$USER/$JOB_NAME"

# Wait for Parquet datasets to be loaded
while ps aux | grep hive_config.sh | grep -v grep > /dev/null; do sleep 1; done

mkdir -p $HOME/analyses && cd $HOME/analyses
mkdir -p logs
mkdir -p output

if [ -n "$JAR" ]; then
    # Run JAR
    aws s3 cp "$JAR" .
    cd output
    time env $ENVIRONMENT spark-submit --master yarn-client "../${JAR##*/}" $ARGS
else
    # Run notebook
    aws s3 cp "$NOTEBOOK" .
    cd output
    time env $ENVIRONMENT runipy "../${NOTEBOOK##*/}" "${NOTEBOOK##*/}" --pylab
fi

# Upload output files
find . -iname "*" -type f | while read f
do
    # Remove the leading "./"
    f=$(sed -e "s/^\.\///" <<< $f)
    echo $f

    UPLOAD_CMD="aws s3 cp './$f' '$S3_BASE/$f'"

    if [[ "$f" == *.gz ]]; then
        UPLOAD_CMD="$UPLOAD_CMD --content-encoding gzip"
    fi

    eval $UPLOAD_CMD
done
