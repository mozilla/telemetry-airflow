set -o verbose

# Log everything
spark_log_dir=/mnt/var/log/spark
sudo chmod o+w $spark_log_dir
rm -f $spark_log_dir/*.log
exec > "$spark_log_dir/job_$(date +%Y%m%d%H%M%S).log"
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
while ps aux | grep hive_config.sh | grep -v grep > /dev/null; do sleep 1; done

mkdir -p $HOME/analyses && cd $HOME/analyses
mkdir -p logs
mkdir -p output

urldecode() {
    local url_encoded="${1//+/ }"
    printf '%b' "${url_encoded//%/\\x}"
}

# Download file
if [[ $uri == s3://* ]]; then
    aws s3 cp "$uri" .
elif [[ $uri == http://* ]]; then
    uri=$(urldecode $uri)
    wget -N "$uri"
elif [[ $uri == https://* ]]; then
    uri=$(urldecode $uri)
    wget -N "$uri"
fi

# Run job
job="${uri##*/}"
cd output

if [[ $uri == *.jar ]]; then
    time env $environment spark-submit --master yarn-client "../$job" $args
elif [[ $uri == *.ipynb ]]; then
    time env $environment runipy "../$job" "$job" --pylab $args
else
    echo "Job type not supported"
    exit 1;
fi

rc=$?
if [[ $rc != 0 ]]; then
    exit $rc;
fi

# Upload output files
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
