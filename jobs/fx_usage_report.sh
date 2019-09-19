
if [[ -z "$bucket" || -z "$date" || -z "$deploy_environment" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

pip install py4j --upgrade
pip install numpy==1.16.4
pip install python-dateutil==2.5.0
pip install pytz==2011k
pip install --no-dependencies pandas==0.24

git clone https://www.github.com/mozilla/Fx_Usage_Report.git
cd Fx_Usage_Report

python usage_report/usage_report.py \
    --date $date \
    --sample 10 \
    --output-bucket $bucket \
    --output-prefix "$deploy_environment/usage_report_data"
    --spark-provider 'dataproc'
