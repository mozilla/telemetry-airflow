
if [[ -z "$bucket" || -z "$date" || -z "$deploy_environment" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

pip install py4j --upgrade
pip install pandas==0.24 --upgrade

git clone https://www.github.com/mozilla/Fx_Usage_Report.git
cd Fx_Usage_Report

python usage_report/usage_report.py \
    --date $date \
    --sample 10 \
    --output-bucket $bucket \
    --output-prefix "$deploy_environment/usage_report_data"
