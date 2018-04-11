
if [[ -z "$bucket" || -z "$date" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

pip install py4j --upgrade

git clone https://www.github.com/mozilla/Fx_Usage_Report.git
cd Fx_Usage_Report

python usage_report/usage_report.py \
    --date $date \
    --sample 10 \
    --output-bucket $bucket
