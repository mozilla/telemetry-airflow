if [[ -z "$date" || -z "$deploy_environment" ]]; then

    echo "Missing arguments!" 1>&2

    exit 1

fi



pip install py4j --upgrade

pip install pandas --upgrade



git clone https://github.com/mozilla/addons_daily.git

cd addons_daily



python -m  addons_daily.addons_report --date $date
