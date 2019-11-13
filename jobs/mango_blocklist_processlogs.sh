if [[ -z "$bucket" || -z "$date" || -z "$domain" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

/usr/local/bin/processlogs --domain $domain --bucket $bucket --date $date
