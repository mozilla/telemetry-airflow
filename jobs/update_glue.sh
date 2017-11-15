#!/bin/bash

if [[ -z "$bucket" || -z "$prefix" || -z "$glue_access_key_id" || -z "$glue_secret_access_key" || -z "$glue_default_region" ]]; then
  echo "Missing arguments!" 1>&2
  exit 1
fi

export AWS_ACCESS_KEY_ID="$glue_access_key_id"
export AWS_SECRET_ACCESS_KEY="$glue_secret_access_key"
export AWS_DEFAULT_REGION="$glue_default_region"

: ${pdsm_version:=ef83fff23e4fecb4a0653273bc0e972a20650cc9}

pip install --upgrade --user git+https://github.com/robotblake/pdsm.git@$pdsm_version#egg=pdsm

~/.local/bin/pdsm s3://$bucket/$prefix
