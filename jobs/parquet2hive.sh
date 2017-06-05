#!/bin/bash

set -euo pipefail

parquet2hive -ulv 1 --sql s3://${bucket}/${prefix} | beeline -u jdbc:hive2://${HIVE_SERVER}:10000
