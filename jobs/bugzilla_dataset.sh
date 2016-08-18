#!/bin/bash

conda install psycopg2 --yes
git clone https://github.com/maurodoglio/bz2db.git
pip install -r bz2db/requirements.txt
cd bz2db && python bz2db/update_bugs.py
