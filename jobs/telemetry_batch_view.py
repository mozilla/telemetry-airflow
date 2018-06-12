#!/usr/bin/env python

import requests
from os import chdir
from os import environ
from subprocess import call, PIPE, Popen


artifact_file = "artifact.jar"


def call_exit_errors(command):
    print("+ {}".format(" ".join(command)))
    rc = call(command)
    if rc > 0:
        exit(rc)


def retrieve_jar():
    jar_url = environ.get("ARTIFACT_URL")

    if jar_url is None:
        exit(1)

    response = requests.get(jar_url)
    with open(artifact_file, 'wb') as f:
        f.write(response.content)


def submit_job():
    command = sum([
        ["--{}".format(key[4:].replace("_", "-")), value]
        for key, value in environ.items()
        if key.startswith("TBV_") and key != "TBV_CLASS"
    ], [
        "spark-submit",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--class", environ["TBV_CLASS"],
        artifact_file,
    ])

    call_exit_errors(command)


def update_metastore(location, hive_server):
    p2h_cmd = ("parquet2hive", "-ulv=1", "--sql", location)
    beeline_cmd = ("beeline", "-u", "jdbc:hive2://{}:10000".format(hive_server))
    print("+ {}".format(" ".join(p2h_cmd)))
    p2h = Popen(p2h_cmd, stdout=PIPE)
    print("+ {}".format(" ".join(beeline_cmd)))
    rc = call(beeline_cmd, stdin=p2h.stdout)
    if rc > 0:
        exit(rc)


if environ.get("DO_RETRIEVE", "True") == "True":
    retrieve_jar()

if environ.get("DO_SUBMIT", "True") == "True":
    submit_job()

if environ.get("METASTORE_LOCATION") != None:
    update_metastore(environ.get("METASTORE_LOCATION"), environ.get("HIVE_SERVER"))
