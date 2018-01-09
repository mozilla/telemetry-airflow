#!/usr/bin/env python

from os import chdir
from os import environ
from subprocess import call

def call_exit_errors(command):
    print("+ {}".format(" ".join(command)))
    rc = call(command)
    if rc > 0:
        exit(rc)

command = [
    "git",
    "clone",
    environ.get("GIT_PATH", "https://github.com/mozilla/telemetry-batch-view.git"),
    "--branch",
    environ.get("GIT_BRANCH", "master"),
]
call_exit_errors(command)

print("+ cd telemetry-batch-view")
chdir("telemetry-batch-view")

command = ["sbt", "assembly"]
call_exit_errors(command)

command = sum([
    ["--{}".format(key[4:].lower().replace("_", "-")), value]
    for key, value in environ.items()
    if key.startswith("TBV_") and key != "TBV_CLASS"
], [
    "spark-submit",
    "--master", "yarn",
    "--deploy-mode", "client",
    "--class", environ["TBV_CLASS"],
    "target/scala-2.11/telemetry-batch-view-1.1.jar",
])
call_exit_errors(command)
