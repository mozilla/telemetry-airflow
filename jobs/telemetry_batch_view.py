#!/usr/bin/env python

from os import chdir
from os import environ
from subprocess import call


def call_exit_errors(command):
    print("+ {}".format(" ".join(command)))
    rc = call(command)
    if rc > 0:
        exit(rc)


def clone_and_compile_tbv():
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
        "target/scala-2.11/telemetry-batch-view-1.1.jar",
    ])
    call_exit_errors(command)


if environ.get("DO_ASSEMBLY", "True") == "True":
    clone_and_compile_tbv()
else:
    print("+ cd telemetry-batch-view")
    chdir("telemetry-batch-view")

if environ.get("DO_SUBMIT", "True") == "True":
    submit_job()
