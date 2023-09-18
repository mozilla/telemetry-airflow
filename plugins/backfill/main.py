# Modified from https://github.com/AnkurChoraywal/airflow-backfill-util.git

# Inbuilt Imports
import datetime
import json
import os
import re

# Custom Imports
import flask
from flask import request
from flask_admin import expose
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose as app_builder_expose
from shelljob import proc

from utils.backfill import BackfillParams

# Inspired from
# https://mortoray.com/2014/03/04/http-streaming-of-command-output-in-python-flask/
# https://www.endpoint.com/blog/2015/01/28/getting-realtime-output-using-python
# RBAC inspired from
# https://github.com/teamclairvoyant/airflow-rest-api-plugin


# Set your Airflow home path
airflow_home_path = os.environ.get("AIRFLOW_HOME", "/tmp")

# Local file where history will be stored
FILE = airflow_home_path + "/logs/backfill_history.txt"

# RE for remove ansi escape characters
ansi_escape = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")


# Creating a flask admin BaseView
def file_ops(mode, data=None):
    """File operators - logging/writing and reading user request."""
    if mode == "r":
        try:
            with open(FILE) as f:
                return f.read()
        except OSError:
            with open(FILE, "w") as f:
                return f.close()

    elif mode == "w" and data:
        today = datetime.datetime.now()
        print(os.getcwd())
        with open(FILE, "a+") as f:
            file_data = f"{data},{today}\n"
            f.write(file_data)
            return 1


def get_baseview():
    return AppBuilderBaseView


class Backfill(get_baseview()):
    route_base = "/admin/backfill/"

    @app_builder_expose("/")
    def list(self):
        return self.render_template("backfill_page.html")

    @expose("/stream")
    @app_builder_expose("/stream")
    def stream(self):
        """Run user request and outputs console stream to client."""
        dag_name = request.args.get("dag_name")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        clear = request.args.get("clear").lower() == "true"
        dry_run = request.args.get("dry_run").lower() == "true"
        use_task_regex = request.args.get("use_task_regex").lower() == "true"
        task_regex = request.args.get("task_regex") if use_task_regex else None

        # Construct the airflow command
        backfill_params = BackfillParams(
            clear=clear,
            dry_run=dry_run,
            task_regex=task_regex,
            dag_name=dag_name,
            start_date=start_date,
            end_date=end_date,
        )

        cmd = backfill_params.generate_backfill_command()

        print("BACKFILL CMD:", cmd)

        # updates command used in history
        file_ops("w", " ".join(cmd))

        g = proc.Group()
        g.run(cmd)

        # To print out cleared dry run task instances
        pattern = r"^\<.*\>$"

        def read_process():
            while g.is_pending():
                lines = g.readlines()
                for _proc, line in lines:
                    line = line.decode("utf-8")
                    result = re.match(pattern, line)

                    if result:
                        # Adhere to text/event-stream format
                        line = line.replace("<", "").replace(">", "")
                    elif clear and not dry_run:
                        # Special case/hack, airflow tasks clear -y no longer outputs a termination string, so we put one
                        line = "Clear Done"

                    yield "data:" + line + "\n\n"

        return flask.Response(read_process(), mimetype="text/event-stream")

    @expose("/background")
    @app_builder_expose("/background")
    def background(self):
        """Run user request in background."""
        dag_name = request.args.get("dag_name")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        clear = request.args.get("clear")
        task_regex = request.args.get("task_regex")
        use_task_regex = request.args.get("use_task_regex")

        # create a screen id based on timestamp
        screen_id = datetime.datetime.now().strftime("%s")

        # Prepare the command and execute in background
        background_cmd = f"screen -dmS {screen_id} "
        if clear == "true":
            background_cmd = background_cmd + "airflow tasks clear -y "
        elif clear == "false":
            background_cmd = background_cmd + "airflow dags backfill "

        if use_task_regex == "true":
            background_cmd = background_cmd + f"-t {task_regex} "

        background_cmd = background_cmd + f"-s {start_date} -e {end_date} {dag_name}"

        # Update command in file
        file_ops("w", background_cmd)

        print(background_cmd)

        os.system(background_cmd)

        response = json.dumps({"submitted": True})
        return flask.Response(response, mimetype="text/json")

    @expose("/history")
    @app_builder_expose("/history")
    def history(self):
        """Output recent user request history."""
        return flask.Response(file_ops("r"), mimetype="text/txt")
