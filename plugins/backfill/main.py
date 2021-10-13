# -*- coding: utf-8 -*-
# Modified from https://github.com/AnkurChoraywal/airflow-backfill-util.git

# Inbuilt Imports
import os
import json
import logging
import datetime
import re

# Custom Imports
import flask
from flask import request
from flask_admin import BaseView, expose
from flask_appbuilder import expose as app_builder_expose, BaseView as AppBuilderBaseView,has_access
from airflow import configuration

from shelljob import proc

# Inspired from
# https://mortoray.com/2014/03/04/http-streaming-of-command-output-in-python-flask/
# https://www.endpoint.com/blog/2015/01/28/getting-realtime-output-using-python
# RBAC inspired from 
# https://github.com/teamclairvoyant/airflow-rest-api-plugin


# Set your Airflow home path
if 'AIRFLOW_HOME' in os.environ:
    airflow_home_path = os.environ['AIRFLOW_HOME']
else:
    airflow_home_path = '/tmp'

# Local file where history will be stored
FILE = airflow_home_path + '/logs/backfill_history.txt'

# RE for remove ansi escape characters
ansi_escape = re.compile(r'\x1B[@-_][0-?]*[ -/]*[@-~]')

# Creating a flask admin BaseView
def file_ops(mode, data=None):
    """ File operators - logging/writing and reading user request """
    if mode == 'r':
        try:
            with open(FILE, 'r') as f:
                return f.read()
        except IOError:
            with open(FILE, 'w') as f:
                return f.close()

    elif mode == 'w' and data:
        today = datetime.datetime.now()
        print(os.getcwd())
        with open(FILE, 'a+') as f:
            file_data = '{},{}\n'.format(data, today)
            f.write(file_data)
            return 1

def get_baseview():
    return AppBuilderBaseView

class Backfill(get_baseview()):

    route_base = "/admin/backfill/"

    @app_builder_expose('/')
    def list(self):
        return self.render_template("backfill_page.html")

    @expose('/stream')
    @app_builder_expose('/stream')
    def stream(self):
        """ Runs user request and outputs console stream to client"""
        dag_name = request.args.get("dag_name")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        clear = request.args.get("clear")
        dry_run = request.args.get("dry_run")
        task_regex = request.args.get("task_regex")
        use_task_regex = request.args.get("use_task_regex")

        # Construct the airflow command
        cmd = ['airflow']

        if clear == 'true':
            cmd.append('clear')
            if dry_run == 'true':
                # For dryruns we simply timeout to avoid zombie procs waiting on user input. The output is what we're interested in
                timeout_list = ['timeout', '60']
                cmd = timeout_list + cmd
            elif dry_run == 'false':
                cmd.append('-c')

            if use_task_regex == 'true':
                cmd.extend(['-t', str(task_regex)])
        elif clear == 'false':
            cmd.append('dags')
            cmd.append('backfill')
            if dry_run == 'true':
                cmd.append('--dry-run')

            if use_task_regex == 'true':
                cmd.extend(['-t', str(task_regex)])

        cmd.extend(['-s', str(start_date), '-e', str(end_date), str(dag_name)])

        print('BACKFILL CMD:', cmd)

        # updates command used in history
        file_ops('w', ' '.join(cmd))

        g = proc.Group()
        g.run(cmd)

        # To print out cleared dry run task instances
        pattern = '^\<.*\>$'

        def read_process():
            while g.is_pending():
                lines = g.readlines()
                for proc, line in lines:
                    line = line.decode("utf-8")
                    result = re.match(pattern, line)

                    if result:
                        # Adhere to text/event-stream format
                        line = line.replace('<','').replace('>','')
                    elif clear == 'true' and dry_run == 'false':
                        # Special case/hack, airflow clear -c no longer outputs a termination string, so we put one
                        line = "Clear Done"

                    yield "data:" + line + "\n\n"


        return flask.Response(read_process(), mimetype='text/event-stream')

    @expose('/background')
    @app_builder_expose('/background')
    def background(self):
        """ Runs user request in background """
        dag_name = request.args.get("dag_name")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        clear = request.args.get("clear")
        task_regex = request.args.get("task_regex")
        use_task_regex = request.args.get("use_task_regex")

        # create a screen id based on timestamp
        screen_id = datetime.datetime.now().strftime('%s')

        # Prepare the command and execute in background
        background_cmd = "screen -dmS {} ".format(screen_id)
        if clear == 'true':
            background_cmd = background_cmd + 'airflow tasks clear -c '
        elif clear == 'false':
            background_cmd = background_cmd + 'airflow dags backfill '

        if use_task_regex == 'true':
            background_cmd = background_cmd + "-t {} ".format(task_regex)

        background_cmd = background_cmd + "-s {} -e {} {}".format(start_date, end_date, dag_name)

        # Update command in file
        file_ops('w', background_cmd)

        print(background_cmd)

        os.system(background_cmd)

        response = json.dumps({'submitted': True})
        return flask.Response(response, mimetype='text/json')

    @expose('/history')
    @app_builder_expose('/history')
    def history(self):
        """ Outputs recent user request history """
        return flask.Response(file_ops('r'), mimetype='text/txt')
