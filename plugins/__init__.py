# -*- coding: utf-8 -*-

# Inbuilt Imports
from flask import Blueprint
from airflow.plugins_manager import AirflowPlugin
from airflow import configuration

# Backfill Plugin Imports
from backfill.main import Backfill

# Get RBAC config.
rbac_authentication_enabled = configuration.getboolean("webserver", "RBAC")

# Init the plugin in Webserver's "Admin" Menu with Menu Item as "Backfill"
if rbac_authentication_enabled == True:
    backfill_admin_view = {"category" : "Admin", "name" : "Backfill (Alpha)",  "view": Backfill()}
else:
    backfill_admin_view = Backfill(category="Admin", name="Backfill (Alpha)")

# Creating a flask blueprint to integrate the templates folder
backfill_blueprint = Blueprint(
    "backfill_blueprint", __name__,
    template_folder='templates')

# Defining the plugin class
class AirflowBackfillPlugin(AirflowPlugin):
    name = "backfill_plugin"
    admin_views = [backfill_admin_view]
    flask_blueprints = [backfill_blueprint]
    appbuilder_views = [backfill_admin_view]
