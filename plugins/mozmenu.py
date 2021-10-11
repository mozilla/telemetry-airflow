"""
Plugin that adds a "Mozilla" entry to the top bar with some useful links.

Based on an example at
https://github.com/airflow-plugins/Getting-Started/blob/master/Tutorial/creating-ui-modification.md
"""
from airflow.plugins_manager import AirflowPlugin


telemetry_airflow = {
    "name": "telemetry-airflow on GitHub",
    "category": "Mozilla",
    "href": "https://github.com/mozilla/telemetry-airflow"
}

wtmo_dev = {
    "name": "WTMO Developer Guide",
    "category": "Mozilla",
    "href": "https://mana.mozilla.org/wiki/display/DOPS/WTMO+Developer+Guide"
}

class MozMenuPlugin(AirflowPlugin):
    name = "Mozilla"
    operators = []
    flask_blueprints = []
    hooks = []
    executors = []
    appbuilder_views = []
    appbuilder_menu_items = [telemetry_airflow, wtmo_dev]
