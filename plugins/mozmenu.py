"""
Plugin that adds a "Mozilla" entry to the top bar with some useful links.

Based on an example at
https://github.com/airflow-plugins/Getting-Started/blob/master/Tutorial/creating-ui-modification.md
"""


from airflow.plugins_manager import AirflowPlugin
from flask_admin.base import MenuLink


telemetry_airflow = MenuLink(
    category="Mozilla",
    name="telemetry-airflow on GitHub",
    url="https://github.com/mozilla/telemetry-airflow")

wtmo_dev = MenuLink(
    category="Mozilla",
    name="WTMO Developer Guide",
    url="https://mana.mozilla.org/wiki/display/DOPS/WTMO+Developer+Guide")

class MozMenuPlugin(AirflowPlugin):
    name = "Mozilla"
    operators = []
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = [telemetry_airflow, wtmo_dev]
