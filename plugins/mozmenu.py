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

airflow_triage_guide = {
    "name": "Airflow Triage Guide",
    "category": "Mozilla",
    "href": "https://mana.mozilla.org/wiki/display/DATA/Airflow+Triage+Process",
}

airflow_bugzilla_bug_template = {
    "name": "Airflow Bugzilla Bug Template",
    "category": "Mozilla",
    "href": "https://bugzilla.mozilla.org/enter_bug.cgi?assigned_to=nobody%40mozilla.org&bug_ignored=0&bug_severity=--&bug_status=NEW&bug_type=defect&cf_fx_iteration=---&cf_fx_points=---&comment=The%20Airflow%20task%20%3CAirflow%20DAG%3E.%3Ctask%20name%3E%20failed%20on%20%3Cdate%3E.&component=Datasets%3A%20General&contenttypemethod=list&contenttypeselection=text%2Fplain&defined_groups=1&filed_via=standard_form&flag_type-4=X&flag_type-607=X&flag_type-800=X&flag_type-803=X&flag_type-936=X&form_name=enter_bug&maketemplate=Remember%20values%20as%20bookmarkable%20template&op_sys=Unspecified&priority=--&product=Data%20Platform%20and%20Tools&rep_platform=Unspecified&short_desc=Airflow%20task%20%3CAirflow%20DAG%3E.%3Ctask%20name%3E%20failing%20on%20%3Cdate%3E&status_whiteboard=%5Bairflow-triage%5D&target_milestone=---&version=unspecified",
}

airflow_previous_bugs = {
    "name": "Airflow Previous Bugs (Bugzilla)",
    "category": "Mozilla",
    "href": "https://bugzilla.mozilla.org/buglist.cgi?query_format=advanced&bug_status=UNCONFIRMED&bug_status=NEW&bug_status=ASSIGNED&bug_status=REOPENED&bug_status=RESOLVED&bug_status=VERIFIED&bug_status=CLOSED&status_whiteboard=%5Bairflow-triage%5D%20&classification=Client%20Software&classification=Developer%20Infrastructure&classification=Components&classification=Server%20Software&classification=Other&resolution=---&resolution=FIXED&resolution=INVALID&resolution=WONTFIX&resolution=INACTIVE&resolution=DUPLICATE&resolution=WORKSFORME&resolution=INCOMPLETE&resolution=SUPPORT&resolution=EXPIRED&resolution=MOVED&status_whiteboard_type=allwordssubstr&list_id=15983703",
}

class MozMenuPlugin(AirflowPlugin):
    name = "Mozilla"
    operators = []
    flask_blueprints = []
    hooks = []
    executors = []
    appbuilder_views = []
    appbuilder_menu_items = [telemetry_airflow, wtmo_dev, airflow_triage_guide, airflow_bugzilla_bug_template, airflow_previous_bugs]
