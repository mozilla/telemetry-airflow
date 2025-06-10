from textwrap import dedent
from typing import ClassVar
from urllib.parse import quote, quote_plus, urlencode

from airflow.configuration import conf
from airflow.models.baseoperator import BaseOperator
from airflow.models.baseoperatorlink import BaseOperatorLink
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.operators.bash import BashOperator
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from fivetran_provider_async.operators import FivetranOperator

from operators.gcp_container_operator import GKEPodOperator


class BugzillaLink(BaseOperatorLink):
    name = "Create Bugzilla Issue"

    operators: ClassVar[list[type[BaseOperator]]] = [
        GKEPodOperator,
        BashOperator,
        PythonOperator,
        EmptyOperator,
        BaseBranchOperator,
        DataprocCreateClusterOperator,
        DataprocDeleteClusterOperator,
        DataprocSubmitJobOperator,
        FivetranOperator,
        SubDagOperator,
    ]

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        base_url = conf.get(section="webserver", key="base_url")
        comment = dedent(
            f"""\
            Airflow task `{operator.dag_id}.{operator.task_id}` failed for run `{ti_key.run_id}`.

            Task link:
            {base_url}/dags/{quote(operator.dag_id)}/grid?dag_run_id={quote_plus(ti_key.run_id)}&task_id={quote_plus(operator.task_id)}

            Log extract:
            ```
              <extract of error log>
            ```"""
        )

        bug_title = f"Airflow task `{operator.dag_id}.{operator.task_id}` failed for run `{ti_key.run_id}`"

        url_params = {
            "assigned_to": operator.owner,
            "bug_ignored": "0",
            "bug_severity": "--",
            "bug_status": "NEW",
            "bug_type": "defect",
            "cf_fx_iteration": "---",
            "cf_fx_points": "---",
            "comment": comment,
            "component": "General",
            "contenttypemethod": "list",
            "contenttypeselection": "text/plain",
            "defined_groups": "1",
            "filed_via": "standard_form",
            "flag_type-4": "X",
            "flag_type-607": "X",
            "flag_type-800": "X",
            "flag_type-803": "X",
            "flag_type-936": "X",
            "form_name": "enter_bug",
            "maketemplate": "Remember values as bookmarkable template",
            "op_sys": "Unspecified",
            "priority": "--",
            "product": "Data Platform and Tools",
            "rep_platform": "Unspecified",
            "short_desc": bug_title,
            "status_whiteboard": "[airflow-triage]",
            "target_milestone": "---",
            "version": "unspecified",
        }
        return "https://bugzilla.mozilla.org/enter_bug.cgi?" + urlencode(url_params)


# Defining the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links: ClassVar[list] = [
        BugzillaLink(),
    ]
