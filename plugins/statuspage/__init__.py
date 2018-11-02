# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from airflow.plugins_manager import AirflowPlugin

from statuspage import hook, operator


class DatasetStatusPlugin(AirflowPlugin):
    name = "dataset_status"
    hooks = [hook.DatasetStatusHook]
    operators = [operator.DatasetStatusOperator]
