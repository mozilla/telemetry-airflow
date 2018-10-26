# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from statuspage.hook import DatasetStatusHook


class DatasetStatusOperator(BaseOperator):
    def __init__(self, name, description, status, statuspage_conn_id='statuspage_default'):
        super(DatasetStatusOperator, self).__init__(**kwargs)
        self.statuspage_conn_id = statuspage_conn_id

    def execute(self, context):
        conn = DatasetStatusHook(self.statuspage_conn_id).get_conn()
        
        raise NotImplemented
