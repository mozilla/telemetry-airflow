# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Statuspage integration

import requests
import jsonschema
import logging

from statuspage import schema


class StatuspageClient:
    def __init__(self, api_key, page_name, group_name):
        """A Statuspage client for a page and group"""

        self.base_url = "https://api.statuspage.io/v1/"
        self.api_key = api_key

        self.page_id = self.get_page_id(page_name)
        self.group_id = self.get_component_group_id(group_name)

    def _request(self, method, path, data=None):
        headers = {"Authorization": "OAuth {}".format(self.api_key)}
        request_method = {
            "get": requests.get,
            "post": requests.post,
            "delete": requests.delete,
            "put": requests.put,
            "patch": requests.patch,
        }.get(method)

        if not request_method:
            raise ValueError("Method {} not supported".format(method))

        url = self.base_url + path
        resp = request_method(url, json=data, headers=headers)
        logging.info("status-code {} for {}".format(resp.status_code, url))
        if resp.status_code != 200:
            logging.error(resp.content)
        resp.raise_for_status()
        return resp

    def get_id(self, data, predicate):
        for row in data:
            if predicate(row):
                return row["id"]
        return None

    def get_page_id(self, name):
        resp = self._request("get", "pages")
        data = resp.json()
        return self.get_id(data, lambda r: r["name"] == name)

    def get_component_group_id(self, name):
        resp = self._request("get", "pages/{}/component-groups".format(self.page_id))
        data = resp.json()
        return self.get_id(data, lambda r: r["name"] == name)

    def get_component_id(self, name):
        resp = self._request("get", "pages/{}/components".format(self.page_id))
        data = resp.json()
        return self.get_id(data, lambda r: r["name"] == name)

    def create_component(self, component):
        jsonschema.validate(component, schema.component)
        resp = self._request(
            "post", "pages/{}/components".format(self.page_id), component
        )
        return resp.json().get("id")

    def update_component(self, component_id, component):
        jsonschema.validate(component, schema.component)
        route = "pages/{}/components/{}".format(self.page_id, component_id)
        resp = self._request("patch", route, component)
        return resp.json().get("id")
