# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

component = {
    "type": "object",
    "properties": {
        "component": {
            "type": "object",
            "properties": {
                "description": {
                    "type": "string",
                    "description": "More detailed description for component",
                },
                "status": {
                    "type": "string",
                    "enum": [
                        "operational",
                        "under_maintenance",
                        "degraded_performance",
                        "partial_outage",
                        "major_outage",
                        "",
                    ],
                    "description": "Status of component",
                },
                "name": {"type": "string", "description": "Display name for component"},
                "only_show_if_degraded": {
                    "type": "boolean",
                    "description": "Requires a special feature flag to be enabled",
                },
                "group_id": {
                    "type": "string",
                    "description": "Component Group identifier",
                },
                "showcase": {
                    "type": "boolean",
                    "description": "Should this component be showcased",
                },
            },
            "additionalProperties": False,
        }
    },
    "additionalProperties": False,
    "required": ["component"],
}
