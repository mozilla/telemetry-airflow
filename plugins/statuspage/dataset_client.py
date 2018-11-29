from statuspage.statuspage_client import StatuspageClient


class DatasetStatus:
    """"A Statuspage client for setting the status of Data Engineering Datasets in Firefox Operations."""

    def __init__(self, api_key):
        self.client = StatuspageClient(
            api_key, "Firefox Operations", "Data Engineering Datasets"
        )

    def _create(self, name, description="", status="operational"):
        return self.client.create_component(
            {
                "component": {
                    "name": name,
                    "description": description,
                    "status": status,
                    "only_show_if_degraded": False,
                    "group_id": self.client.group_id,
                    "showcase": True,
                }
            }
        )

    def get_or_create(self, name, description=""):
        """Get or create the component id of a statuspage. 
        
        :param name:        The name of the component
        :param description: The description associated with the component
        :returns:   A component id or None
        """
        cid = self.client.get_component_id(name)
        if not cid:
            cid = self._create(name, description)
        return cid

    def update(self, component_id, status):
        """Set the state of a component.

        :param component_id: The identifier of a component
        :param status: one of [operational, under_maintenance, degraded_performance, partial_outage, major_outage]
        :returns:   The component id if successful, None otherwise
        """
        patch = {"component": {"status": status}}
        return self.client.update_component(component_id, patch)
