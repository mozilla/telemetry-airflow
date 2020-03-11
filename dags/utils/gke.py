def create_gke_config(
    name,
    service_account,
    owner_label,
    team_label,
    machine_type="n1-standard-1",
    disk_size_gb=100,
    preemptible=True,
    disk_type="pd-standard",
    location="us-west1-b",
    subnetwork="default",
    is_dev=False,
):

    """
    Helper function to create gke cluster definition dict. All fields must match
    their protobuf definitions.

    See:
        https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1beta1/projects.locations.clusters#Cluster
        https://googleapis.dev/python/container/latest/gapic/v1/types.html#google.cloud.container_v1.types.Cluster
    owner and team labels can contain only lowercase letters, numeric characters,
    underscores, and dashes. E.g. owner_label='hwoo', team_label='dataops'

    """

    cluster_def_dict = {
        "name": name,
        "initial_node_count": None,
        # Setting `{"enabled": true}` will open the GKE cluster to the world.
        # This is config is disabled when run locally, otherwise job submissions
        # will fail.
        "master_authorized_networks_config": {"enabled": not is_dev},
        "node_pools": [
            {
                "name": name,
                "config": {
                    "machine_type": machine_type,
                    "disk_size_gb": disk_size_gb,
                    "oauth_scopes": [
                        "https://www.googleapis.com/auth/bigquery",
                        "https://www.googleapis.com/auth/devstorage.read_write",
                        "https://www.googleapis.com/auth/logging.write",
                        "https://www.googleapis.com/auth/monitoring",
                        "https://www.googleapis.com/auth/service.management.readonly",
                        "https://www.googleapis.com/auth/servicecontrol",
                        "https://www.googleapis.com/auth/trace.append",
                    ],
                    "service_account": service_account,
                    "labels": {"owner": owner_label, "team": team_label},
                    "preemptible": preemptible,
                    "diskType": disk_type,
                },
                "initial_node_count": 1,
                "autoscaling": {
                    "enabled": True,
                    "min_node_count": 1,
                    "max_node_count": 5,
                },
            }
        ],
        "locations": [location],
        "network": "default",
        "subnetwork": subnetwork,
    }

    return cluster_def_dict
