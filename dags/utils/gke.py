def create_gke_config(
    name,
    service_account,
    owner_label,
    team_label,
    machine_type="n1-standard-1",
    disk_size_gb=100,
    preemptible=True,
    disk_type="pd-standard",
    location="us-west1-b"):

    """
    Helper function to create gke cluster definition dict.
    See https://google-cloud-python.readthedocs.io/en/latest/container/gapic/v1/types.html#google.cloud.container_v1.types.Cluster

    owner and team labels  can contain only lowercase letters, numeric characters,
    underscores, and dashes. E.g. owner_label='hwoo', team_label='dataops'

    """
    
    cluster_def_dict = {
        "name": name,
        "nodePools": [
            {
                "name": name,
                "config": {
                    "machineType": machine_type,
                    "diskSizeGb": disk_size_gb,
                    "oauthScopes": [
                        "https://www.googleapis.com/auth/devstorage.read_only",
                        "https://www.googleapis.com/auth/logging.write",
                        "https://www.googleapis.com/auth/monitoring",
                        "https://www.googleapis.com/auth/service.management.readonly",
                        "https://www.googleapis.com/auth/servicecontrol",
                        "https://www.googleapis.com/auth/trace.append",
                    ],
                    "serviceAccount": service_account,
                    "labels": {
                        "owner": owner_label,
                        "team": team_label
                    },
                    "preemptible": preemptible,
                    "diskType": disk_type
                },
                "initialNodeCount": 1,
                "autoscaling": {
                    "enabled" : True,
                    "minNodeCount": 1,
                    "maxNodeCount": 5
                }
                }
            ],
        "locations": [
            location
        ],
        "network": "default",
        "subnetwork": "gke-subnet"
    }

    return cluster_def_dict
