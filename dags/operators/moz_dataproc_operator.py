import os
import re
import time
import uuid
from datetime import timedelta

from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.contrib.operators.dataproc_operator import DataprocOperationBaseOperator
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils import timezone
from airflow.version import version

"""
We overwrite DataprocClusterCreateOperator here to create clusters with an option to
install component gateway, which we install by default. We also add labels to the gce
cluster config.

Previously on 1.10.2, we had to include DataprocOperationBaseOperator from master
which used the v1beta2 rest api for creating clusters allowing us to install optional
components and component gateway, but this class has been updated since 1.10.4.

"""

# pylint: disable=too-many-instance-attributes
class DataprocClusterCreateOperator(DataprocOperationBaseOperator):
    """
    --
    Pulled from 1.10.7

    We modify the _build_gce_cluster_config method to install component gateway.
    --
    Create a new cluster on Google Cloud Dataproc. The operator will wait until the
    creation is successful or an error occurs in the creation process.

    The parameters allow to configure the cluster. Please refer to

    https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters

    for a detailed explanation on the different parameters. Most of the configuration
    parameters detailed in the link are available as a parameter to this operator.

    :param cluster_name: The name of the DataProc cluster to create. (templated)
    :type cluster_name: str
    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :type project_id: str
    :param num_workers: The # of workers to spin up. If set to zero will
        spin up cluster in a single node mode
    :type num_workers: int
    :param storage_bucket: The storage bucket to use, setting to None lets dataproc
        generate a custom one for you
    :type storage_bucket: str
    :param init_actions_uris: List of GCS uri's containing
        dataproc initialization scripts
    :type init_actions_uris: list[str]
    :param init_action_timeout: Amount of time executable scripts in
        init_actions_uris has to complete
    :type init_action_timeout: str
    :param metadata: dict of key-value google compute engine metadata entries
        to add to all instances
    :type metadata: dict
    :param image_version: the version of software inside the Dataproc cluster
    :type image_version: str
    :param custom_image: custom Dataproc image for more info see
        https://cloud.google.com/dataproc/docs/guides/dataproc-images
    :type custom_image: str
    :param custom_image_project_id: project id for the custom Dataproc image, for more info see
        https://cloud.google.com/dataproc/docs/guides/dataproc-images
    :type custom_image_project_id: str
    :param autoscaling_policy: The autoscaling policy used by the cluster. Only resource names
        including projectid and location (region) are valid. Example:
        ``projects/[projectId]/locations/[dataproc_region]/autoscalingPolicies/[policy_id]``
    :type autoscaling_policy: str
    :param properties: dict of properties to set on
        config files (e.g. spark-defaults.conf), see
        https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#SoftwareConfig
    :type properties: dict
    :param optional_components: List of optional cluster components, for more info see
        https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig#Component
    :type optional_components: list[str]
    :param num_masters: The # of master nodes to spin up
    :type num_masters: int
    :param master_machine_type: Compute engine machine type to use for the master node
    :type master_machine_type: str
    :param master_disk_type: Type of the boot disk for the master node
        (default is ``pd-standard``).
        Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
        ``pd-standard`` (Persistent Disk Hard Disk Drive).
    :type master_disk_type: str
    :param master_disk_size: Disk size for the master node
    :type master_disk_size: int
    :param master_num_local_ssds : Number of local SSDs to mount
        (default is 0)
    :type master_num_local_ssds : int
    :param worker_machine_type: Compute engine machine type to use for the worker nodes
    :type worker_machine_type: str
    :param worker_disk_type: Type of the boot disk for the worker node
        (default is ``pd-standard``).
        Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
        ``pd-standard`` (Persistent Disk Hard Disk Drive).
    :type worker_disk_type: str
    :param worker_disk_size: Disk size for the worker nodes
    :type worker_disk_size: int
    :param worker_num_local_ssds : Number of local SSDs to mount
        (default is 0)
    :type worker_num_local_ssds : int
    :param num_preemptible_workers: The # of preemptible worker nodes to spin up
    :type num_preemptible_workers: int
    :param labels: dict of labels to add to the cluster
    :type labels: dict
    :param zone: The zone where the cluster will be located. Set to None to auto-zone. (templated)
    :type zone: str
    :param network_uri: The network uri to be used for machine communication, cannot be
        specified with subnetwork_uri
    :type network_uri: str
    :param subnetwork_uri: The subnetwork uri to be used for machine communication,
        cannot be specified with network_uri
    :type subnetwork_uri: str
    :param internal_ip_only: If true, all instances in the cluster will only
        have internal IP addresses. This can only be enabled for subnetwork
        enabled networks
    :type internal_ip_only: bool
    :param tags: The GCE tags to add to all instances
    :type tags: list[str]
    :param region: leave as 'global', might become relevant in the future. (templated)
    :type region: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param service_account: The service account of the dataproc instances.
    :type service_account: str
    :param service_account_scopes: The URIs of service account scopes to be included.
    :type service_account_scopes: list[str]
    :param idle_delete_ttl: The longest duration that cluster would keep alive while
        staying idle. Passing this threshold will cause cluster to be auto-deleted.
        A duration in seconds.
    :type idle_delete_ttl: int
    :param auto_delete_time:  The time when cluster will be auto-deleted.
    :type auto_delete_time: datetime.datetime
    :param auto_delete_ttl: The life duration of cluster, the cluster will be
        auto-deleted at the end of this duration.
        A duration in seconds. (If auto_delete_time is set this parameter will be ignored)
    :type auto_delete_ttl: int
    :param customer_managed_key: The customer-managed key used for disk encryption
        ``projects/[PROJECT_STORING_KEYS]/locations/[LOCATION]/keyRings/[KEY_RING_NAME]/cryptoKeys/[KEY_NAME]`` # noqa # pylint: disable=line-too-long
    :type customer_managed_key: str

    Moz specific
    :param install_component_gateway: Install alpha feature component gateway.
    :type install_component_gateway: boolean

    """

    template_fields = ['cluster_name', 'project_id', 'zone', 'region']

    # pylint: disable=too-many-arguments,too-many-locals
    @apply_defaults
    def __init__(self,
                 project_id,
                 cluster_name,
                 num_workers,
                 zone=None,
                 network_uri=None,
                 subnetwork_uri=None,
                 internal_ip_only=None,
                 tags=None,
                 storage_bucket=None,
                 init_actions_uris=None,
                 init_action_timeout="10m",
                 metadata=None,
                 custom_image=None,
                 custom_image_project_id=None,
                 image_version=None,
                 autoscaling_policy=None,
                 properties=None,
                 optional_components=['ANACONDA'], # Moz specific
                 num_masters=1,
                 master_machine_type='n1-standard-4',
                 master_disk_type='pd-standard',
                 master_disk_size=500,
                 master_num_local_ssds=0,
                 worker_machine_type='n1-standard-4',
                 worker_disk_type='pd-standard',
                 worker_disk_size=500,
                 worker_num_local_ssds=0,
                 num_preemptible_workers=0,
                 labels=None,
                 region='global',
                 service_account=None,
                 service_account_scopes=None,
                 idle_delete_ttl=None,
                 auto_delete_time=None,
                 auto_delete_ttl=None,
                 customer_managed_key=None,
                 install_component_gateway=True, # Moz specific
                 *args,
                 **kwargs):

        super(DataprocClusterCreateOperator, self).__init__(
            project_id=project_id, region=region, *args, **kwargs)
        self.cluster_name = cluster_name
        self.num_masters = num_masters
        self.num_workers = num_workers
        self.num_preemptible_workers = num_preemptible_workers
        self.storage_bucket = storage_bucket
        self.init_actions_uris = init_actions_uris
        self.init_action_timeout = init_action_timeout
        self.metadata = metadata
        self.custom_image = custom_image
        self.custom_image_project_id = custom_image_project_id
        self.image_version = image_version
        self.properties = properties or dict()
        self.optional_components = optional_components
        self.master_machine_type = master_machine_type
        self.master_disk_type = master_disk_type
        self.master_disk_size = master_disk_size
        self.master_num_local_ssds = master_num_local_ssds
        self.autoscaling_policy = autoscaling_policy
        self.worker_machine_type = worker_machine_type
        self.worker_disk_type = worker_disk_type
        self.worker_disk_size = worker_disk_size
        self.worker_num_local_ssds = worker_num_local_ssds
        self.labels = labels
        self.zone = zone
        self.network_uri = network_uri
        self.subnetwork_uri = subnetwork_uri
        self.internal_ip_only = internal_ip_only
        self.tags = tags
        self.service_account = service_account
        self.service_account_scopes = service_account_scopes
        self.idle_delete_ttl = idle_delete_ttl
        self.auto_delete_time = auto_delete_time
        self.auto_delete_ttl = auto_delete_ttl
        self.customer_managed_key = customer_managed_key
        self.single_node = num_workers == 0
        self.install_component_gateway = install_component_gateway # Moz specific

        assert not (self.custom_image and self.image_version), \
            "custom_image and image_version can't be both set"

        assert (
            not self.single_node or (
                self.single_node and self.num_preemptible_workers == 0
            )
        ), "num_workers == 0 means single node mode - no preemptibles allowed"

    def _get_init_action_timeout(self):
        match = re.match(r"^(\d+)(s|m)$", self.init_action_timeout)
        if match:
            if match.group(2) == "s":
                return self.init_action_timeout
            elif match.group(2) == "m":
                val = float(match.group(1))
                return "{}s".format(timedelta(minutes=val).seconds)

        raise AirflowException(
            "DataprocClusterCreateOperator init_action_timeout"
            " should be expressed in minutes or seconds. i.e. 10m, 30s")

    def _build_gce_cluster_config(self, cluster_data):
        """
        We optionally add alpha feature 'enable component gateway'

        """

        if self.install_component_gateway: # Moz specific start
            # Fetch current nested dict and add nested keys
            cluster_config_new = cluster_data['config']
            cluster_config_new.update({'endpointConfig' : {'enableHttpPortAccess' : True}})

            # Overwrite the config key with newly created
            cluster_data.update({'config' : cluster_config_new}) # Moz specific end


        if self.zone:
            zone_uri = \
                'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
                    self.project_id, self.zone
                )
            cluster_data['config']['gceClusterConfig']['zoneUri'] = zone_uri

        if self.metadata:
            cluster_data['config']['gceClusterConfig']['metadata'] = self.metadata

        if self.network_uri:
            cluster_data['config']['gceClusterConfig']['networkUri'] = self.network_uri

        if self.subnetwork_uri:
            cluster_data['config']['gceClusterConfig']['subnetworkUri'] = \
                self.subnetwork_uri

        if self.internal_ip_only:
            if not self.subnetwork_uri:
                raise AirflowException("Set internal_ip_only to true only when"
                                       " you pass a subnetwork_uri.")
            cluster_data['config']['gceClusterConfig']['internalIpOnly'] = True

        if self.tags:
            cluster_data['config']['gceClusterConfig']['tags'] = self.tags

        if self.service_account:
            cluster_data['config']['gceClusterConfig']['serviceAccount'] = \
                self.service_account

        if self.service_account_scopes:
            cluster_data['config']['gceClusterConfig']['serviceAccountScopes'] = \
                self.service_account_scopes

        return cluster_data

    def _build_lifecycle_config(self, cluster_data):
        if self.idle_delete_ttl:
            cluster_data['config']['lifecycleConfig']['idleDeleteTtl'] = \
                "{}s".format(self.idle_delete_ttl)

        if self.auto_delete_time:
            utc_auto_delete_time = timezone.convert_to_utc(self.auto_delete_time)
            cluster_data['config']['lifecycleConfig']['autoDeleteTime'] = \
                utc_auto_delete_time.format('%Y-%m-%dT%H:%M:%S.%fZ', formatter='classic')
        elif self.auto_delete_ttl:
            cluster_data['config']['lifecycleConfig']['autoDeleteTtl'] = \
                "{}s".format(self.auto_delete_ttl)

        return cluster_data

    def _build_cluster_data(self):
        if self.zone:
            master_type_uri = \
                "https://www.googleapis.com/compute/v1/projects/{}/zones/{}/machineTypes/{}"\
                .format(self.project_id, self.zone, self.master_machine_type)
            worker_type_uri = \
                "https://www.googleapis.com/compute/v1/projects/{}/zones/{}/machineTypes/{}"\
                .format(self.project_id, self.zone, self.worker_machine_type)
        else:
            master_type_uri = self.master_machine_type
            worker_type_uri = self.worker_machine_type

        cluster_data = {
            'projectId': self.project_id,
            'clusterName': self.cluster_name,
            'labels': { # Moz specific start
                'owner': self.owner,
                'env': os.getenv('DEPLOY_ENVIRONMENT', 'env_not_set')
            }, # Moz specific end
            'config': {
                'gceClusterConfig': {
                },
                'masterConfig': {
                    'numInstances': self.num_masters,
                    'machineTypeUri': master_type_uri,
                    'diskConfig': {
                        'bootDiskType': self.master_disk_type,
                        'bootDiskSizeGb': self.master_disk_size,
                        'numLocalSsds': self.master_num_local_ssds,
                    }
                },
                'workerConfig': {
                    'numInstances': self.num_workers,
                    'machineTypeUri': worker_type_uri,
                    'diskConfig': {
                        'bootDiskType': self.worker_disk_type,
                        'bootDiskSizeGb': self.worker_disk_size,
                        'numLocalSsds': self.worker_num_local_ssds,
                    }
                },
                'secondaryWorkerConfig': {},
                'softwareConfig': {},
                'lifecycleConfig': {},
                'encryptionConfig': {},
                'autoscalingConfig': {},
            }
        }
        if self.num_preemptible_workers > 0:
            cluster_data['config']['secondaryWorkerConfig'] = {
                'numInstances': self.num_preemptible_workers,
                'machineTypeUri': worker_type_uri,
                'diskConfig': {
                    'bootDiskType': self.worker_disk_type,
                    'bootDiskSizeGb': self.worker_disk_size
                },
                'isPreemptible': True
            }

        cluster_data['labels'] = self.labels or {}

        # Dataproc labels must conform to the following regex:
        # [a-z]([-a-z0-9]*[a-z0-9])? (current airflow version string follows
        # semantic versioning spec: x.y.z).
        cluster_data['labels'].update({'airflow-version':
                                       'v' + version.replace('.', '-').replace('+', '-')})
        if self.storage_bucket:
            cluster_data['config']['configBucket'] = self.storage_bucket

        if self.image_version:
            cluster_data['config']['softwareConfig']['imageVersion'] = self.image_version

        elif self.custom_image:
            project_id = self.custom_image_project_id if (self.custom_image_project_id) else self.project_id
            custom_image_url = 'https://www.googleapis.com/compute/beta/projects/' \
                               '{}/global/images/{}'.format(project_id,
                                                            self.custom_image)
            cluster_data['config']['masterConfig']['imageUri'] = custom_image_url
            if not self.single_node:
                cluster_data['config']['workerConfig']['imageUri'] = custom_image_url

        cluster_data = self._build_gce_cluster_config(cluster_data)

        if self.single_node:
            self.properties["dataproc:dataproc.allow.zero.workers"] = "true"

        if self.properties:
            cluster_data['config']['softwareConfig']['properties'] = self.properties

        if self.optional_components:
            cluster_data['config']['softwareConfig']['optionalComponents'] = self.optional_components

        cluster_data = self._build_lifecycle_config(cluster_data)

        if self.init_actions_uris:
            init_actions_dict = [
                {
                    'executableFile': uri,
                    'executionTimeout': self._get_init_action_timeout()
                } for uri in self.init_actions_uris
            ]
            cluster_data['config']['initializationActions'] = init_actions_dict

        if self.customer_managed_key:
            cluster_data['config']['encryptionConfig'] =\
                {'gcePdKmsKeyName': self.customer_managed_key}
        if self.autoscaling_policy:
            cluster_data['config']['autoscalingConfig'] = {'policyUri': self.autoscaling_policy}

        return cluster_data

    def start(self):
        """
        Create a new cluster on Google Cloud Dataproc.
        """
        self.log.info('Creating cluster: %s', self.cluster_name)
        cluster_data = self._build_cluster_data()

        return (
            self.hook.get_conn().projects().regions().clusters().create(  # pylint: disable=no-member
                projectId=self.project_id,
                region=self.region,
                body=cluster_data,
                requestId=str(uuid.uuid4()),
            ).execute())

