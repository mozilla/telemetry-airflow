# Airflow Configuration Diff
Output of git diff between reference config and airflow.cfg. This is a temporary file 
to help in removing our custom Airflow config. The goal is to use the default Airflow 
config and override parameters through environment variables set in:

* docker-compose.yml for **local deployment** 
* helm chart configmap for **prod and nonprod deployments**)

**The goal is to limit where we set Airflow parameters**.

# Scripts and commands
This script cleans config files and sorts them by sections and keys to have a more 
comprehensible `git diff` output. Reference Airflow config was taken from 
[here](https://github.com/apache/airflow/blob/v2-3-stable/airflow/config_templates/default_airflow.cfg)

### Clean and sort configs
```python
from configparser import ConfigParser
from pathlib import Path

def parse_config(path: Path) -> ConfigParser:
    if not path.exists():
        raise FileExistsError
    config = ConfigParser()
    config.read(path)
    return config

def sort_config(config: ConfigParser) -> ConfigParser:
    sorted_config = ConfigParser()
    sections = sorted(config._sections)
    for section in sections:
        sorted_config.add_section(section)
        items = sorted(config._sections[section].items())
        for item in items:
            sorted_config.set(section, item[0], item[1])

    return sorted_config

if __name__ == '__main__':
    ref_cfg = parse_config(Path("default_airflow.cfg"))
    comparable = parse_config(Path("airflow.cfg"))

    sorted_ref_cfg = sort_config(config=ref_cfg)
    sorted_comparable = sort_config(config=comparable)

    with open("clean_ref.cfg", "w") as file:
        sorted_ref_cfg.write(file)
    with open("clean_airflow.cfg", "w") as file:
        sorted_comparable.write(file)
```

### Get diff
```shell
git diff --no-index clean_ref.cfg clean_airflow.cfg > diff.txt
```

# Git diff output

```shell
diff --git a/clean_ref.cfg b/clean_airflow.cfg
index 1383fa6..ec837af 100644
--- a/clean_ref.cfg
+++ b/clean_airflow.cfg
@@ -1,60 +1,34 @@
 [api]
-access_control_allow_headers = 
-access_control_allow_methods = 
-access_control_allow_origins = 
 auth_backends = airflow.api.auth.backend.session
 enable_experimental_api = False
 fallback_page_limit = 100
-google_key_path = 
-google_oauth2_audience = 
 maximum_page_limit = 100
 
-[atlas]
-host = 
-password = 
-port = 21000
-sasl_enabled = False
-username = 
-
 [celery]
-broker_url = redis://redis:6379/0
+broker_url = $AIRFLOW_BROKER_URL
 celery_app_name = airflow.executors.celery_executor
 celery_config_options = airflow.config_templates.default_celery.DEFAULT_CELERY_CONFIG
-flower_basic_auth = 
 flower_host = 0.0.0.0
-flower_port = 5555
-flower_url_prefix = 
-operation_timeout = 1.0
+flower_port = $AIRFLOW_FLOWER_PORT
+operation_timeout = 3.0
 pool = prefork
-result_backend = db+postgresql://postgres:airflow@postgres/airflow
+result_backend = $AIRFLOW_RESULT_URL
 ssl_active = False
-ssl_cacert = 
-ssl_cert = 
-ssl_key = 
-stalled_task_timeout = 0
 sync_parallelism = 0
 task_adoption_timeout = 600
 task_publish_max_retries = 3
 task_track_started = True
-worker_concurrency = 16
-worker_enable_remote_control = true
+worker_concurrency = 32
 worker_precheck = False
-worker_prefetch_multiplier = 1
 
 [celery_broker_transport_options]
 
 [celery_kubernetes_executor]
-kubernetes_queue = kubernetes
-
-[cli]
-api_client = airflow.api.client.local_client
-endpoint_url = http://localhost:8080
 
 [core]
 check_slas = True
 compress_serialized_dags = False
-daemon_umask = 0o077
-dag_discovery_safe_mode = True
+dag_discovery_safe_mode = False
 dag_file_processor_timeout = 50
 dag_ignore_file_syntax = regexp
 dag_run_conf_overrides_params = True
@@ -62,188 +36,99 @@ dagbag_import_error_traceback_depth = 2
 dagbag_import_error_tracebacks = True
 dagbag_import_timeout = 30.0
 dags_are_paused_at_creation = True
-dags_folder = {AIRFLOW_HOME}/dags
-default_impersonation = 
-default_pool_task_slot_count = 128
+dags_folder = $AIRFLOW_HOME/dags
+default_pool_task_slot_count = 50
 default_task_execution_timeout = 
 default_task_retries = 0
 default_task_weight_rule = downstream
 default_timezone = utc
-donot_pickle = True
+donot_pickle = False
 enable_xcom_pickling = False
 execute_tasks_new_python_interpreter = False
-executor = SequentialExecutor
-fernet_key = {FERNET_KEY}
+executor = CeleryExecutor
 hide_sensitive_var_conn_fields = True
-hostname_callable = socket.getfqdn
 killed_task_cleanup_time = 60
 lazy_discover_providers = True
 lazy_load_plugins = True
-load_examples = True
-max_active_runs_per_dag = 16
+load_examples = False
+max_active_runs_per_dag = 5
 max_active_tasks_per_dag = 16
 max_map_length = 1024
 max_num_rendered_ti_fields_per_task = 30
-min_serialized_dag_fetch_interval = 10
-min_serialized_dag_update_interval = 30
-parallelism = 32
-plugins_folder = {AIRFLOW_HOME}/plugins
-security = 
-sensitive_var_conn_names = 
+min_serialized_dag_fetch_interval = 5
+min_serialized_dag_update_interval = 10
+parallelism = 16
+plugins_folder = $AIRFLOW_HOME/plugins
+sensitive_var_conn_names = 'cred,CRED,secret,SECRET,pass,PASS,password,Password,PASSWORD,private,PRIVATE,key,KEY,cert,CERT,token,TOKEN,AKIA'
 task_runner = StandardTaskRunner
 unit_test_mode = False
 xcom_backend = airflow.models.xcom.BaseXCom
 
-[dask]
-cluster_address = 127.0.0.1:8786
-tls_ca = 
-tls_cert = 
-tls_key = 
-
 [database]
-load_default_connections = True
+load_default_connections = False
 max_db_retries = 3
-sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/airflow.db
-sql_alchemy_max_overflow = 10
-sql_alchemy_pool_enabled = True
-sql_alchemy_pool_pre_ping = True
-sql_alchemy_pool_recycle = 1800
+sql_alchemy_conn = $AIRFLOW_DATABASE_URL
+sql_alchemy_pool_recycle = 3600
 sql_alchemy_pool_size = 5
-sql_alchemy_schema = 
-sql_engine_encoding = utf-8
 
 [debug]
 fail_fast = False
 
-[elasticsearch]
-end_of_log_mark = end_of_log
-frontend = 
-host = 
-host_field = host
-json_fields = asctime, filename, lineno, levelname, message
-json_format = False
-log_id_template = {{dag_id}}-{{task_id}}-{{run_id}}-{{map_index}}-{{try_number}}
-offset_field = offset
-write_stdout = False
-
-[elasticsearch_configs]
-use_ssl = False
-verify_certs = True
-
 [email]
 default_email_on_failure = True
 default_email_on_retry = True
-email_backend = airflow.utils.email.send_email_smtp
-email_conn_id = smtp_default
-
-[github_enterprise]
-api_rev = v3
-
-[hive]
-default_hive_mapred_queue = 
-
-[kerberos]
-ccache = /tmp/airflow_krb5_ccache
-forwardable = True
-include_ip = True
-keytab = airflow.keytab
-kinit_path = kinit
-principal = airflow
-reinit_frequency = 3600
-
-[kubernetes]
-delete_option_kwargs = 
-delete_worker_pods = True
-delete_worker_pods_on_failure = False
-enable_tcp_keepalive = True
-in_cluster = True
-kube_client_request_args = 
-multi_namespace_mode = False
-namespace = default
-pod_template_file = 
-tcp_keep_cnt = 6
-tcp_keep_idle = 120
-tcp_keep_intvl = 30
-verify_ssl = True
-worker_container_repository = 
-worker_container_tag = 
-worker_pods_creation_batch_size = 1
-worker_pods_pending_timeout = 300
-worker_pods_pending_timeout_batch_size = 100
-worker_pods_pending_timeout_check_interval = 120
-worker_pods_queued_check_interval = 60
+email_backend = $AIRFLOW_EMAIL_BACKEND
 
 [lineage]
-backend = 
-
-[local_kubernetes_executor]
-kubernetes_queue = kubernetes
 
 [logging]
-base_log_folder = {AIRFLOW_HOME}/logs
-celery_logging_level = 
+base_log_folder = $AIRFLOW_HOME/logs
 colored_console_log = True
 colored_formatter_class = airflow.utils.log.colored_log.CustomTTYColoredFormatter
 colored_log_format = [%%(blue)s%%(asctime)s%%(reset)s] {{%%(blue)s%%(filename)s:%%(reset)s%%(lineno)d}} %%(log_color)s%%(levelname)s%%(reset)s - %%(log_color)s%%(message)s%%(reset)s
-dag_processor_manager_log_location = {AIRFLOW_HOME}/logs/dag_processor_manager/dag_processor_manager.log
-encrypt_s3_logs = False
-extra_logger_names = 
+dag_processor_manager_log_location = $AIRFLOW_HOME/logs/dag_processor_manager/dag_processor_manager.log
 fab_logging_level = WARNING
-google_key_path = 
-log_filename_template = dag_id={{{{ ti.dag_id }}}}/run_id={{{{ ti.run_id }}}}/task_id={{{{ ti.task_id }}}}/{{%% if ti.map_index >= 0 %%}}map_index={{{{ ti.map_index }}}}/{{%% endif %%}}attempt={{{{ try_number }}}}.log
+log_filename_template = {{ ti.dag_id }}/{{ ti.task_id }}/{{ execution_date.strftime("%%Y-%%m-%%dT%%H:%%M:%%S") }}/{{ try_number }}.log
 log_format = [%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s
-log_formatter_class = airflow.utils.log.timezone_aware.TimezoneAware
-log_processor_filename_template = {{{{ filename }}}}.log
-logging_config_class = 
+log_processor_filename_template = {{ filename }}.log
 logging_level = INFO
-remote_base_log_folder = 
-remote_log_conn_id = 
-remote_logging = False
 simple_log_format = %%(asctime)s %%(levelname)s - %%(message)s
-task_log_prefix_template = 
 task_log_reader = task
 worker_log_server_port = 8793
 
+[mesos]
+authenticate = False
+checkpoint = False
+framework_name = Airflow
+master = localhost:5050
+task_cpu = 1
+task_memory = 256
+
 [metrics]
-stat_name_handler = 
-statsd_allow_list = 
 statsd_datadog_enabled = False
-statsd_datadog_tags = 
-statsd_host = localhost
-statsd_on = False
-statsd_port = 8125
-statsd_prefix = airflow
 
 [operators]
 allow_illegal_arguments = False
-default_cpus = 1
-default_disk = 512
-default_gpus = 0
-default_owner = airflow
 default_queue = default
-default_ram = 512
 
 [scheduler]
 allow_trigger_in_future = False
-catchup_by_default = True
-child_process_log_directory = {AIRFLOW_HOME}/logs/scheduler
-dag_dir_list_interval = 300
-deactivate_stale_dags_interval = 60
+catchup_by_default = False
+child_process_log_directory = $AIRFLOW_HOME/logs/scheduler
+dag_dir_list_interval = 30
+deactivate_stale_dags_interval = 120
 dependency_detector = airflow.serialization.serialized_objects.DependencyDetector
 file_parsing_sort_mode = modified_time
 ignore_first_depends_on_past_by_default = True
 job_heartbeat_sec = 5
 max_callbacks_per_loop = 20
-max_dagruns_per_loop_to_schedule = 20
-max_dagruns_to_create_per_loop = 10
 max_tis_per_query = 512
-min_file_process_interval = 30
+min_file_process_interval = 60
 num_runs = -1
 orphaned_tasks_check_interval = 300.0
 parsing_processes = 2
-pool_metrics_interval = 5.0
+pool_metrics_interval = 20.0
 print_stats_interval = 30
-schedule_after_task_execution = True
 scheduler_health_check_threshold = 30
 scheduler_heartbeat_sec = 5
 scheduler_idle_sleep_time = 1
@@ -252,43 +137,31 @@ standalone_dag_processor = False
 trigger_timeout_check_interval = 15
 use_job_schedule = True
 use_row_level_locking = True
-zombie_detection_interval = 10.0
-
-[secrets]
-backend = 
-backend_kwargs = 
+zombie_detection_interval = 60.0
 
 [sensors]
-default_timeout = 604800
+default_timeout = 259200
 
 [sentry]
-sentry_dsn = 
-sentry_on = false
-
-[smart_sensor]
-sensors_enabled = NamedHivePartitionSensor
-shard_code_upper_limit = 10000
-shards = 5
-use_smart_sensor = False
 
 [smtp]
-smtp_host = localhost
-smtp_mail_from = airflow@example.com
-smtp_port = 25
-smtp_retry_limit = 5
+smtp_host = $AIRFLOW_SMTP_HOST
+smtp_mail_from = $AIRFLOW_SMTP_FROM
+smtp_password = $AIRFLOW_SMTP_PASSWORD
+smtp_port = 587
 smtp_ssl = False
 smtp_starttls = True
-smtp_timeout = 30
+smtp_user = $AIRFLOW_SMTP_USER
 
 [triggerer]
 default_capacity = 1000
 
 [webserver]
 access_logfile = -
-access_logformat = 
 audit_view_excluded_events = gantt,landing_times,tries,duration,calendar,graph,grid,tree,tree_data
+auth_backend = $AIRFLOW_AUTH_BACKEND
 auto_refresh_interval = 3
-base_url = http://localhost:8080
+base_url = $URL
 cookie_samesite = Lax
 cookie_secure = False
 dag_default_view = grid
@@ -298,7 +171,7 @@ default_ui_timezone = UTC
 default_wrap = False
 enable_proxy_fix = False
 error_logfile = -
-expose_config = False
+expose_config = True
 expose_hostname = True
 expose_stacktrace = True
 hide_paused_dags_by_default = False
@@ -314,20 +187,15 @@ proxy_fix_x_host = 1
 proxy_fix_x_port = 1
 proxy_fix_x_prefix = 1
 proxy_fix_x_proto = 1
-reload_on_plugin_change = False
-secret_key = {SECRET_KEY}
-session_backend = database
+reload_on_plugin_change = True
 session_lifetime_minutes = 43200
 show_recent_stats_for_completed_runs = True
 update_fab_perms = True
 warn_deployment_exposure = True
 web_server_host = 0.0.0.0
-web_server_master_timeout = 120
-web_server_port = 8080
-web_server_ssl_cert = 
-web_server_ssl_key = 
-web_server_worker_timeout = 120
-worker_class = sync
+web_server_master_timeout = 300
+web_server_worker_timeout = 300
+worker_class = gevent
 worker_refresh_batch_size = 1
 worker_refresh_interval = 6000
 workers = 4
```