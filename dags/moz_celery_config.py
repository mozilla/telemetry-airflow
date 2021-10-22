from airflow.config_templates.default_celery import DEFAULT_CELERY_CONFIG

CELERY_CONFIG = dict(DEFAULT_CELERY_CONFIG,
                     **{"worker_log_format": "[%(asctime)s] %(levelname)s - %(processName)s - %(message)s",
                        "worker_redirect_stdouts_level": "INFO"
                     })
