import os

from airflow import configuration as conf
from flask_appbuilder.security.manager import AUTH_OAUTH

basedir = os.path.abspath(os.path.dirname(__file__))
SQLALCHEMY_DATABASE_URI = conf.conf.get("database", "SQL_ALCHEMY_CONN")
CSRF_ENABLED = True
AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"

GOOGLE_KEY = os.getenv("AIRFLOW_GOOGLE_CLIENT_ID", "GOOGLE_KEY_NOT_SET")
GOOGLE_SECRET = os.getenv("AIRFLOW_GOOGLE_CLIENT_SECRET", "GOOGLE_SECRET_NOT_SET")
OAUTH_PROVIDERS = [
    {
        "name": "google",
        "whitelist": ["@mozilla.com"],
        "token_key": "access_token",
        "icon": "fa-google",
        "remote_app": {
            "api_base_url": "https://www.googleapis.com/oauth2/v2/",
            "client_kwargs": {
                "scope": "email profile"
            },
            "access_token_url": "https://accounts.google.com/o/oauth2/token",
            "authorize_url": "https://accounts.google.com/o/oauth2/auth",
            "request_token_url": None,
            "client_id": GOOGLE_KEY,
            "client_secret": GOOGLE_SECRET,
        }
    },
]