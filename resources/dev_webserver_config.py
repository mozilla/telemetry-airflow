import os

from flask_appbuilder.security.manager import AUTH_DB

basedir = os.path.abspath(os.path.dirname(__file__))

WTF_CSRF_ENABLED = True
AUTH_TYPE = AUTH_DB
AUTH_ROLE_PUBLIC = 'Admin'
