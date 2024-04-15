import os
from airflow import configuration as conf
from flask_appbuilder.security.manager import AUTH_DB
from flask_appbuilder.security.manager import AUTH_LDAP

basedir = os.path.abspath(os.path.dirname(__file__))

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = conf.get('core', 'SQL_ALCHEMY_CONN')

# Flask-WTF flag for CSRF
CSRF_ENABLED = False

AUTH_TYPE = AUTH_LDAP

# Uncomment to setup Full admin role name
AUTH_ROLE_ADMIN = 'Admin'

# Uncomment to setup Public role name, no authentication needed
AUTH_ROLE_PUBLIC = 'Public'

# Will allow user self registration
AUTH_USER_REGISTRATION = True

# The default user self registration role
AUTH_USER_REGISTRATION_ROLE = "Viewer"
# AUTH_LDAP_SERVER = "ldaps://114.115.222.184:636"
AUTH_LDAP_SERVER = "ldaps://internal.ldap-sg.ushareit.me:636"
AUTH_LDAP_USE_TLS = False
AUTH_LDAP_ALLOW_SELF_SIGNED = True
AUTH_LDAP_SEARCH = "ou=users,dc=ushareit,dc=me"
#AUTH_LDAP_SEARCH_FILTER = "(memberOf=cn=airflow,ou=systems,dc=ushareit,dc=me)"
AUTH_LDAP_BIND_USER = "cn=read,dc=ushareit,dc=me"
AUTH_LDAP_BIND_PASSWORD = "z8KFO0J9FMqvGtZi"
AUTH_LDAP_UID_FIELD = "uid"