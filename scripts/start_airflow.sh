#!/usr/bin/env bash
# Local demo only: make the documented admin/admin login consistent across restarts.
set -euo pipefail

rm -f "${AIRFLOW_HOME}/airflow-webserver.pid"
airflow db migrate
if airflow users list -o json | python -c 'import json,sys; sys.exit(0 if any(u.get("username") == "admin" for u in json.load(sys.stdin)) else 1)'; then
    airflow users reset-password --username admin --password admin
else
    airflow users create --username admin --firstname Admin --lastname User \
        --role Admin --email admin@example.com --password admin
fi
printf '%s' admin > "${AIRFLOW_HOME}/standalone_admin_password.txt"
exec airflow standalone
