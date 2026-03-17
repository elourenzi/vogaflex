#!/bin/sh
set -e

# Collect static files
python manage.py collectstatic --noinput

# Background: run sync_smclick every 5 minutes
(
  while true; do
    sleep 300
    python manage.py sync_smclick 2>&1 | head -20
  done
) &

# Start gunicorn (foreground)
exec gunicorn vogaflex.wsgi:application \
  --bind "0.0.0.0:${PORT:-8000}" \
  --workers "${WEB_CONCURRENCY:-2}" \
  --threads "${GUNICORN_THREADS:-4}" \
  --timeout "${GUNICORN_TIMEOUT:-120}"
