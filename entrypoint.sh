#!/bin/sh
set -e

# Collect static files
python manage.py collectstatic --noinput

# Run DB setup in background (heavy backfill, don't block startup)
python manage.py setup_db &

# Background: run sync_smclick every 5 minutes
(
  while true; do
    sleep 300
    python manage.py sync_smclick 2>&1 | head -20
  done
) &

# Background: run sync_orders every 30 minutes
(
  sleep 60
  python manage.py sync_orders --days 7 2>&1 | tail -5
  while true; do
    sleep 1800
    python manage.py sync_orders --days 7 2>&1 | tail -5
  done
) &

# Start gunicorn (foreground)
exec gunicorn vogaflex.wsgi:application \
  --bind "0.0.0.0:${PORT:-8000}" \
  --workers "${WEB_CONCURRENCY:-2}" \
  --threads "${GUNICORN_THREADS:-4}" \
  --timeout "${GUNICORN_TIMEOUT:-120}"
