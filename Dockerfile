FROM node:20-alpine AS frontend
WORKDIR /app

COPY frontend/package.json frontend/package-lock.json ./frontend/
RUN cd frontend && npm ci

COPY frontend ./frontend
RUN cd frontend && npm run build


FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    DJANGO_SETTINGS_MODULE=vogaflex.settings

WORKDIR /app

COPY requirements.txt ./
RUN pip install --retries 10 -r requirements.txt

COPY . .

# Copia apenas o build do front para onde o Django espera servir estático
# (ajuste o destino se seu build não cair nesse path)
COPY --from=frontend /app/frontend/dist /app/dashboard/static/frontend

# NÃO roda collectstatic no build (evita quebra por env ausente)
# Você roda isso no start, quando EasyPanel já injetou SECRET_KEY, DB, etc.

CMD ["sh", "-c", "python manage.py collectstatic --noinput && gunicorn vogaflex.wsgi:application --bind 0.0.0.0:${PORT:-8000} --workers ${WEB_CONCURRENCY:-2} --threads ${GUNICORN_THREADS:-4} --timeout ${GUNICORN_TIMEOUT:-120}"]

