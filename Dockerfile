FROM node:20-alpine AS frontend
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json ./frontend/
RUN cd frontend && npm ci
COPY frontend ./frontend
COPY dashboard ./dashboard
RUN cd frontend && npm run build

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DEFAULT_TIMEOUT=120
WORKDIR /app
COPY requirements.txt ./
RUN pip install --retries 10 -r requirements.txt
COPY . .
COPY --from=frontend /app/dashboard/static/frontend /app/dashboard/static/frontend
RUN python manage.py collectstatic --noinput
CMD ["gunicorn", "vogaflex.wsgi:application", "--bind", "0.0.0.0:8000"]
