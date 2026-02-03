"""
WSGI config for vogaflex project.
"""
import os

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from django.core.wsgi import get_wsgi_application

if load_dotenv:
    load_dotenv()
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "vogaflex.settings")

application = get_wsgi_application()
