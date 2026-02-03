"""
ASGI config for vogaflex project.
"""
import os

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from django.core.asgi import get_asgi_application

if load_dotenv:
    load_dotenv()
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "vogaflex.settings")

application = get_asgi_application()
