#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def main() -> None:
    """Run administrative tasks."""
    if load_dotenv:
        load_dotenv()
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "vogaflex.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Django nao esta instalado. Instale com `pip install -r requirements.txt`."
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
