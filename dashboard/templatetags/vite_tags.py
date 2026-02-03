import json
from pathlib import Path

from django import template

register = template.Library()


def _load_manifest():
    manifest_path = (
        Path(__file__).resolve().parent.parent / "static" / "frontend" / ".vite" / "manifest.json"
    )
    if not manifest_path.exists():
        return {}
    return json.loads(manifest_path.read_text(encoding="utf-8"))


@register.simple_tag
def vite_asset(entry):
    manifest = _load_manifest()
    data = manifest.get(entry, {})
    return data.get("file", "")


@register.simple_tag
def vite_css(entry):
    manifest = _load_manifest()
    data = manifest.get(entry, {})
    css_files = data.get("css", [])
    return css_files[0] if css_files else ""
