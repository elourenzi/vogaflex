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


def _normalize_entry(entry, manifest):
    if entry in manifest:
        return entry
    if "index.html" in manifest:
        return "index.html"
    return entry


def _prefix_frontend(path):
    if not path:
        return ""
    if path.startswith("frontend/"):
        return path
    return f"frontend/{path}"


@register.simple_tag
def vite_asset(entry):
    manifest = _load_manifest()
    entry_key = _normalize_entry(entry, manifest)
    data = manifest.get(entry_key, {})
    return _prefix_frontend(data.get("file", ""))


@register.simple_tag
def vite_css(entry):
    manifest = _load_manifest()
    entry_key = _normalize_entry(entry, manifest)
    data = manifest.get(entry_key, {})
    css_files = data.get("css", [])
    return _prefix_frontend(css_files[0]) if css_files else ""
