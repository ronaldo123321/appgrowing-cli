"""JSON schema loading and validation helpers."""

from __future__ import annotations

import json
from importlib import resources
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


SCHEMA_DIR_NAME = "schemas"


def _candidate_schema_dirs() -> list[Path]:
    current_file = Path(__file__).resolve()
    repo_root = current_file.parent.parent
    cwd = Path.cwd()
    return [
        repo_root / SCHEMA_DIR_NAME,
        cwd / SCHEMA_DIR_NAME,
    ]


def _load_schema_from_package(schema_file_name: str) -> dict[str, Any] | None:
    try:
        schema_path = resources.files("appgrowing_cli").joinpath(SCHEMA_DIR_NAME, schema_file_name)
    except (FileNotFoundError, ModuleNotFoundError):
        return None
    if not schema_path.is_file():
        return None
    return json.loads(schema_path.read_text(encoding="utf-8"))


def load_schema(schema_file_name: str) -> dict[str, Any]:
    """Load schema json from known schema directories."""
    packaged = _load_schema_from_package(schema_file_name)
    if isinstance(packaged, dict):
        return packaged

    for schema_dir in _candidate_schema_dirs():
        schema_path = schema_dir / schema_file_name
        if schema_path.exists():
            return json.loads(schema_path.read_text(encoding="utf-8"))
    searched = ", ".join(str(d) for d in _candidate_schema_dirs())
    raise FileNotFoundError(
        f"Schema {schema_file_name} not found in package data or local dirs. Searched dirs: {searched}"
    )


def validate_payload(schema_file_name: str, payload: dict[str, Any]) -> None:
    """Validate payload against a schema file."""
    schema = load_schema(schema_file_name)
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda e: e.path)
    if errors:
        first = errors[0]
        path = ".".join(str(p) for p in first.path) or "<root>"
        raise ValueError(f"Schema validation failed at {path}: {first.message}")
