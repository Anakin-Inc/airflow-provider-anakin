"""
Apache Airflow provider for Anakin (https://api.anakin.io).

This module is the entry point Airflow's provider manager calls at startup
(via the ``apache_airflow_provider`` entry point declared in
``pyproject.toml``) to discover what this package contributes — a
connection type, a hook, and operators. Deliberately has zero Airflow
imports at module scope: ``get_provider_info()`` must be callable (and
callable fast) purely from metadata, without importing ``airflow.hooks.base``
or constructing anything. The actual hook/operator classes are imported
lazily by Airflow only when a DAG or the Connections UI needs them, via the
dotted paths returned below.
"""

from __future__ import annotations

from typing import Any

__version__ = "0.2.0"


def get_provider_info() -> dict[str, Any]:
    """Return provider metadata conforming to Airflow's provider_info schema.

    Structure (package-name/name/description/versions/integrations/
    connection-types/operators) confirmed against Airflow's real
    ``howto/create-custom-providers.html`` docs and the
    ``astronomer/airflow-provider-sample`` reference implementation's
    ``sample_provider/__init__.py`` and ``pyproject.toml`` — not guessed.
    """
    return {
        "package-name": "airflow-provider-anakin",
        "name": "Anakin",
        "description": (
            "Operators and a hook for the Anakin API (https://anakin.io): "
            "scrape, map, crawl, search, agentic search, Wire site actions, "
            "website monitors, AI visibility, browser sessions and AI browser "
            "tasks. Built on the official anakin-sdk Python client."
        ),
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": "Anakin",
                "external-doc-url": "https://anakin.io/docs",
                "tags": ["service"],
            }
        ],
        "connection-types": [
            {
                "connection-type": "anakin",
                "hook-class-name": "anakin_provider.hooks.anakin.AnakinHook",
                "hook-name": "Anakin",
                # Connection form metadata for Airflow 3's UI. Airflow 2 builds
                # the form from AnakinHook.get_ui_field_behaviour() instead.
                "ui-field-behaviour": {
                    "hidden-fields": ["schema", "login", "port", "host"],
                    "relabeling": {"password": "API Key"},
                    "placeholders": {"password": "ask_..."},
                },
                "conn-fields": {
                    "base_url": {
                        "label": "Base URL",
                        "description": "Defaults to https://api.anakin.io/v1 if left blank.",
                        "schema": {"type": ["string", "null"]},
                    }
                },
            }
        ],
        "operators": [
            {
                "integration-name": "Anakin",
                "python-modules": ["anakin_provider.operators.anakin"],
            }
        ],
    }
