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

__version__ = "0.2.0"


def get_provider_info() -> dict:
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
            "Full coverage of the Anakin API (https://api.anakin.io/v1) for "
            "Apache Airflow: web scraping, site mapping/crawling, AI-powered "
            "search and multi-stage agentic research, Wire pre-built site "
            "automations, website change monitoring, AI-visibility (LLM "
            "answer-engine) search, browser-session management, and AI "
            "browser automation. Built on the official ``anakin-sdk`` "
            "Python client where it covers an endpoint, with direct HTTP "
            "calls (reusing the SDK's authenticated transport) for the "
            "handful of endpoints the SDK does not wrap yet."
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
            }
        ],
        "operators": [
            {
                "integration-name": "Anakin",
                "python-modules": ["anakin_provider.operators.anakin"],
            }
        ],
    }
