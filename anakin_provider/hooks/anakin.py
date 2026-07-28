"""AnakinHook — Airflow Hook for the Anakin API.

Wraps the official ``anakin-sdk`` client (``anakin.Anakin``); this hook does
not reimplement HTTP calls, retries, or polling — all of that lives in the
SDK. The hook's job is exactly the job a hook is supposed to do: resolve an
Airflow Connection into SDK-client credentials and hand back a client (or
convenience passthrough methods) to operators.

Class-attribute convention (``conn_name_attr``, ``default_conn_name``,
``conn_type``, ``hook_name``, ``get_connection_form_widgets``,
``get_ui_field_behaviour``) and the ``__init__(self, <x>_conn_id=...)`` /
``get_conn()`` shape mirror ``astronomer/airflow-provider-sample``'s
``sample_provider/hooks/sample.py`` — the real, current reference
implementation Astronomer publishes for exactly this purpose.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook

from anakin import Anakin

if TYPE_CHECKING:
    from anakin.models import AgenticSearchResult, Document, SearchResult

DEFAULT_ANAKIN_BASE_URL = "https://api.anakin.io/v1"


class AnakinHook(BaseHook):
    """Hook for the Anakin API (https://api.anakin.io/v1).

    :param anakin_conn_id: Airflow connection id. The connection's
        ``password`` field is used as the Anakin API key; an optional
        ``base_url`` key in the connection's Extra JSON overrides the
        default API base URL. If no Connection row with this id exists,
        falls through to the ``ANAKIN_API_KEY`` environment variable — the
        same fallback ``anakin.Anakin()`` implements itself — so DAGs can
        run purely off a Secrets-Backend-injected env var with no
        Connection object configured at all.
    """

    conn_name_attr = "anakin_conn_id"
    default_conn_name = "anakin_default"
    conn_type = "anakin"
    hook_name = "Anakin"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Extra field(s) shown on the Airflow UI's "Add Connection" form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "base_url": StringField(
                lazy_gettext("Base URL"),
                widget=BS3TextFieldWidget(),
                description="Defaults to https://api.anakin.io/v1 if left blank.",
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Hide/relabel the generic Connection form fields that don't apply
        to a simple API-key-in-a-header connection."""
        return {
            "hidden_fields": ["schema", "login", "port", "host"],
            "relabeling": {
                "password": "API Key",
            },
            "placeholders": {
                "password": "ask-...",
            },
        }

    def __init__(self, anakin_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.anakin_conn_id = anakin_conn_id
        self._client: Anakin | None = None

    def _resolve_credentials(self) -> tuple[str | None, str]:
        """Return ``(api_key, base_url)`` from the Airflow Connection, if any.

        ``api_key`` is ``None`` (not an empty string) when there is no
        Connection row and no password field, so the caller can distinguish
        "no key configured here" from "key resolves via env var elsewhere."
        """
        api_key: str | None = None
        base_url = DEFAULT_ANAKIN_BASE_URL
        try:
            conn = self.get_connection(self.anakin_conn_id)
        except AirflowNotFoundException:
            self.log.info(
                "No Airflow connection '%s' found; anakin.Anakin() will fall "
                "back to the ANAKIN_API_KEY environment variable.",
                self.anakin_conn_id,
            )
            return api_key, base_url

        api_key = conn.password or None
        extra = conn.extra_dejson or {}
        if extra.get("base_url"):
            base_url = extra["base_url"]
        return api_key, base_url

    def get_conn(self) -> Anakin:
        """Return a cached ``anakin.Anakin`` SDK client for this connection.

        Raises ``anakin.errors.ConfigurationError`` (propagated from the
        SDK, not swallowed) if no API key resolves from either the
        Connection or the ``ANAKIN_API_KEY`` environment variable.
        """
        if self._client is not None:
            return self._client
        api_key, base_url = self._resolve_credentials()
        self._client = Anakin(api_key=api_key, base_url=base_url)
        return self._client

    def test_connection(self) -> tuple[bool, str]:
        """Validate that an API key resolves for this connection.

        Deliberately does **not** call a billed endpoint — ``/search`` costs
        3 credits and ``/agentic-search`` costs 10 credits per call per the
        Anakin API contract, and a "Test Connection" button firing in the
        Airflow UI should never spend the user's credits. This only checks
        that a client can be constructed with a non-empty API key. For a
        real live check, run a DAG task that calls ``scrape()``/``search()``.
        """
        api_key, _ = self._resolve_credentials()
        if not (api_key or os.environ.get("ANAKIN_API_KEY")):
            return False, (
                "No Anakin API key found in the connection's password field "
                "or the ANAKIN_API_KEY environment variable."
            )
        try:
            self.get_conn()
        except Exception as e:  # noqa: BLE001 — surfaced verbatim to the UI
            return False, str(e)
        return True, "Anakin client constructed with a resolved API key (no network call made)."

    # ─── passthrough convenience methods ───────────────────────────────────
    # Thin wrappers so operators (and ad-hoc PythonOperator/@task code) can
    # call `AnakinHook(...).scrape(...)` without reaching into `.get_conn()`
    # themselves. Signatures mirror `anakin.Anakin`'s methods exactly.

    def scrape(self, url: str, **kwargs: Any) -> Document:
        return self.get_conn().scrape(url, **kwargs)

    def search(self, prompt: str, **kwargs: Any) -> SearchResult:
        return self.get_conn().search(prompt, **kwargs)

    def agentic_search(self, prompt: str, **kwargs: Any) -> AgenticSearchResult:
        return self.get_conn().agentic_search(prompt, **kwargs)
