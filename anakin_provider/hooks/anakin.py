"""AnakinHook — Airflow Hook for the Anakin API.

Wraps the official ``anakin-sdk`` client (``anakin.Anakin``); this hook does
not reimplement HTTP calls, retries, or polling — all of that lives in the
SDK. The hook's job is exactly the job a hook is supposed to do: resolve an
Airflow Connection into SDK-client credentials and hand back a client (or
convenience passthrough methods) to operators.

The one exception is :meth:`AnakinHook.browser_task`: ``anakin-sdk`` 0.2
has no method for the AI browser-task endpoint (``POST /ai/evaluate``), so
that method calls the REST API directly with ``httpx`` (the HTTP library
``anakin-sdk`` itself depends on).

Class-attribute convention (``conn_name_attr``, ``default_conn_name``,
``conn_type``, ``hook_name``, ``get_connection_form_widgets``,
``get_ui_field_behaviour``) and the ``__init__(self, <x>_conn_id=...)`` /
``get_conn()`` shape mirror ``astronomer/airflow-provider-sample``.
"""

from __future__ import annotations

import os
import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from airflow.exceptions import AirflowNotFoundException

try:  # Airflow 3
    from airflow.sdk import BaseHook
except ImportError:  # Airflow 2
    from airflow.hooks.base import BaseHook  # type: ignore[no-redef,attr-defined]
from anakin import Anakin

if TYPE_CHECKING:
    from anakin.models import (
        AgenticSearchResult,
        AIVisibilitySearch,
        AIVisibilitySource,
        BatchScrapeResult,
        BrowserSession,
        CrawlResult,
        Document,
        MapResult,
        Monitor,
        MonitorChange,
        MonitorRun,
        SearchResult,
        WireActionMatch,
        WireBuildRequest,
        WireCatalog,
        WireCatalogDetail,
        WireIdentity,
        WireLoginResult,
        WireResult,
    )

DEFAULT_ANAKIN_BASE_URL = "https://api.anakin.io/v1"

#: Actions accepted by :meth:`AnakinHook.monitor_control`.
MONITOR_ACTIONS = frozenset({"pause", "resume", "run_now", "delete"})

#: Default wait for :meth:`AnakinHook.browser_task`. A task runs for up to
#: about 5 minutes server-side.
BROWSER_TASK_POLL_TIMEOUT = 360.0
BROWSER_TASK_POLL_INTERVAL = 3.0


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
        from flask_appbuilder.fieldwidgets import (  # type: ignore[import-not-found]
            BS3TextFieldWidget,
        )
        from flask_babel import lazy_gettext  # type: ignore[import-not-found]
        from wtforms import StringField  # type: ignore[import-untyped]

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
                "password": "ask_...",
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
        base_url = extra.get("base_url") or extra.get("extra__anakin__base_url") or base_url
        return api_key, base_url

    def get_conn(self) -> Anakin:
        """Return a cached ``anakin.Anakin`` SDK client for this connection.

        With no API key from either the Connection or ``ANAKIN_API_KEY``, the
        SDK runs in keyless mode: only ``scrape`` and Wire discovery work, and
        every other call raises ``anakin.ConfigurationError``.
        """
        if self._client is not None:
            return self._client
        api_key, base_url = self._resolve_credentials()
        self._client = Anakin(api_key=api_key, base_url=base_url)
        return self._client

    def test_connection(self) -> tuple[bool, str]:
        """Validate that an API key resolves for this connection.

        Deliberately does **not** call a billed endpoint: a "Test Connection"
        button in the Airflow UI should never spend the user's credits. This
        only checks that a client can be constructed with a non-empty API
        key. For a live check, run a DAG task that calls ``scrape()``.
        """
        api_key, _ = self._resolve_credentials()
        if not (api_key or os.environ.get("ANAKIN_API_KEY")):
            return False, (
                "No Anakin API key found in the connection's password field "
                "or the ANAKIN_API_KEY environment variable."
            )
        try:
            self.get_conn()
        except Exception as e:  # surfaced verbatim to the UI
            return False, str(e)
        return True, "Anakin client constructed with a resolved API key (no network call made)."

    # ─── passthrough convenience methods ───────────────────────────────────
    # Thin wrappers so operators (and ad-hoc @task code) can call
    # `AnakinHook(...).scrape(...)` without reaching into `.get_conn()`
    # themselves. Keyword arguments go straight to the matching
    # `anakin.Anakin` method.

    def scrape(self, url: str, **kwargs: Any) -> Document:
        return self.get_conn().scrape(url, **kwargs)

    def scrape_batch(self, urls: Sequence[str], **kwargs: Any) -> BatchScrapeResult:
        return self.get_conn().scrape_batch(urls, **kwargs)

    def search(self, prompt: str, **kwargs: Any) -> SearchResult:
        return self.get_conn().search(prompt, **kwargs)

    def agentic_search(self, prompt: str, **kwargs: Any) -> AgenticSearchResult:
        return self.get_conn().agentic_search(prompt, **kwargs)

    def map(self, url: str, **kwargs: Any) -> MapResult:
        return self.get_conn().map(url, **kwargs)

    def crawl(self, url: str, **kwargs: Any) -> CrawlResult:
        return self.get_conn().crawl(url, **kwargs)

    # ─── Wire ──────────────────────────────────────────────────────────────
    # Read and write actions hit the same endpoint (`POST /wire/task`). They
    # are separate hook methods and operators, as in `anakin-mcp`, so a DAG
    # states whether a task can change state on the target site.

    def wire_discover(
        self,
        q: str,
        *,
        catalog: str | None = None,
        category: str | None = None,
        auth_mode: str | None = None,
        limit: int | None = None,
    ) -> list[WireActionMatch]:
        return self.get_conn().wire.discover(
            q,
            catalog=catalog,
            category=category,
            auth_mode=auth_mode,  # type: ignore[arg-type]
            limit=limit,
        )

    def wire_catalog(self, slug: str | None = None) -> WireCatalogDetail | list[WireCatalog]:
        """One site's actions when ``slug`` is set, else every catalog."""
        wire = self.get_conn().wire
        return wire.catalog(slug) if slug else wire.catalogs()

    def wire_read_action(
        self,
        action_id: str,
        params: Mapping[str, Any] | None = None,
        *,
        credential_id: str | None = None,
        identity_id: str | None = None,
        poll_timeout: float | None = None,
    ) -> WireResult:
        return self.get_conn().wire.run(
            action_id,
            params or {},
            credential_id=credential_id,
            identity_id=identity_id,
            poll_timeout=poll_timeout,
        )

    def wire_write_action(
        self,
        action_id: str,
        params: Mapping[str, Any] | None = None,
        *,
        credential_id: str | None = None,
        identity_id: str | None = None,
        poll_timeout: float | None = None,
    ) -> WireResult:
        return self.get_conn().wire.run(
            action_id,
            params or {},
            credential_id=credential_id,
            identity_id=identity_id,
            poll_timeout=poll_timeout,
        )

    def wire_identities(self, catalog_id: str | None = None) -> list[WireIdentity]:
        return self.get_conn().wire.identities(catalog_id=catalog_id)

    def wire_login(
        self,
        catalog_slug: str,
        params: Mapping[str, Any] | None = None,
        *,
        identity_name: str | None = None,
        source_id: str | None = None,
        source_ref: Mapping[str, Any] | None = None,
    ) -> WireLoginResult:
        return self.get_conn().wire.login(
            catalog_slug,
            params,
            identity_name=identity_name,
            source_id=source_id,
            source_ref=source_ref,
        )

    def wire_build(self, website_url: str, goal: str, **kwargs: Any) -> WireBuildRequest:
        return self.get_conn().wire.build(website_url, goal, **kwargs)

    def wire_build_status(self, build_id: str) -> WireBuildRequest:
        return self.get_conn().wire.get_build(build_id)

    # ─── Website monitoring ────────────────────────────────────────────────

    def monitor_create(self, url: str, interval_minutes: int, **options: Any) -> Monitor:
        """Create a monitor. ``options`` are ``anakin.types.MonitorOptions``
        keys; ``None`` values are dropped so the API defaults apply."""
        set_options = {k: v for k, v in options.items() if v is not None}
        return self.get_conn().monitors.create(url, interval_minutes, **set_options)

    def monitor_list(self) -> list[Monitor]:
        return self.get_conn().monitors.list()

    def monitor_changes(self, monitor_id: str) -> list[MonitorChange]:
        return self.get_conn().monitors.changes(monitor_id)

    def monitor_control(self, monitor_id: str, action: str) -> Monitor | MonitorRun | None:
        """Pause, resume, run now, or delete a monitor.

        :param action: One of ``"pause"``, ``"resume"``, ``"run_now"``, ``"delete"``.
        """
        if action not in MONITOR_ACTIONS:
            raise ValueError(
                f"Unknown monitor action {action!r}; expected one of {sorted(MONITOR_ACTIONS)}."
            )
        monitors = self.get_conn().monitors
        if action == "pause":
            return monitors.pause(monitor_id)
        if action == "resume":
            return monitors.resume(monitor_id)
        if action == "run_now":
            return monitors.run_now(monitor_id)
        monitors.delete(monitor_id)
        return None

    # ─── AI visibility ─────────────────────────────────────────────────────

    def ai_visibility_sources(self) -> list[AIVisibilitySource]:
        return self.get_conn().ai_visibility.sources()

    def ai_visibility_search(
        self,
        query: str,
        *,
        sources: Sequence[str] | None = None,
        country: str = "us",
        poll_timeout: float | None = None,
    ) -> AIVisibilitySearch:
        """Raises ``anakin.JobFailedError`` only when every source failed."""
        return self.get_conn().ai_visibility.search(
            query, sources=sources, country=country, poll_timeout=poll_timeout
        )

    # ─── Browser sessions ──────────────────────────────────────────────────

    def session_list(self, *, domain: str | None = None) -> list[BrowserSession]:
        return self.get_conn().sessions.list(domain=domain)

    def session_delete(self, session_id: str) -> None:
        self.get_conn().sessions.delete(session_id)

    # ─── AI browser task (not in anakin-sdk 0.2) ───────────────────────────

    def browser_task(
        self,
        prompt: str,
        *,
        url: str | None = None,
        session_id: str | None = None,
        max_steps: int | None = None,
        timeout_ms: int | None = None,
        output_schema: Mapping[str, Any] | None = None,
        poll_timeout: float | None = None,
    ) -> dict[str, Any]:
        """Run a natural-language task in an AI-driven cloud browser.

        Submits ``POST /ai/evaluate`` with ``async: true`` and polls
        ``GET /ai/jobs/:id`` until the job is ``completed``. Returns the job
        body. Raises ``anakin.JobFailedError`` on ``failed``/``timed_out``
        and ``anakin.JobTimeoutError`` if ``poll_timeout`` passes first.
        """
        import httpx
        from anakin.errors import (
            AnakinError,
            ConfigurationError,
            JobFailedError,
            JobTimeoutError,
        )

        api_key, base_url = self._resolve_credentials()
        api_key = api_key or os.environ.get("ANAKIN_API_KEY")
        if not api_key:
            raise ConfigurationError(
                "The browser task needs an Anakin API key: set the connection's "
                "password or ANAKIN_API_KEY. Get one free at https://anakin.io/dashboard."
            )
        body: dict[str, Any] = {"prompt": prompt, "async": True}
        optional = {
            "url": url,
            "session_id": session_id,
            "max_steps": max_steps,
            "timeout_ms": timeout_ms,
            "output_schema": dict(output_schema) if output_schema is not None else None,
        }
        body.update({k: v for k, v in optional.items() if v is not None})

        def request(http: httpx.Client, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
            try:
                res = http.request(method, path, **kwargs)
            except httpx.HTTPError as err:
                raise AnakinError(f"Could not reach the Anakin API: {err}") from err
            try:
                data = res.json() if res.content else {}
            except ValueError:
                data = {}
            if res.status_code >= 400:
                detail = None
                if isinstance(data, dict):
                    detail = data.get("message") or data.get("error")
                raise AnakinError(
                    f"{method} {path} failed with HTTP {res.status_code}: "
                    f"{detail or res.text[:200]}",
                    status_code=res.status_code,
                    body=data,
                )
            return data if isinstance(data, dict) else {"data": data}

        timeout = BROWSER_TASK_POLL_TIMEOUT if poll_timeout is None else poll_timeout
        with httpx.Client(
            base_url=base_url.rstrip("/"), headers={"X-API-Key": api_key}, timeout=30.0
        ) as http:
            accepted = request(http, "POST", "/ai/evaluate", json=body)
            workflow_id = accepted.get("workflow_id")
            if not workflow_id:
                return accepted
            deadline = time.monotonic() + timeout
            path = f"/ai/jobs/{quote(str(workflow_id), safe='')}"
            while True:
                job = request(http, "GET", path)
                status = job.get("status")
                if status == "completed":
                    return job
                if status in ("failed", "timed_out"):
                    raise JobFailedError(
                        f"Browser task {status}: {job.get('error') or 'unknown error'}",
                        job_id=str(workflow_id),
                        body=job,
                    )
                if time.monotonic() >= deadline:
                    raise JobTimeoutError(
                        f"Browser task {workflow_id} still running after {timeout:g}s.",
                        job_id=str(workflow_id),
                        body=job,
                    )
                time.sleep(BROWSER_TASK_POLL_INTERVAL)
