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
    from anakin.models import (
        AgenticSearchResult,
        BrowserSession,
        CrawlResult,
        Document,
        MapResult,
        SearchResult,
        WireResult,
    )

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

    def map(self, url: str, **kwargs: Any) -> MapResult:
        return self.get_conn().map(url, **kwargs)

    def crawl(self, url: str, **kwargs: Any) -> CrawlResult:
        return self.get_conn().crawl(url, **kwargs)

    # ─── Wire: execution (covered by anakin-sdk's Anakin.wire()) ───────────
    #
    # Anakin's own MCP server (anakin-mcp/src/tools/wire.ts) splits Wire
    # action execution into two tools — wire_read_action / wire_write_action
    # — that share one implementation and differ only in the safety
    # annotation attached (read-only vs destructive), because the Wire
    # Connectors Directory rejects a single catch-all tool mixing safe and
    # unsafe operations behind one parameter. Both wrap the same
    # POST /wire/task (+ poll GET /wire/jobs/:id) endpoint, which
    # `anakin-sdk` already implements as `Anakin.wire(action_id, params)` —
    # so both hook methods below delegate to it, same as the two tools
    # share `runWireAction()` in the MCP server.
    #
    # Known gap vs. the full REST API: the TypeScript client
    # (anakin-mcp/src/client.ts `wireRun`) also accepts `credential_id` /
    # `identity_id` for actions whose auth_mode is "required"/"optional".
    # `anakin-sdk` v0.1.0's `Anakin.wire()` does not expose either
    # parameter yet (its request body is only `{action_id, params}`) — so
    # these two operators currently only support actions with
    # `auth_mode: none`. Documented here (and in SUBMIT.md) rather than
    # silently reimplementing raw HTTP around the SDK to add the missing
    # parameters, per this package's "don't reimplement what the SDK
    # already covers" convention. Revisit once anakin-sdk adds support.

    def wire_read_action(
        self,
        action_id: str,
        params: dict[str, Any] | None = None,
        *,
        poll_timeout: float | None = None,
    ) -> WireResult:
        return self.get_conn().wire(action_id, params or {}, poll_timeout=poll_timeout)

    def wire_write_action(
        self,
        action_id: str,
        params: dict[str, Any] | None = None,
        *,
        poll_timeout: float | None = None,
    ) -> WireResult:
        return self.get_conn().wire(action_id, params or {}, poll_timeout=poll_timeout)

    # ─── Browser sessions (covered by anakin-sdk's Anakin.sessions.*) ───────

    def session_list(self, *, domain: str | None = None) -> list[BrowserSession]:
        return self.get_conn().sessions.list(domain=domain)

    def session_delete(self, session_id: str) -> None:
        self.get_conn().sessions.delete(session_id)

    # ─── Endpoints not yet wrapped by anakin-sdk's public surface ──────────
    #
    # Wire discovery/catalog/identities/login/build, Website Monitoring,
    # AI Visibility, and the AI browser-task endpoint have no public
    # `anakin-sdk` methods as of v0.1.0 (confirmed by reading
    # anakin-py/src/anakin/client.py directly — only `scrape`, `map`,
    # `crawl`, `search`, `agentic_search`, `wire`, `countries`, and
    # `sessions.*` exist there). Rather than hand-roll a second HTTP client
    # with its own auth-header/retry/backoff logic, the methods below reuse
    # the SDK client's own internal transport (`anakin.Anakin()._http`, an
    # `anakin._http.HttpClient`) — the exact same authenticated, retrying
    # `httpx` client `scrape()`/`map()`/etc. already use. This is a
    # deliberate, documented reach into a `_`-prefixed internal, made safe
    # by this package's `anakin-sdk>=0.1.0,<0.2.0` pin; switch to public
    # SDK methods if/when a later SDK minor adds them.
    #
    # Endpoint paths/bodies below are read directly from Anakin's own MCP
    # server (anakin-mcp/src/client.ts and src/tools/{wire,monitor,
    # ai-visibility,browser-task}.ts) — the same ground truth already
    # confirmed for the endpoints anakin-sdk *does* implement.

    def _raw_http(self) -> Any:
        """Return the SDK client's internal ``HttpClient`` (see module note above)."""
        return self.get_conn()._http  # noqa: SLF001 — intentional, see docstring above

    def wire_discover(self, q: str, *, limit: int | None = None) -> dict[str, Any]:
        """``GET /wire/resolve`` — natural-language intent to ranked candidate actions."""
        params: dict[str, Any] = {"q": q}
        if limit is not None:
            params["limit"] = limit
        return self._raw_http().get("/wire/resolve", params=params)

    def wire_catalog(self, slug: str | None = None) -> dict[str, Any]:
        """``GET /wire/catalog`` (all catalogs) or ``GET /wire/catalog/:slug``."""
        path = f"/wire/catalog/{slug}" if slug else "/wire/catalog"
        return self._raw_http().get(path)

    def wire_identities(self, catalog_id: str | None = None) -> dict[str, Any]:
        """``GET /wire/identities`` — saved Wire identities/credentials."""
        params = {"catalog_id": catalog_id} if catalog_id else None
        return self._raw_http().get("/wire/identities", params=params)

    def wire_login(
        self,
        catalog_slug: str,
        *,
        params: dict[str, Any] | None = None,
        identity_name: str | None = None,
        source_id: str | None = None,
        source_ref: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """``POST /wire/login`` — credentials-mode sign-in to a Wire catalog site."""
        body: dict[str, Any] = {"catalog_slug": catalog_slug}
        if params is not None:
            body["params"] = params
        if identity_name is not None:
            body["identity_name"] = identity_name
        if source_id is not None:
            body["source_id"] = source_id
        if source_ref is not None:
            body["source_ref"] = source_ref
        return self._raw_http().post("/wire/login", json=body)

    def wire_build(
        self,
        website_url: str,
        goal: str,
        *,
        catalog_id: str | None = None,
        visibility: str | None = None,
        force: bool | None = None,
    ) -> dict[str, Any]:
        """``POST /wire/build-request`` — request a new Wire action for an
        unsupported site."""
        body: dict[str, Any] = {"website_url": website_url, "goal": goal}
        if catalog_id is not None:
            body["catalog_id"] = catalog_id
        if visibility is not None:
            body["visibility"] = visibility
        if force is not None:
            body["force"] = force
        return self._raw_http().post("/wire/build-request", json=body)

    def monitor_create(
        self,
        url: str,
        interval_minutes: int,
        *,
        scope: str | None = None,
        watch_mode: str | None = None,
        watch_format: str | None = None,
        output_schema: dict[str, Any] | None = None,
        ai_mode: bool | None = None,
        ai_goal: str | None = None,
        use_browser: bool | None = None,
        country: str | None = None,
        session_id: str | None = None,
        is_active: bool | None = None,
        expires_at: str | None = None,
        alert_webhook_url: str | None = None,
        alert_emails: str | None = None,
        max_pages: int | None = None,
        max_depth: int | None = None,
        include_patterns: list[str] | tuple[str, ...] | None = None,
        exclude_patterns: list[str] | tuple[str, ...] | None = None,
        wire_action_id: str | None = None,
        wire_catalog_slug: str | None = None,
        wire_credential_id: str | None = None,
        wire_params: dict[str, Any] | None = None,
        wire_watch_paths: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        """``POST /monitors`` — create a scheduled website monitor.

        Keyword args are only sent when set (not ``None``) so the API's own
        defaults apply otherwise — mirrors the SDK's own body-building
        convention (see ``Anakin.scrape``/``Anakin.map``).
        """
        body: dict[str, Any] = {"url": url, "intervalMinutes": interval_minutes}
        optional_fields: dict[str, Any] = {
            "scope": scope,
            "watchMode": watch_mode,
            "watchFormat": watch_format,
            "outputSchema": output_schema,
            "aiMode": ai_mode,
            "aiGoal": ai_goal,
            "useBrowser": use_browser,
            "country": country,
            "sessionId": session_id,
            "isActive": is_active,
            "expiresAt": expires_at,
            "alertWebhookUrl": alert_webhook_url,
            "alertEmails": alert_emails,
            "maxPages": max_pages,
            "maxDepth": max_depth,
            "includePatterns": list(include_patterns) if include_patterns else None,
            "excludePatterns": list(exclude_patterns) if exclude_patterns else None,
            "wireActionId": wire_action_id,
            "wireCatalogSlug": wire_catalog_slug,
            "wireCredentialId": wire_credential_id,
            "wireParams": wire_params,
            "wireWatchPaths": list(wire_watch_paths) if wire_watch_paths else None,
        }
        for key, value in optional_fields.items():
            if value is not None:
                body[key] = value
        return self._raw_http().post("/monitors", json=body)

    def monitor_list(self) -> dict[str, Any]:
        """``GET /monitors`` — list all website monitors."""
        return self._raw_http().get("/monitors")

    def monitor_changes(self, monitor_id: str) -> dict[str, Any]:
        """``GET /monitors/:id/changes`` — detected changes for one monitor."""
        return self._raw_http().get(f"/monitors/{monitor_id}/changes")

    def monitor_control(self, monitor_id: str, action: str) -> dict[str, Any]:
        """Pause/resume/run/delete a monitor.

        :param action: One of ``"pause"``, ``"resume"``, ``"run_now"``,
            ``"delete"``. ``pause``/``resume``/``run_now`` POST to
            ``/monitors/:id/{pause,resume,run}``; ``delete`` sends
            ``DELETE /monitors/:id``.
        """
        valid_actions = {"pause", "resume", "run_now", "delete"}
        if action not in valid_actions:
            raise ValueError(
                f"Unknown monitor action {action!r}; expected one of {sorted(valid_actions)}."
            )
        base = f"/monitors/{monitor_id}"
        http = self._raw_http()
        if action == "delete":
            return http.delete(base)
        path_suffix = "run" if action == "run_now" else action
        return http.post(f"{base}/{path_suffix}")

    def ai_visibility_sources(self) -> dict[str, Any]:
        """``GET /ai-visibility/sources`` — AI engines available to search."""
        return self._raw_http().get("/ai-visibility/sources")

    def ai_visibility_search(
        self,
        query: str,
        *,
        sources: list[str] | tuple[str, ...] | None = None,
        country: str | None = None,
        poll_timeout: float | None = None,
    ) -> dict[str, Any]:
        """``POST /ai-visibility/search``, polls ``GET .../search/:id`` until
        the run leaves ``"running"`` — a ``"failed"`` result is returned
        (not raised) since it still carries per-source results/errors.
        """
        body: dict[str, Any] = {"query": query}
        if sources is not None:
            body["sources"] = list(sources)
        if country is not None:
            body["country"] = country

        client = self.get_conn()
        http = self._raw_http()
        submitted = http.post("/ai-visibility/search", json=body)
        search_id = submitted.get("search_id")
        if not search_id:
            return submitted
        return http.poll(
            f"/ai-visibility/search/{search_id}",
            is_terminal=lambda body: body.get("status") != "running",
            poll_interval=client._poll_interval,  # noqa: SLF001 — see module note
            poll_max_interval=client._poll_max_interval,  # noqa: SLF001
            poll_timeout=poll_timeout if poll_timeout is not None else client._poll_timeout,  # noqa: SLF001
        )

    def browser_task(
        self,
        prompt: str,
        *,
        url: str | None = None,
        session_id: str | None = None,
        max_steps: int | None = None,
        timeout_ms: int | None = None,
        output_schema: dict[str, Any] | None = None,
        poll_timeout: float | None = None,
    ) -> dict[str, Any]:
        """``POST /ai/evaluate`` (async), polls ``GET /ai/jobs/:id``.

        Always submits with ``async: true`` — the synchronous variant blocks
        the HTTP request for up to ~5.5 minutes, far past a sane per-request
        timeout, per ``anakin-mcp``'s own client (same ground truth used
        throughout this method).
        """
        body: dict[str, Any] = {"prompt": prompt, "async": True}
        if url is not None:
            body["url"] = url
        if session_id is not None:
            body["session_id"] = session_id
        if max_steps is not None:
            body["max_steps"] = max_steps
        if timeout_ms is not None:
            body["timeout_ms"] = timeout_ms
        if output_schema is not None:
            body["output_schema"] = output_schema

        client = self.get_conn()
        http = self._raw_http()
        accepted = http.post("/ai/evaluate", json=body)
        workflow_id = accepted.get("workflow_id")
        if not workflow_id:
            return accepted
        return http.poll(
            f"/ai/jobs/{workflow_id}",
            is_terminal=lambda body: body.get("status") in {"completed", "failed", "timed_out"},
            poll_interval=client._poll_interval,  # noqa: SLF001 — see module note
            poll_max_interval=client._poll_max_interval,  # noqa: SLF001
            poll_timeout=poll_timeout if poll_timeout is not None else client._poll_timeout,  # noqa: SLF001
        )
