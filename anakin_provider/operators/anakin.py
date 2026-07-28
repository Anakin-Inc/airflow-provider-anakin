"""Operators for the Anakin provider.

Each operator is a thin wrapper: build request kwargs from constructor
params, call the matching :class:`~anakin_provider.hooks.anakin.AnakinHook`
method (which itself just calls the ``anakin-sdk`` client), and return the
result as a plain JSON-serialisable dict via Pydantic's
``model_dump(mode="json")`` so it can go straight to Airflow's default
(JSON) XCom backend without a custom serializer.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator

from anakin_provider.hooks.anakin import AnakinHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AnakinScrapeOperator(BaseOperator):
    """Scrape a single URL via Anakin.

    Submits ``POST /url-scraper`` and polls ``GET /url-scraper/:jobId``
    until the job reaches a terminal state (handled inside ``anakin-sdk``,
    not reimplemented here). Pushes the scraped
    :class:`anakin.models.Document` to XCom as a dict.

    :param url: The URL to scrape. Templated.
    :param country: Proxy country code, e.g. ``"us"``, ``"gb"``, ``"de"``.
    :param use_browser: Use a headless browser — needed for JS-heavy sites.
    :param generate_json: AI-extract structured JSON from the page content.
    :param force_fresh: Bypass the cache and force a fresh extraction.
    :param formats: Content formats to request, e.g.
        ``("markdown", "html", "screenshot")``. Defaults to ``("markdown",)``.
    :param session_id: Reuse an existing authenticated browser session.
    :param session_name: Name for a new session when ``session_id`` is unset.
    :param poll_timeout: Seconds to wait for the job to finish; falls back
        to the SDK client's own default (300s) when unset.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("url", "session_id", "session_name")
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        url: str,
        country: str = "us",
        use_browser: bool = False,
        generate_json: bool = False,
        force_fresh: bool = False,
        formats: Sequence[str] = ("markdown",),
        session_id: str | None = None,
        session_name: str | None = None,
        poll_timeout: float | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.country = country
        self.use_browser = use_browser
        self.generate_json = generate_json
        self.force_fresh = force_fresh
        self.formats = list(formats)
        self.session_id = session_id
        self.session_name = session_name
        self.poll_timeout = poll_timeout
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        document = hook.scrape(
            self.url,
            country=self.country,
            use_browser=self.use_browser,
            generate_json=self.generate_json,
            force_fresh=self.force_fresh,
            formats=self.formats,
            session_id=self.session_id,
            session_name=self.session_name,
            poll_timeout=self.poll_timeout,
        )
        return document.model_dump(mode="json")


class AnakinSearchOperator(BaseOperator):
    """AI-powered web search via Anakin — synchronous, no polling.

    Submits ``POST /search`` and returns immediately. Costs 3 credits per
    call. Pushes the :class:`anakin.models.SearchResult` to XCom as a dict.

    :param prompt: The search query or question. Templated.
    :param limit: Maximum number of results to return (default 5).
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("prompt",)
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        prompt: str,
        limit: int = 5,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.prompt = prompt
        self.limit = limit
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        result = hook.search(self.prompt, limit=self.limit)
        return result.model_dump(mode="json")


class AnakinAgenticSearchOperator(BaseOperator):
    """Multi-stage AI research pipeline via Anakin: search, scrape citation
    sources, and extract structured data with a summary.

    Submits ``POST /agentic-search`` and polls ``GET /agentic-search/:jobId``
    until terminal. Slower than :class:`AnakinSearchOperator` (can take
    minutes) and costs 10 credits per call. Pushes the
    :class:`anakin.models.AgenticSearchResult` to XCom as a dict.

    :param prompt: The research question or topic (max 8KB). Templated.
    :param use_browser: Use a headless browser for citation scraping.
        Defaults to ``True`` — matches the ``anakin-sdk`` client default.
    :param schema: Optional JSON Schema (max 50KB) constraining the
        structured output. Leave unset to let the pipeline choose its own.
    :param poll_timeout: Seconds to wait for the job to finish; falls back
        to the SDK client's own default (300s) when unset.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("prompt",)
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        prompt: str,
        use_browser: bool = True,
        schema: dict[str, Any] | None = None,
        poll_timeout: float | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.prompt = prompt
        self.use_browser = use_browser
        self.schema = schema
        self.poll_timeout = poll_timeout
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        result = hook.agentic_search(
            self.prompt,
            use_browser=self.use_browser,
            schema=self.schema,
            poll_timeout=self.poll_timeout,
        )
        return result.model_dump(mode="json")
