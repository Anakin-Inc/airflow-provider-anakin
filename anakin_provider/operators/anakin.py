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


class AnakinMapOperator(BaseOperator):
    """Discover URLs on a website (sitemap + link traversal) via Anakin.

    Submits ``POST /map`` and polls ``GET /map/:jobId`` until terminal
    (handled inside ``anakin-sdk``). Pushes the
    :class:`anakin.models.MapResult` to XCom as a dict.

    :param url: The site to map. Templated.
    :param limit: Maximum number of URLs to return.
    :param depth: Maximum link-traversal depth.
    :param limit_per_level: Maximum URLs collected per traversal depth.
    :param include_subdomains: Include subdomains of ``url``.
    :param include_external_links: Include links to other domains.
    :param search: Optional keyword filter for discovered URLs. Templated.
    :param use_browser: Use a headless browser for traversal.
    :param session_id: Reuse an existing authenticated browser session.
    :param poll_timeout: Seconds to wait for the job to finish; falls back
        to the SDK client's own default (300s) when unset.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("url", "search", "session_id")
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        url: str,
        limit: int = 100,
        depth: int = 2,
        limit_per_level: int = 100,
        include_subdomains: bool = False,
        include_external_links: bool = False,
        search: str | None = None,
        use_browser: bool = False,
        session_id: str | None = None,
        poll_timeout: float | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.limit = limit
        self.depth = depth
        self.limit_per_level = limit_per_level
        self.include_subdomains = include_subdomains
        self.include_external_links = include_external_links
        self.search = search
        self.use_browser = use_browser
        self.session_id = session_id
        self.poll_timeout = poll_timeout
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        result = hook.map(
            self.url,
            limit=self.limit,
            depth=self.depth,
            limit_per_level=self.limit_per_level,
            include_subdomains=self.include_subdomains,
            include_external_links=self.include_external_links,
            search=self.search,
            use_browser=self.use_browser,
            session_id=self.session_id,
            poll_timeout=self.poll_timeout,
        )
        return result.model_dump(mode="json")


class AnakinCrawlOperator(BaseOperator):
    """Crawl a website and return the scraped pages, via Anakin.

    Submits ``POST /crawl`` and polls ``GET /crawl/:jobId`` until terminal
    (handled inside ``anakin-sdk``). Pushes the
    :class:`anakin.models.CrawlResult` to XCom as a dict.

    :param url: The site to crawl. Templated.
    :param max_pages: Maximum number of pages to crawl.
    :param depth: Maximum link-traversal depth.
    :param include_patterns: Glob patterns restricting which URLs are crawled.
    :param exclude_patterns: Glob patterns to skip.
    :param country: Proxy country code, e.g. ``"us"``, ``"gb"``, ``"de"``.
    :param use_browser: Use a headless browser — needed for JS-heavy sites.
    :param session_id: Reuse an existing authenticated browser session.
    :param poll_timeout: Seconds to wait for the job to finish; falls back
        to the SDK client's own default (300s) when unset.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("url", "session_id")
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        url: str,
        max_pages: int = 10,
        depth: int = 1,
        include_patterns: Sequence[str] = (),
        exclude_patterns: Sequence[str] = (),
        country: str = "us",
        use_browser: bool = False,
        session_id: str | None = None,
        poll_timeout: float | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.max_pages = max_pages
        self.depth = depth
        self.include_patterns = list(include_patterns)
        self.exclude_patterns = list(exclude_patterns)
        self.country = country
        self.use_browser = use_browser
        self.session_id = session_id
        self.poll_timeout = poll_timeout
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        result = hook.crawl(
            self.url,
            max_pages=self.max_pages,
            depth=self.depth,
            include_patterns=self.include_patterns,
            exclude_patterns=self.exclude_patterns,
            country=self.country,
            use_browser=self.use_browser,
            session_id=self.session_id,
            poll_timeout=self.poll_timeout,
        )
        return result.model_dump(mode="json")


class AnakinWireDiscoverOperator(BaseOperator):
    """Find Wire actions for a task from a natural-language intent.

    Wire is Anakin's catalog of pre-built automation actions across
    hundreds of sites (Amazon, Walmart, LinkedIn, Airbnb, Zillow, ...).
    Submits ``GET /wire/resolve``. Returns ranked candidate actions, each
    with its ``action_id``, ``type`` (``"read"``/``"write"``), param
    schema, credit cost, and whether auth is needed — run the chosen action
    with :class:`AnakinWireReadActionOperator` or
    :class:`AnakinWireWriteActionOperator`.

    :param q: The intent in natural language, e.g. ``"top phones on
        walmart"``. Templated.
    :param limit: Maximum number of candidate actions to return.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("q",)
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        q: str,
        limit: int | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.q = q
        self.limit = limit
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.wire_discover(self.q, limit=self.limit)


class AnakinWireCatalogOperator(BaseOperator):
    """Browse the Wire catalog.

    Submits ``GET /wire/catalog`` (all catalogs and their action counts) or
    ``GET /wire/catalog/:slug`` (one site's full action list with exact
    parameter schemas, auth mode, and credit cost) when ``slug`` is set.

    :param slug: Catalog slug to inspect, e.g. ``"walmart"``. Omit to list
        every catalog. Templated.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("slug",)
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        slug: str | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.slug = slug
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.wire_catalog(self.slug)


class AnakinWireReadActionOperator(BaseOperator):
    """Run a Wire READ action — one whose ``type`` is ``"read"``: it
    extracts data and does not change state on the target site (search
    listings, fetch a category's products, read a profile, ...).

    Submits ``POST /wire/task`` and polls ``GET /wire/jobs/:jobId`` until
    terminal (handled inside ``anakin-sdk``'s ``Anakin.wire()``, which this
    operator delegates to — see :meth:`AnakinHook.wire_read_action`).
    Discover ``action_id`` first with :class:`AnakinWireDiscoverOperator` or
    :class:`AnakinWireCatalogOperator` and confirm the action's type is
    ``"read"``. Pushes the :class:`anakin.models.WireResult` to XCom.

    .. note::
       ``anakin-sdk`` v0.1.0 does not yet expose ``credential_id``/
       ``identity_id`` on ``Anakin.wire()``, so this operator currently
       only supports actions whose ``auth_mode`` is ``"none"``. See
       SUBMIT.md for detail.

    :param action_id: The action to run (from Wire discovery/catalog). Templated.
    :param params: The action's input parameters (shape depends on the
        action — use its schema from discovery). Templated.
    :param poll_timeout: Seconds to wait for the job to finish; falls back
        to the SDK client's own default (300s) when unset.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("action_id", "params")
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        action_id: str,
        params: dict[str, Any] | None = None,
        poll_timeout: float | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.action_id = action_id
        self.params = params
        self.poll_timeout = poll_timeout
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        result = hook.wire_read_action(
            self.action_id, self.params, poll_timeout=self.poll_timeout
        )
        return result.model_dump(mode="json")


class AnakinWireWriteActionOperator(BaseOperator):
    """Run a Wire WRITE action — one whose ``type`` is ``"write"``: it
    performs a state-changing interaction on the target site (submit a
    form, add to a cart, post content, ...).

    Submits ``POST /wire/task`` and polls ``GET /wire/jobs/:jobId`` until
    terminal (handled inside ``anakin-sdk``'s ``Anakin.wire()``, which this
    operator delegates to — see :meth:`AnakinHook.wire_write_action`).
    Discover ``action_id`` first with :class:`AnakinWireDiscoverOperator` or
    :class:`AnakinWireCatalogOperator` and confirm the action's type is
    ``"write"``. Pushes the :class:`anakin.models.WireResult` to XCom.

    Unlike the read-action operator, this can change state on the target
    site — treat as a destructive operation when reasoning about retries
    and idempotency in your DAG. It does not execute payments or transfer
    funds (rejected by the Anakin API itself).

    .. note::
       Same ``credential_id``/``identity_id`` gap as
       :class:`AnakinWireReadActionOperator` — see its docstring.

    :param action_id: The action to run (from Wire discovery/catalog). Templated.
    :param params: The action's input parameters. Templated.
    :param poll_timeout: Seconds to wait for the job to finish; falls back
        to the SDK client's own default (300s) when unset.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("action_id", "params")
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        action_id: str,
        params: dict[str, Any] | None = None,
        poll_timeout: float | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.action_id = action_id
        self.params = params
        self.poll_timeout = poll_timeout
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        result = hook.wire_write_action(
            self.action_id, self.params, poll_timeout=self.poll_timeout
        )
        return result.model_dump(mode="json")


class AnakinWireIdentitiesOperator(BaseOperator):
    """List your saved Wire identities and their credentials.

    Submits ``GET /wire/identities``. Each credential's ``id`` is the
    ``credential_id`` a future SDK release will accept on
    :class:`AnakinWireReadActionOperator`/:class:`AnakinWireWriteActionOperator`
    for auth-required actions.

    :param catalog_id: Optional — restrict to identities for one catalog.
        Templated.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("catalog_id",)
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        catalog_id: str | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.catalog_id = catalog_id
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.wire_identities(self.catalog_id)


class AnakinWireLoginOperator(BaseOperator):
    """Sign in to a credentials-mode Wire site and get a ``credential_id``.

    Submits ``POST /wire/login``. The password is never stored by Anakin,
    only the resulting encrypted session.

    .. warning::
       ``params`` typically carries login credentials (e.g. a password).
       Airflow renders templated fields and task logs in the UI and stores
       them in the metadata DB — pass secrets via an Airflow Connection/
       Variable backed by a Secrets Backend (not a literal DAG-file string)
       so they aren't written to the metadata DB or task logs in plaintext.

    :param catalog_slug: The catalog to sign in to, e.g. ``"walmart"``. Templated.
    :param params: Login fields defined by the catalog's login schema (see
        :class:`AnakinWireCatalogOperator`), e.g. ``{"email": ..., "password": ...}``.
    :param identity_name: Optional name for the identity.
    :param source_id: Optional 1Password identity-source ID (alternative to ``params``).
    :param source_ref: Optional 1Password item locator (used with ``source_id``).
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("catalog_slug", "identity_name")
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        catalog_slug: str,
        params: dict[str, Any] | None = None,
        identity_name: str | None = None,
        source_id: str | None = None,
        source_ref: dict[str, Any] | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.catalog_slug = catalog_slug
        self.params = params
        self.identity_name = identity_name
        self.source_id = source_id
        self.source_ref = source_ref
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.wire_login(
            self.catalog_slug,
            params=self.params,
            identity_name=self.identity_name,
            source_id=self.source_id,
            source_ref=self.source_ref,
        )


class AnakinWireBuildOperator(BaseOperator):
    """Request a brand-new Wire action for a website not yet in the catalog.

    Submits ``POST /wire/build-request``. Asynchronous — returns status
    ``"pending"``; Wire generates and auto-tests a scraper in the
    background, then publishes it. Charges credits, refunded automatically
    if the build fails. Only use after :class:`AnakinWireDiscoverOperator`/
    :class:`AnakinWireCatalogOperator` confirm no existing action covers
    the site.

    :param website_url: The site to build an action for. Templated.
    :param goal: Natural-language description of what the action should do
        or extract — be specific, the builder synthesizes the scraper from
        this. Templated.
    :param catalog_id: Optional — attach to an existing catalog instead of
        creating one.
    :param visibility: ``"private"`` (default) or ``"public"``.
    :param force: Build even if similar actions already exist for the
        domain (otherwise rejected with ``ACTION_EXISTS``).
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("website_url", "goal")
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        website_url: str,
        goal: str,
        catalog_id: str | None = None,
        visibility: str | None = None,
        force: bool | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.website_url = website_url
        self.goal = goal
        self.catalog_id = catalog_id
        self.visibility = visibility
        self.force = force
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.wire_build(
            self.website_url,
            self.goal,
            catalog_id=self.catalog_id,
            visibility=self.visibility,
            force=self.force,
        )


class AnakinMonitorCreateOperator(BaseOperator):
    """Create a scheduled website monitor via Anakin.

    Submits ``POST /monitors``. Checks ``url`` every ``interval_minutes``
    (minimum 15) and records a change whenever the content differs —
    optionally alerting a webhook/email. ``scope="page"`` (default) watches
    one URL; ``"site"`` crawls the site each run; ``"wire"`` runs a Wire
    action each check and diffs its JSON.

    :param url: The URL to watch (root URL for ``site`` scope; the Wire
        site's URL for ``wire`` scope). Templated.
    :param interval_minutes: Check frequency in minutes. Minimum 15.
    :param scope: ``"page"`` (default), ``"site"``, or ``"wire"``.
    :param watch_mode: ``"full_page"`` (default, 2 credits/check) or
        ``"specific_data"`` (3 credits/check, requires ``output_schema``).
    :param watch_format: Format compared in ``full_page`` mode — one of
        ``"markdown"`` (default), ``"html"``, ``"cleaned_html"``.
    :param output_schema: JSON Schema of the fields to track; required when
        ``watch_mode="specific_data"``.
    :param ai_mode: AI meaningful-change filtering (+1 credit/check).
    :param ai_goal: Natural-language description of which changes count as
        meaningful (used with ``ai_mode``).
    :param use_browser: Render checks with a headless browser.
    :param country: Two-letter proxy country code. Defaults to ``"us"``.
    :param session_id: Saved browser-session ID for login-protected pages.
    :param is_active: Start running immediately. Defaults to ``True``.
    :param expires_at: Optional end date (ISO 8601 or ``YYYY-MM-DD``); the
        monitor auto-pauses when it passes.
    :param alert_webhook_url: Webhook URL that receives signed change alerts.
    :param alert_emails: Comma-separated email recipients for change alerts.
    :param max_pages: Site scope: max pages crawled per run.
    :param max_depth: Site scope: crawl depth (1-5).
    :param include_patterns: Site scope: glob patterns/URLs to track.
    :param exclude_patterns: Site scope: glob patterns to skip.
    :param wire_action_id: Wire scope (required there): the action run each check.
    :param wire_catalog_slug: Wire scope: catalog slug of the Wire site.
    :param wire_credential_id: Wire scope: credential ID when the action needs auth.
    :param wire_params: Wire scope: parameters passed to the action each check.
    :param wire_watch_paths: Wire scope: JSON paths to diff instead of the whole response.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("url", "alert_emails")
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        url: str,
        interval_minutes: int,
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
        include_patterns: Sequence[str] | None = None,
        exclude_patterns: Sequence[str] | None = None,
        wire_action_id: str | None = None,
        wire_catalog_slug: str | None = None,
        wire_credential_id: str | None = None,
        wire_params: dict[str, Any] | None = None,
        wire_watch_paths: Sequence[str] | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.interval_minutes = interval_minutes
        self.scope = scope
        self.watch_mode = watch_mode
        self.watch_format = watch_format
        self.output_schema = output_schema
        self.ai_mode = ai_mode
        self.ai_goal = ai_goal
        self.use_browser = use_browser
        self.country = country
        self.session_id = session_id
        self.is_active = is_active
        self.expires_at = expires_at
        self.alert_webhook_url = alert_webhook_url
        self.alert_emails = alert_emails
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.include_patterns = include_patterns
        self.exclude_patterns = exclude_patterns
        self.wire_action_id = wire_action_id
        self.wire_catalog_slug = wire_catalog_slug
        self.wire_credential_id = wire_credential_id
        self.wire_params = wire_params
        self.wire_watch_paths = wire_watch_paths
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.monitor_create(
            self.url,
            self.interval_minutes,
            scope=self.scope,
            watch_mode=self.watch_mode,
            watch_format=self.watch_format,
            output_schema=self.output_schema,
            ai_mode=self.ai_mode,
            ai_goal=self.ai_goal,
            use_browser=self.use_browser,
            country=self.country,
            session_id=self.session_id,
            is_active=self.is_active,
            expires_at=self.expires_at,
            alert_webhook_url=self.alert_webhook_url,
            alert_emails=self.alert_emails,
            max_pages=self.max_pages,
            max_depth=self.max_depth,
            include_patterns=self.include_patterns,
            exclude_patterns=self.exclude_patterns,
            wire_action_id=self.wire_action_id,
            wire_catalog_slug=self.wire_catalog_slug,
            wire_credential_id=self.wire_credential_id,
            wire_params=self.wire_params,
            wire_watch_paths=self.wire_watch_paths,
        )


class AnakinMonitorListOperator(BaseOperator):
    """List your website monitors via Anakin.

    Submits ``GET /monitors``. Use to find a monitor's ``id`` before
    :class:`AnakinMonitorChangesOperator`/:class:`AnakinMonitorControlOperator`.

    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.monitor_list()


class AnakinMonitorChangesOperator(BaseOperator):
    """Get the detected changes for a website monitor via Anakin.

    Submits ``GET /monitors/:id/changes``. Each entry records when the
    watched content differed from the previous check, with a diff/summary
    (and the AI change summary when the monitor's ``ai_mode`` is on).

    :param monitor_id: The monitor ID (from :class:`AnakinMonitorListOperator`
        or :class:`AnakinMonitorCreateOperator`). Templated.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("monitor_id",)
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        monitor_id: str,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.monitor_id = monitor_id
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.monitor_changes(self.monitor_id)


class AnakinMonitorControlOperator(BaseOperator):
    """Pause, resume, run-now, or delete a website monitor via Anakin.

    Submits ``POST /monitors/:id/{pause,resume,run}`` or
    ``DELETE /monitors/:id`` depending on ``action``. ``"delete"``
    permanently removes the monitor and its history.

    :param monitor_id: The monitor ID (from :class:`AnakinMonitorListOperator`
        or :class:`AnakinMonitorCreateOperator`). Templated.
    :param action: One of ``"pause"``, ``"resume"``, ``"run_now"``, ``"delete"``.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("monitor_id", "action")
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        monitor_id: str,
        action: str,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.monitor_id = monitor_id
        self.action = action
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.monitor_control(self.monitor_id, self.action)


class AnakinAiVisibilitySearchOperator(BaseOperator):
    """Ask multiple AI answer engines the same question and compare answers.

    Submits ``POST /ai-visibility/search`` and polls
    ``GET /ai-visibility/search/:id`` until the run leaves ``"running"``.
    Returns one result per engine (status, answer summary, latency,
    consensus/outlier verdict) plus an AI-generated synthesis. A
    ``"failed"`` run is still returned (not raised) — it carries whichever
    per-source results/errors did come back.

    :param query: The question to ask every engine (max 2000 characters). Templated.
    :param sources: Engine slugs to query (see
        :class:`AnakinAiVisibilitySourcesOperator`). Omit to query all
        enabled engines.
    :param country: Two-letter ISO country for search geography. Defaults to ``"us"``.
    :param include_full_content: Include each engine's raw full answer in
        the XCom payload. Defaults to ``False`` (summaries/synthesis are
        returned regardless) — full answers can be large and bloat XCom.
    :param poll_timeout: Seconds to wait for the search to finish; falls
        back to the SDK client's own default (300s) when unset.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("query",)
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        query: str,
        sources: Sequence[str] | None = None,
        country: str | None = None,
        include_full_content: bool = False,
        poll_timeout: float | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.sources = sources
        self.country = country
        self.include_full_content = include_full_content
        self.poll_timeout = poll_timeout
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        result = hook.ai_visibility_search(
            self.query,
            sources=self.sources,
            country=self.country,
            poll_timeout=self.poll_timeout,
        )
        if not self.include_full_content:
            results = result.get("results")
            if isinstance(results, list):
                result = {
                    **result,
                    "results": [
                        {k: v for k, v in r.items() if k != "full_content"}
                        if isinstance(r, dict)
                        else r
                        for r in results
                    ],
                }
        return result


class AnakinAiVisibilitySourcesOperator(BaseOperator):
    """List the AI answer engines available to :class:`AnakinAiVisibilitySearchOperator`.

    Submits ``GET /ai-visibility/sources`` — each engine's slug (what you
    pass as ``sources``) and display label.

    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.ai_visibility_sources()


class AnakinSessionListOperator(BaseOperator):
    """List saved browser sessions via Anakin.

    Delegates to ``anakin-sdk``'s ``Anakin.sessions.list()``. Each
    session's ``id`` is what you pass as ``session_id`` to
    :class:`AnakinScrapeOperator`/:class:`AnakinCrawlOperator`,
    :class:`AnakinMonitorCreateOperator`, or
    :class:`AnakinBrowserTaskOperator` to work with login-protected pages.
    Sessions are created interactively via the Anakin dashboard (not
    exposed here — it requires a live noVNC browser for the user to log
    into, including 2FA/captchas).

    :param domain: Filter to sessions for one website domain, e.g.
        ``"amazon.com"``. Templated.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("domain",)
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        domain: str | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.domain = domain
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> list[dict[str, Any]]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        sessions = hook.session_list(domain=self.domain)
        return [s.model_dump(mode="json") for s in sessions]


class AnakinSessionDeleteOperator(BaseOperator):
    """Permanently delete a saved browser session via Anakin.

    Delegates to ``anakin-sdk``'s ``Anakin.sessions.delete()``.
    Irreversible — any monitor or task referencing this ``session_id``
    loses authenticated access; the user must log in again through the
    dashboard to recreate it.

    :param session_id: The session ID to delete (from
        :class:`AnakinSessionListOperator`). Templated.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("session_id",)
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        session_id: str,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.session_id = session_id
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        hook.session_delete(self.session_id)
        # The SDK's `sessions.delete()` returns nothing on success (204);
        # synthesize a small, useful XCom payload rather than pushing None.
        return {"session_id": self.session_id, "deleted": True}


class AnakinBrowserTaskOperator(BaseOperator):
    """Run a natural-language task in a real cloud browser driven by an AI agent.

    Submits ``POST /ai/evaluate`` (async) and polls ``GET /ai/jobs/:id``
    until terminal (up to ~6 minutes of polling; the server hard-caps a run
    at ~5.5 minutes). Use when :class:`AnakinScrapeOperator` can't do the
    job (multi-step flows, interactions, complex navigation) and no Wire
    action covers the site (check :class:`AnakinWireDiscoverOperator`
    first — Wire actions are faster and cheaper). It does not execute
    payments or transfer funds (rejected by the Anakin API itself).

    .. warning::
       Never put passwords/secrets in ``prompt`` — it is templated and
       lands in Airflow's task logs and metadata DB. For login-protected
       tasks, pass ``session_id`` from :class:`AnakinSessionListOperator` instead.

    :param prompt: The task in natural language. Be specific about the
        goal and what to return. Templated.
    :param url: Navigate here before starting. Omit to let the agent follow
        URLs named in the prompt.
    :param session_id: Saved browser-session ID so the task runs logged in.
    :param max_steps: Cap on agent steps (navigation/click/type actions).
    :param timeout_ms: Task timeout in milliseconds (server caps runs at
        ~330s regardless).
    :param output_schema: JSON Schema for the result — the agent returns
        structured data conforming to it.
    :param poll_timeout: Seconds to wait for the job to finish; falls back
        to the SDK client's own default (300s) when unset. Note the
        underlying job itself may run up to ~5.5 minutes server-side.
    :param anakin_conn_id: Airflow connection id for :class:`AnakinHook`.
    """

    template_fields: Sequence[str] = ("prompt", "url", "session_id")
    ui_color = "#6b4fbb"

    def __init__(
        self,
        *,
        prompt: str,
        url: str | None = None,
        session_id: str | None = None,
        max_steps: int | None = None,
        timeout_ms: int | None = None,
        output_schema: dict[str, Any] | None = None,
        poll_timeout: float | None = None,
        anakin_conn_id: str = AnakinHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.prompt = prompt
        self.url = url
        self.session_id = session_id
        self.max_steps = max_steps
        self.timeout_ms = timeout_ms
        self.output_schema = output_schema
        self.poll_timeout = poll_timeout
        self.anakin_conn_id = anakin_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = AnakinHook(anakin_conn_id=self.anakin_conn_id)
        return hook.browser_task(
            self.prompt,
            url=self.url,
            session_id=self.session_id,
            max_steps=self.max_steps,
            timeout_ms=self.timeout_ms,
            output_schema=self.output_schema,
            poll_timeout=self.poll_timeout,
        )
