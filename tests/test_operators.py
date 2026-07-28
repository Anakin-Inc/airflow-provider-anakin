"""Unit tests for the Anakin operators.

Mocks `AnakinHook` entirely (patched where each operator module looks it
up) — these test the operator <-> hook wiring (constructor args passed
through, XCom-safe dict returned via model_dump), not the hook or SDK
themselves. Requires `apache-airflow` and `anakin-sdk` installed to *run*;
see SUBMIT.md.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from anakin.models import (
    AgenticSearchResult,
    BrowserSession,
    CrawlResult,
    Document,
    MapResult,
    SearchResult,
    WireResult,
)

from anakin_provider.operators.anakin import (
    AnakinAgenticSearchOperator,
    AnakinAiVisibilitySearchOperator,
    AnakinAiVisibilitySourcesOperator,
    AnakinBrowserTaskOperator,
    AnakinCrawlOperator,
    AnakinMapOperator,
    AnakinMonitorChangesOperator,
    AnakinMonitorControlOperator,
    AnakinMonitorCreateOperator,
    AnakinMonitorListOperator,
    AnakinScrapeOperator,
    AnakinSearchOperator,
    AnakinSessionDeleteOperator,
    AnakinSessionListOperator,
    AnakinWireBuildOperator,
    AnakinWireCatalogOperator,
    AnakinWireDiscoverOperator,
    AnakinWireIdentitiesOperator,
    AnakinWireLoginOperator,
    AnakinWireReadActionOperator,
    AnakinWireWriteActionOperator,
)


def test_scrape_operator_execute_returns_json_safe_dict() -> None:
    op = AnakinScrapeOperator(task_id="scrape", url="https://example.com")
    document = Document(
        id="job_1",
        url="https://example.com",
        status="completed",
        cached=False,
        duration_ms=120,
        markdown="# Hello",
    )
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.scrape.return_value = document
        result = op.execute(context={})

    assert result["markdown"] == "# Hello"
    assert result["id"] == "job_1"
    mock_hook_cls.return_value.scrape.assert_called_once_with(
        "https://example.com",
        country="us",
        use_browser=False,
        generate_json=False,
        force_fresh=False,
        formats=["markdown"],
        session_id=None,
        session_name=None,
        poll_timeout=None,
    )


def test_search_operator_execute_returns_json_safe_dict() -> None:
    op = AnakinSearchOperator(task_id="search", prompt="latest AI agents", limit=3)
    search_result = SearchResult(id="job_2", results=[])
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.search.return_value = search_result
        result = op.execute(context={})

    assert result["id"] == "job_2"
    assert result["results"] == []
    mock_hook_cls.return_value.search.assert_called_once_with(
        "latest AI agents", limit=3
    )


def test_agentic_search_operator_execute_returns_json_safe_dict() -> None:
    op = AnakinAgenticSearchOperator(task_id="agentic_search", prompt="compare CRMs")
    agentic_result = AgenticSearchResult(id="job_3", status="completed")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.agentic_search.return_value = agentic_result
        result = op.execute(context={})

    assert result["id"] == "job_3"
    assert result["status"] == "completed"
    mock_hook_cls.return_value.agentic_search.assert_called_once_with(
        "compare CRMs", use_browser=True, schema=None, poll_timeout=None
    )


def test_scrape_operator_default_conn_id_matches_hook_default() -> None:
    from anakin_provider.hooks.anakin import AnakinHook

    op = AnakinScrapeOperator(task_id="scrape", url="https://example.com")
    assert op.anakin_conn_id == AnakinHook.default_conn_name


# ─── map / crawl ─────────────────────────────────────────────────────────────


def test_map_operator_execute_returns_json_safe_dict() -> None:
    op = AnakinMapOperator(task_id="map", url="https://example.com", depth=3)
    map_result = MapResult(id="job_map", url="https://example.com", links=["https://example.com/a"])
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.map.return_value = map_result
        result = op.execute(context={})

    assert result["id"] == "job_map"
    assert result["links"] == ["https://example.com/a"]
    mock_hook_cls.return_value.map.assert_called_once_with(
        "https://example.com",
        limit=100,
        depth=3,
        limit_per_level=100,
        include_subdomains=False,
        include_external_links=False,
        search=None,
        use_browser=False,
        session_id=None,
        poll_timeout=None,
    )


def test_crawl_operator_execute_returns_json_safe_dict() -> None:
    op = AnakinCrawlOperator(task_id="crawl", url="https://example.com", max_pages=5)
    crawl_result = CrawlResult(id="job_crawl", url="https://example.com")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.crawl.return_value = crawl_result
        result = op.execute(context={})

    assert result["id"] == "job_crawl"
    mock_hook_cls.return_value.crawl.assert_called_once_with(
        "https://example.com",
        max_pages=5,
        depth=1,
        include_patterns=[],
        exclude_patterns=[],
        country="us",
        use_browser=False,
        session_id=None,
        poll_timeout=None,
    )


# ─── wire ──────────────────────────────────────────────────────────────────


def test_wire_discover_operator_returns_raw_dict() -> None:
    op = AnakinWireDiscoverOperator(task_id="wire_discover", q="top phones on walmart", limit=3)
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_discover.return_value = {"results": []}
        result = op.execute(context={})

    assert result == {"results": []}
    mock_hook_cls.return_value.wire_discover.assert_called_once_with(
        "top phones on walmart", limit=3
    )


def test_wire_catalog_operator_returns_raw_dict() -> None:
    op = AnakinWireCatalogOperator(task_id="wire_catalog", slug="walmart")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_catalog.return_value = {"actions": []}
        result = op.execute(context={})

    assert result == {"actions": []}
    mock_hook_cls.return_value.wire_catalog.assert_called_once_with("walmart")


def test_wire_read_action_operator_returns_json_safe_dict() -> None:
    op = AnakinWireReadActionOperator(
        task_id="wire_read", action_id="walmart.search_products", params={"q": "phones"}
    )
    wire_result = WireResult(job_id="job_wire", status="completed", data={"items": []})
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_read_action.return_value = wire_result
        result = op.execute(context={})

    assert result["job_id"] == "job_wire"
    assert result["data"] == {"items": []}
    mock_hook_cls.return_value.wire_read_action.assert_called_once_with(
        "walmart.search_products", {"q": "phones"}, poll_timeout=None
    )


def test_wire_write_action_operator_returns_json_safe_dict() -> None:
    op = AnakinWireWriteActionOperator(
        task_id="wire_write", action_id="walmart.add_to_cart", params={"sku": "123"}
    )
    wire_result = WireResult(job_id="job_wire_2", status="completed")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_write_action.return_value = wire_result
        result = op.execute(context={})

    assert result["job_id"] == "job_wire_2"
    mock_hook_cls.return_value.wire_write_action.assert_called_once_with(
        "walmart.add_to_cart", {"sku": "123"}, poll_timeout=None
    )


def test_wire_identities_operator_returns_raw_dict() -> None:
    op = AnakinWireIdentitiesOperator(task_id="wire_identities", catalog_id="cat_1")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_identities.return_value = {"identities": []}
        result = op.execute(context={})

    assert result == {"identities": []}
    mock_hook_cls.return_value.wire_identities.assert_called_once_with("cat_1")


def test_wire_login_operator_returns_raw_dict() -> None:
    op = AnakinWireLoginOperator(
        task_id="wire_login", catalog_slug="neb", params={"email": "a@b.com", "password": "x"}
    )
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_login.return_value = {"credential_id": "cred_1"}
        result = op.execute(context={})

    assert result == {"credential_id": "cred_1"}
    mock_hook_cls.return_value.wire_login.assert_called_once_with(
        "neb",
        params={"email": "a@b.com", "password": "x"},
        identity_name=None,
        source_id=None,
        source_ref=None,
    )


def test_wire_build_operator_returns_raw_dict() -> None:
    op = AnakinWireBuildOperator(
        task_id="wire_build", website_url="https://example.com", goal="extract prices"
    )
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_build.return_value = {"status": "pending"}
        result = op.execute(context={})

    assert result == {"status": "pending"}
    mock_hook_cls.return_value.wire_build.assert_called_once_with(
        "https://example.com", "extract prices", catalog_id=None, visibility=None, force=None
    )


# ─── website monitoring ──────────────────────────────────────────────────────


def test_monitor_create_operator_passes_through_all_fields() -> None:
    op = AnakinMonitorCreateOperator(
        task_id="monitor_create",
        url="https://example.com",
        interval_minutes=60,
        scope="page",
        ai_mode=True,
    )
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.monitor_create.return_value = {"id": "mon_1"}
        result = op.execute(context={})

    assert result == {"id": "mon_1"}
    mock_hook_cls.return_value.monitor_create.assert_called_once_with(
        "https://example.com",
        60,
        scope="page",
        watch_mode=None,
        watch_format=None,
        output_schema=None,
        ai_mode=True,
        ai_goal=None,
        use_browser=None,
        country=None,
        session_id=None,
        is_active=None,
        expires_at=None,
        alert_webhook_url=None,
        alert_emails=None,
        max_pages=None,
        max_depth=None,
        include_patterns=None,
        exclude_patterns=None,
        wire_action_id=None,
        wire_catalog_slug=None,
        wire_credential_id=None,
        wire_params=None,
        wire_watch_paths=None,
    )


def test_monitor_list_operator_returns_raw_dict() -> None:
    op = AnakinMonitorListOperator(task_id="monitor_list")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.monitor_list.return_value = {"monitors": []}
        result = op.execute(context={})

    assert result == {"monitors": []}
    mock_hook_cls.return_value.monitor_list.assert_called_once_with()


def test_monitor_changes_operator_returns_raw_dict() -> None:
    op = AnakinMonitorChangesOperator(task_id="monitor_changes", monitor_id="mon_1")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.monitor_changes.return_value = {"changes": []}
        result = op.execute(context={})

    assert result == {"changes": []}
    mock_hook_cls.return_value.monitor_changes.assert_called_once_with("mon_1")


def test_monitor_control_operator_passes_through_action() -> None:
    op = AnakinMonitorControlOperator(task_id="monitor_control", monitor_id="mon_1", action="pause")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.monitor_control.return_value = {"status": "paused"}
        result = op.execute(context={})

    assert result == {"status": "paused"}
    mock_hook_cls.return_value.monitor_control.assert_called_once_with("mon_1", "pause")


# ─── AI visibility ────────────────────────────────────────────────────────────


def test_ai_visibility_search_operator_strips_full_content_by_default() -> None:
    op = AnakinAiVisibilitySearchOperator(task_id="ai_vis_search", query="what is anakin?")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.ai_visibility_search.return_value = {
            "search_id": "srch_1",
            "status": "completed",
            "results": [{"source": "chatgpt", "summary": "...", "full_content": "very long text"}],
        }
        result = op.execute(context={})

    assert result["results"] == [{"source": "chatgpt", "summary": "..."}]
    mock_hook_cls.return_value.ai_visibility_search.assert_called_once_with(
        "what is anakin?", sources=None, country=None, poll_timeout=None
    )


def test_ai_visibility_search_operator_keeps_full_content_when_requested() -> None:
    op = AnakinAiVisibilitySearchOperator(
        task_id="ai_vis_search_full", query="what is anakin?", include_full_content=True
    )
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.ai_visibility_search.return_value = {
            "results": [{"source": "chatgpt", "full_content": "very long text"}],
        }
        result = op.execute(context={})

    assert result["results"] == [{"source": "chatgpt", "full_content": "very long text"}]


def test_ai_visibility_sources_operator_returns_raw_dict() -> None:
    op = AnakinAiVisibilitySourcesOperator(task_id="ai_vis_sources")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.ai_visibility_sources.return_value = {"sources": []}
        result = op.execute(context={})

    assert result == {"sources": []}


# ─── browser sessions ─────────────────────────────────────────────────────────


def test_session_list_operator_returns_json_safe_list() -> None:
    op = AnakinSessionListOperator(task_id="session_list", domain="amazon.com")
    session = BrowserSession(sessionId="sess_1", name="my session")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.session_list.return_value = [session]
        result = op.execute(context={})

    assert len(result) == 1
    assert result[0]["id"] == "sess_1"
    assert result[0]["name"] == "my session"
    mock_hook_cls.return_value.session_list.assert_called_once_with(domain="amazon.com")


def test_session_delete_operator_synthesizes_confirmation_dict() -> None:
    op = AnakinSessionDeleteOperator(task_id="session_delete", session_id="sess_1")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.session_delete.return_value = None
        result = op.execute(context={})

    assert result == {"session_id": "sess_1", "deleted": True}
    mock_hook_cls.return_value.session_delete.assert_called_once_with("sess_1")


# ─── browser task ─────────────────────────────────────────────────────────────


def test_browser_task_operator_returns_raw_dict() -> None:
    op = AnakinBrowserTaskOperator(
        task_id="browser_task", prompt="find the cheapest TV", url="https://example.com"
    )
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.browser_task.return_value = {"success": True, "result": {}}
        result = op.execute(context={})

    assert result == {"success": True, "result": {}}
    mock_hook_cls.return_value.browser_task.assert_called_once_with(
        "find the cheapest TV",
        url="https://example.com",
        session_id=None,
        max_steps=None,
        timeout_ms=None,
        output_schema=None,
        poll_timeout=None,
    )
