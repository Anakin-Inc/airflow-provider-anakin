"""Unit tests for the Anakin operators.

Mocks `AnakinHook` entirely (patched where each operator module looks it
up) — these test the operator <-> hook wiring (constructor args passed
through, XCom-safe dict returned via model_dump), not the hook or SDK
themselves.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from anakin.models import (
    AgenticSearchResult,
    AIVisibilitySearch,
    AIVisibilitySource,
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
    WireCatalogDetail,
    WireIdentity,
    WireLoginResult,
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
    AnakinWireBuildStatusOperator,
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
    mock_hook_cls.return_value.search.assert_called_once_with("latest AI agents", limit=3)


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


def test_wire_discover_operator_returns_list_of_dicts() -> None:
    op = AnakinWireDiscoverOperator(
        task_id="wire_discover", q="top phones on walmart", auth_mode="none", limit=3
    )
    match = WireActionMatch(action_id="walmart.search", catalog_slug="walmart", credits=2)
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_discover.return_value = [match]
        result = op.execute(context={})

    assert result[0]["action_id"] == "walmart.search"
    json.dumps(result)
    mock_hook_cls.return_value.wire_discover.assert_called_once_with(
        "top phones on walmart", catalog=None, category=None, auth_mode="none", limit=3
    )


def test_wire_catalog_operator_returns_dict() -> None:
    op = AnakinWireCatalogOperator(task_id="wire_catalog", slug="walmart")
    detail = WireCatalogDetail.model_validate(
        {"catalog": {"id": "cat_1", "slug": "walmart", "name": "Walmart"}, "actions": []}
    )
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_catalog.return_value = detail
        result = op.execute(context={})

    assert result["actions"] == []
    mock_hook_cls.return_value.wire_catalog.assert_called_once_with("walmart")


@pytest.mark.parametrize(
    ("operator_cls", "hook_method"),
    [
        (AnakinWireReadActionOperator, "wire_read_action"),
        (AnakinWireWriteActionOperator, "wire_write_action"),
    ],
)
def test_wire_action_operators_pass_params_and_credentials(operator_cls, hook_method) -> None:
    op = operator_cls(
        task_id="wire",
        action_id="walmart.search_products",
        action_params={"q": "phones"},
        credential_id="cred_1",
    )
    wire_result = WireResult(job_id="job_wire", status="completed", data={"items": []})
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        getattr(mock_hook_cls.return_value, hook_method).return_value = wire_result
        result = op.execute(context={})

    assert result["job_id"] == "job_wire"
    assert result["data"] == {"items": []}
    getattr(mock_hook_cls.return_value, hook_method).assert_called_once_with(
        "walmart.search_products",
        {"q": "phones"},
        credential_id="cred_1",
        identity_id=None,
        poll_timeout=None,
    )


def test_wire_action_params_do_not_collide_with_airflow_params() -> None:
    """``params`` is reserved by Airflow (DAG/task Params, merged at run
    time); action input must live in its own attribute."""
    op = AnakinWireReadActionOperator(
        task_id="wire", action_id="a.b", action_params={"q": "x"}, params={"env": "prod"}
    )
    assert op.action_params == {"q": "x"}
    assert "q" not in op.params


def test_wire_identities_operator_returns_list_of_dicts() -> None:
    op = AnakinWireIdentitiesOperator(task_id="wire_identities", catalog_id="cat_1")
    identity = WireIdentity.model_validate({"id": "idn_1", "credentials": []})
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_identities.return_value = [identity]
        result = op.execute(context={})

    assert result[0]["id"] == "idn_1"
    mock_hook_cls.return_value.wire_identities.assert_called_once_with("cat_1")


def test_wire_login_operator_returns_dict() -> None:
    op = AnakinWireLoginOperator(
        task_id="wire_login", catalog_slug="neb", login_params={"email": "a@b.com", "password": "x"}
    )
    login = WireLoginResult(status="connected", identity_id="idn_1", credential_id="cred_1")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_login.return_value = login
        result = op.execute(context={})

    assert result["credential_id"] == "cred_1"
    mock_hook_cls.return_value.wire_login.assert_called_once_with(
        "neb",
        {"email": "a@b.com", "password": "x"},
        identity_name=None,
        source_id=None,
        source_ref=None,
    )


def test_wire_build_and_status_operators() -> None:
    build = WireBuildRequest(id="build_1", status="pending")
    op = AnakinWireBuildOperator(
        task_id="wire_build", website_url="https://example.com", goal="extract prices"
    )
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_build.return_value = build
        result = op.execute(context={})

    assert result["id"] == "build_1"
    mock_hook_cls.return_value.wire_build.assert_called_once_with(
        "https://example.com",
        "extract prices",
        actions=None,
        catalog_id=None,
        country=None,
        visibility=None,
        force=False,
    )

    status_op = AnakinWireBuildStatusOperator(task_id="wire_build_status", build_id="build_1")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.wire_build_status.return_value = WireBuildRequest(
            id="build_1", status="published", action_id="example.prices"
        )
        result = status_op.execute(context={})

    assert result["action_id"] == "example.prices"
    mock_hook_cls.return_value.wire_build_status.assert_called_once_with("build_1")


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
        mock_hook_cls.return_value.monitor_create.return_value = Monitor(
            id="mon_1", url="https://example.com"
        )
        result = op.execute(context={})

    assert result["id"] == "mon_1"
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


def test_monitor_list_and_changes_operators_return_lists() -> None:
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.monitor_list.return_value = [
            Monitor(id="mon_1", url="https://example.com")
        ]
        mock_hook_cls.return_value.monitor_changes.return_value = [MonitorChange(id="chg_1")]
        listed = AnakinMonitorListOperator(task_id="monitor_list").execute(context={})
        changes = AnakinMonitorChangesOperator(
            task_id="monitor_changes", monitor_id="mon_1"
        ).execute(context={})

    assert [m["id"] for m in listed] == ["mon_1"]
    assert [c["id"] for c in changes] == ["chg_1"]
    mock_hook_cls.return_value.monitor_changes.assert_called_once_with("mon_1")


def test_monitor_control_operator_passes_through_action() -> None:
    op = AnakinMonitorControlOperator(
        task_id="monitor_control", monitor_id="mon_1", action="run_now"
    )
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.monitor_control.return_value = MonitorRun(
            success=True, job_id="job_1"
        )
        result = op.execute(context={})

    assert result["job_id"] == "job_1"
    mock_hook_cls.return_value.monitor_control.assert_called_once_with("mon_1", "run_now")


def test_monitor_control_operator_delete_returns_confirmation() -> None:
    op = AnakinMonitorControlOperator(task_id="monitor_delete", monitor_id="mon_1", action="delete")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.monitor_control.return_value = None
        result = op.execute(context={})

    assert result == {"monitor_id": "mon_1", "deleted": True}


# ─── AI visibility ────────────────────────────────────────────────────────────


def _ai_search() -> AIVisibilitySearch:
    return AIVisibilitySearch.model_validate(
        {
            "search_id": "srch_1",
            "status": "completed",
            "results": [
                {
                    "source": "chatgpt",
                    "status": "completed",
                    "summary": "...",
                    "full_content": "very long",
                }
            ],
        }
    )


def test_ai_visibility_search_operator_strips_full_content_by_default() -> None:
    op = AnakinAiVisibilitySearchOperator(task_id="ai_vis_search", query="what is anakin?")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.ai_visibility_search.return_value = _ai_search()
        result = op.execute(context={})

    assert "full_content" not in result["results"][0]
    assert result["results"][0]["summary"] == "..."
    mock_hook_cls.return_value.ai_visibility_search.assert_called_once_with(
        "what is anakin?", sources=None, country="us", poll_timeout=None
    )


def test_ai_visibility_search_operator_keeps_full_content_when_requested() -> None:
    op = AnakinAiVisibilitySearchOperator(
        task_id="ai_vis_search_full", query="what is anakin?", include_full_content=True
    )
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.ai_visibility_search.return_value = _ai_search()
        result = op.execute(context={})

    assert result["results"][0]["full_content"] == "very long"


def test_ai_visibility_sources_operator_returns_list_of_dicts() -> None:
    op = AnakinAiVisibilitySourcesOperator(task_id="ai_vis_sources")
    with patch("anakin_provider.operators.anakin.AnakinHook") as mock_hook_cls:
        mock_hook_cls.return_value.ai_visibility_sources.return_value = [
            AIVisibilitySource.model_validate({"slug": "chatgpt", "label": "ChatGPT"})
        ]
        result = op.execute(context={})

    assert result[0]["slug"] == "chatgpt"


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
