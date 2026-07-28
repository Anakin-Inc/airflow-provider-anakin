"""Unit tests for the Anakin operators.

Mocks `AnakinHook` entirely (patched where each operator module looks it
up) — these test the operator <-> hook wiring (constructor args passed
through, XCom-safe dict returned via model_dump), not the hook or SDK
themselves. Requires `apache-airflow` and `anakin-sdk` installed to *run*;
see SUBMIT.md.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from anakin.models import AgenticSearchResult, Document, SearchResult

from anakin_provider.operators.anakin import (
    AnakinAgenticSearchOperator,
    AnakinScrapeOperator,
    AnakinSearchOperator,
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
