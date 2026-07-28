"""Unit tests for AnakinHook.

Mocks `BaseHook.get_connection` (the Airflow metadata-DB lookup) and the
`anakin.Anakin` client construction — never makes a network call and never
needs a real Airflow metadata database. Requires `apache-airflow` and
`anakin-sdk` installed to *run*; see SUBMIT.md for why that wasn't
exercised in this environment.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowNotFoundException

from anakin_provider.hooks.anakin import DEFAULT_ANAKIN_BASE_URL, AnakinHook


def _fake_connection(*, password: str | None, extra: dict | None = None) -> MagicMock:
    conn = MagicMock()
    conn.password = password
    conn.extra_dejson = extra or {}
    return conn


@pytest.fixture(autouse=True)
def _no_ambient_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANAKIN_API_KEY", raising=False)


def test_default_conn_name_and_conn_type() -> None:
    assert AnakinHook.default_conn_name == "anakin_default"
    assert AnakinHook.conn_type == "anakin"
    assert AnakinHook.conn_name_attr == "anakin_conn_id"


def test_get_conn_uses_connection_password_as_api_key() -> None:
    hook = AnakinHook(anakin_conn_id="anakin_test")
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            hook.get_conn()
            mock_anakin_cls.assert_called_once_with(
                api_key="ask-secret", base_url=DEFAULT_ANAKIN_BASE_URL
            )


def test_get_conn_honours_base_url_override_from_extra() -> None:
    hook = AnakinHook()
    with patch.object(
        hook,
        "get_connection",
        return_value=_fake_connection(
            password="ask-secret", extra={"base_url": "https://staging.anakin.io/v1"}
        ),
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            hook.get_conn()
            mock_anakin_cls.assert_called_once_with(
                api_key="ask-secret", base_url="https://staging.anakin.io/v1"
            )


def test_get_conn_falls_back_to_env_var_when_connection_missing() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", side_effect=AirflowNotFoundException("no such conn")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            hook.get_conn()
            # api_key=None tells anakin.Anakin() to fall back to ANAKIN_API_KEY itself.
            mock_anakin_cls.assert_called_once_with(
                api_key=None, base_url=DEFAULT_ANAKIN_BASE_URL
            )


def test_get_conn_caches_the_client() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            client1 = hook.get_conn()
            client2 = hook.get_conn()
            assert client1 is client2
            mock_anakin_cls.assert_called_once()


def test_test_connection_fails_with_no_key_anywhere() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", side_effect=AirflowNotFoundException("no such conn")
    ):
        ok, message = hook.test_connection()
    assert ok is False
    assert "ANAKIN_API_KEY" in message


def test_test_connection_succeeds_without_a_network_call() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            ok, message = hook.test_connection()
            assert ok is True
            assert "no network call" in message.lower()
            # test_connection must not touch billed endpoints.
            mock_anakin_cls.return_value.search.assert_not_called()
            mock_anakin_cls.return_value.agentic_search.assert_not_called()


def test_scrape_search_agentic_search_delegate_to_client() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            client = mock_anakin_cls.return_value

            hook.scrape("https://example.com", country="gb")
            client.scrape.assert_called_once_with("https://example.com", country="gb")

            hook.search("latest AI agents", limit=3)
            client.search.assert_called_once_with("latest AI agents", limit=3)

            hook.agentic_search("compare CRMs", use_browser=False)
            client.agentic_search.assert_called_once_with("compare CRMs", use_browser=False)


# ─── map / crawl / wire (SDK-backed passthrough) ────────────────────────────


def test_map_and_crawl_delegate_to_client() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            client = mock_anakin_cls.return_value

            hook.map("https://example.com", depth=3)
            client.map.assert_called_once_with("https://example.com", depth=3)

            hook.crawl("https://example.com", max_pages=5)
            client.crawl.assert_called_once_with("https://example.com", max_pages=5)


def test_wire_read_and_write_action_both_delegate_to_client_wire() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            client = mock_anakin_cls.return_value

            hook.wire_read_action("walmart.search_products", {"q": "phones"}, poll_timeout=30)
            client.wire.assert_called_once_with(
                "walmart.search_products", {"q": "phones"}, poll_timeout=30
            )

            client.wire.reset_mock()
            hook.wire_write_action("walmart.add_to_cart", {"sku": "123"})
            client.wire.assert_called_once_with("walmart.add_to_cart", {"sku": "123"}, poll_timeout=None)


def test_wire_action_defaults_params_to_empty_dict() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            client = mock_anakin_cls.return_value
            hook.wire_read_action("some.action_with_no_params")
            client.wire.assert_called_once_with("some.action_with_no_params", {}, poll_timeout=None)


def test_session_list_and_delete_delegate_to_client_sessions() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            client = mock_anakin_cls.return_value

            hook.session_list(domain="amazon.com")
            client.sessions.list.assert_called_once_with(domain="amazon.com")

            hook.session_delete("sess_123")
            client.sessions.delete.assert_called_once_with("sess_123")


# ─── endpoints reached via the SDK's internal HttpClient (no public SDK method) ──


def test_wire_discover_gets_wire_resolve_with_query_params() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            http = mock_anakin_cls.return_value._http
            hook.wire_discover("top phones on walmart", limit=5)
            http.get.assert_called_once_with(
                "/wire/resolve", params={"q": "top phones on walmart", "limit": 5}
            )


def test_wire_catalog_with_and_without_slug() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            http = mock_anakin_cls.return_value._http

            hook.wire_catalog("walmart")
            http.get.assert_called_once_with("/wire/catalog/walmart")

            http.get.reset_mock()
            hook.wire_catalog()
            http.get.assert_called_once_with("/wire/catalog")


def test_wire_login_builds_expected_body() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            http = mock_anakin_cls.return_value._http
            hook.wire_login("neb", params={"email": "a@b.com", "password": "x"})
            http.post.assert_called_once_with(
                "/wire/login",
                json={
                    "catalog_slug": "neb",
                    "params": {"email": "a@b.com", "password": "x"},
                },
            )


def test_wire_build_builds_expected_body() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            http = mock_anakin_cls.return_value._http
            hook.wire_build("https://example.com", "extract product prices", force=True)
            http.post.assert_called_once_with(
                "/wire/build-request",
                json={
                    "website_url": "https://example.com",
                    "goal": "extract product prices",
                    "force": True,
                },
            )


def test_monitor_create_only_sends_set_fields() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            http = mock_anakin_cls.return_value._http
            hook.monitor_create("https://example.com", 60, scope="page", ai_mode=True)
            http.post.assert_called_once_with(
                "/monitors",
                json={
                    "url": "https://example.com",
                    "intervalMinutes": 60,
                    "scope": "page",
                    "aiMode": True,
                },
            )


def test_monitor_control_maps_actions_to_paths() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            http = mock_anakin_cls.return_value._http

            hook.monitor_control("mon_1", "pause")
            http.post.assert_called_with("/monitors/mon_1/pause")

            hook.monitor_control("mon_1", "run_now")
            http.post.assert_called_with("/monitors/mon_1/run")

            hook.monitor_control("mon_1", "delete")
            http.delete.assert_called_once_with("/monitors/mon_1")


def test_monitor_control_rejects_unknown_action() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin"):
            with pytest.raises(ValueError, match="Unknown monitor action"):
                hook.monitor_control("mon_1", "explode")


def test_ai_visibility_search_submits_then_polls_until_not_running() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            client = mock_anakin_cls.return_value
            http = client._http
            http.post.return_value = {"search_id": "srch_1", "status": "running"}
            http.poll.return_value = {"search_id": "srch_1", "status": "completed", "results": []}

            result = hook.ai_visibility_search("what is anakin?", sources=["chatgpt"])

            http.post.assert_called_once_with(
                "/ai-visibility/search", json={"query": "what is anakin?", "sources": ["chatgpt"]}
            )
            assert http.poll.call_args.args == ("/ai-visibility/search/srch_1",)
            assert http.poll.call_args.kwargs["poll_timeout"] == client._poll_timeout
            assert result["status"] == "completed"


def test_browser_task_submits_then_polls_until_terminal() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            client = mock_anakin_cls.return_value
            http = client._http
            http.post.return_value = {"workflow_id": "wf_1", "status": "running"}
            http.poll.return_value = {"status": "completed", "result": {"ok": True}}

            result = hook.browser_task("find the cheapest TV", url="https://example.com")

            http.post.assert_called_once_with(
                "/ai/evaluate",
                json={"prompt": "find the cheapest TV", "async": True, "url": "https://example.com"},
            )
            assert http.poll.call_args.args == ("/ai/jobs/wf_1",)
            assert result["status"] == "completed"
