"""Unit tests for AnakinHook.

Mocks `BaseHook.get_connection` (the Airflow metadata-DB lookup) and the
`anakin.Anakin` client construction — never makes a network call and never
needs a real Airflow metadata database.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest
from airflow.exceptions import AirflowNotFoundException
from anakin.errors import AnakinError, ConfigurationError, JobFailedError

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
    with (
        patch.object(hook, "get_connection", return_value=_fake_connection(password="ask_secret")),
        patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls,
    ):
        hook.get_conn()
        mock_anakin_cls.assert_called_once_with(
            api_key="ask_secret", base_url=DEFAULT_ANAKIN_BASE_URL
        )


def test_get_conn_honours_base_url_override_from_extra() -> None:
    hook = AnakinHook()
    with (
        patch.object(
            hook,
            "get_connection",
            return_value=_fake_connection(
                password="ask_secret", extra={"base_url": "https://staging.anakin.io/v1"}
            ),
        ),
        patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls,
    ):
        hook.get_conn()
        mock_anakin_cls.assert_called_once_with(
            api_key="ask_secret", base_url="https://staging.anakin.io/v1"
        )


def test_get_conn_reads_prefixed_base_url_extra() -> None:
    hook = AnakinHook()
    with (
        patch.object(
            hook,
            "get_connection",
            return_value=_fake_connection(
                password="ask_secret", extra={"extra__anakin__base_url": "https://x.test/v1"}
            ),
        ),
        patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls,
    ):
        hook.get_conn()
        mock_anakin_cls.assert_called_once_with(api_key="ask_secret", base_url="https://x.test/v1")


def test_get_conn_falls_back_to_env_var_when_connection_missing() -> None:
    hook = AnakinHook()
    with (
        patch.object(hook, "get_connection", side_effect=AirflowNotFoundException("no such conn")),
        patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls,
    ):
        hook.get_conn()
        # api_key=None tells anakin.Anakin() to fall back to ANAKIN_API_KEY itself.
        mock_anakin_cls.assert_called_once_with(api_key=None, base_url=DEFAULT_ANAKIN_BASE_URL)


def test_get_conn_caches_the_client() -> None:
    hook = AnakinHook()
    with (
        patch.object(hook, "get_connection", return_value=_fake_connection(password="ask_secret")),
        patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls,
    ):
        client1 = hook.get_conn()
        client2 = hook.get_conn()
        assert client1 is client2
        mock_anakin_cls.assert_called_once()


def test_test_connection_fails_with_no_key_anywhere() -> None:
    hook = AnakinHook()
    with patch.object(hook, "get_connection", side_effect=AirflowNotFoundException("no such conn")):
        ok, message = hook.test_connection()
    assert ok is False
    assert "ANAKIN_API_KEY" in message


def test_test_connection_succeeds_without_a_network_call() -> None:
    hook = AnakinHook()
    with (
        patch.object(hook, "get_connection", return_value=_fake_connection(password="ask_secret")),
        patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls,
    ):
        ok, message = hook.test_connection()
        assert ok is True
        assert "no network call" in message.lower()
        # test_connection must not touch billed endpoints.
        mock_anakin_cls.return_value.search.assert_not_called()
        mock_anakin_cls.return_value.agentic_search.assert_not_called()


def test_scrape_search_agentic_search_delegate_to_client() -> None:
    hook = AnakinHook()
    with (
        patch.object(hook, "get_connection", return_value=_fake_connection(password="ask_secret")),
        patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls,
    ):
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
    with (
        patch.object(hook, "get_connection", return_value=_fake_connection(password="ask_secret")),
        patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls,
    ):
        client = mock_anakin_cls.return_value

        hook.map("https://example.com", depth=3)
        client.map.assert_called_once_with("https://example.com", depth=3)

        hook.crawl("https://example.com", max_pages=5)
        client.crawl.assert_called_once_with("https://example.com", max_pages=5)


@pytest.fixture
def client():
    """A mocked ``anakin.Anakin`` client behind a hook with a resolved key."""
    hook = AnakinHook()
    with (
        patch.object(
            AnakinHook, "get_connection", return_value=_fake_connection(password="ask_secret")
        ),
        patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls,
    ):
        mock_anakin_cls.return_value.hook = hook
        yield mock_anakin_cls.return_value


def test_wire_read_and_write_action_both_delegate_to_wire_run(client: MagicMock) -> None:
    hook = client.hook
    hook.wire_read_action("walmart.search_products", {"q": "phones"}, poll_timeout=30)
    client.wire.run.assert_called_once_with(
        "walmart.search_products",
        {"q": "phones"},
        credential_id=None,
        identity_id=None,
        poll_timeout=30,
    )

    client.wire.run.reset_mock()
    hook.wire_write_action("amazon.add_to_cart", {"sku": "123"}, credential_id="cred_1")
    client.wire.run.assert_called_once_with(
        "amazon.add_to_cart",
        {"sku": "123"},
        credential_id="cred_1",
        identity_id=None,
        poll_timeout=None,
    )


def test_wire_action_defaults_params_to_empty_dict(client: MagicMock) -> None:
    client.hook.wire_read_action("some.action_with_no_params")
    client.wire.run.assert_called_once_with(
        "some.action_with_no_params", {}, credential_id=None, identity_id=None, poll_timeout=None
    )


def test_session_list_and_delete_delegate_to_client_sessions(client: MagicMock) -> None:
    client.hook.session_list(domain="amazon.com")
    client.sessions.list.assert_called_once_with(domain="amazon.com")

    client.hook.session_delete("sess_123")
    client.sessions.delete.assert_called_once_with("sess_123")


def test_wire_discover_delegates_with_filters(client: MagicMock) -> None:
    client.hook.wire_discover("top phones on walmart", catalog="walmart", limit=5)
    client.wire.discover.assert_called_once_with(
        "top phones on walmart", catalog="walmart", category=None, auth_mode=None, limit=5
    )


def test_wire_catalog_with_and_without_slug(client: MagicMock) -> None:
    client.hook.wire_catalog("walmart")
    client.wire.catalog.assert_called_once_with("walmart")
    client.wire.catalogs.assert_not_called()

    client.hook.wire_catalog()
    client.wire.catalogs.assert_called_once_with()


def test_wire_identities_login_build_and_status(client: MagicMock) -> None:
    hook = client.hook
    hook.wire_identities("cat_1")
    client.wire.identities.assert_called_once_with(catalog_id="cat_1")

    hook.wire_login("neb", {"email": "a@b.com", "password": "x"})
    client.wire.login.assert_called_once_with(
        "neb",
        {"email": "a@b.com", "password": "x"},
        identity_name=None,
        source_id=None,
        source_ref=None,
    )

    hook.wire_build("https://example.com", "extract product prices", force=True)
    client.wire.build.assert_called_once_with(
        "https://example.com", "extract product prices", force=True
    )

    hook.wire_build_status("build_1")
    client.wire.get_build.assert_called_once_with("build_1")


def test_monitor_create_drops_unset_options(client: MagicMock) -> None:
    client.hook.monitor_create("https://example.com", 60, scope="page", ai_mode=True, country=None)
    client.monitors.create.assert_called_once_with(
        "https://example.com", 60, scope="page", ai_mode=True
    )


def test_monitor_list_and_changes(client: MagicMock) -> None:
    client.hook.monitor_list()
    client.monitors.list.assert_called_once_with()
    client.hook.monitor_changes("mon_1")
    client.monitors.changes.assert_called_once_with("mon_1")


def test_monitor_control_maps_actions_to_sdk_methods(client: MagicMock) -> None:
    hook = client.hook
    assert hook.monitor_control("mon_1", "pause") is client.monitors.pause.return_value
    client.monitors.pause.assert_called_once_with("mon_1")

    hook.monitor_control("mon_1", "resume")
    client.monitors.resume.assert_called_once_with("mon_1")

    assert hook.monitor_control("mon_1", "run_now") is client.monitors.run_now.return_value
    client.monitors.run_now.assert_called_once_with("mon_1")

    assert hook.monitor_control("mon_1", "delete") is None
    client.monitors.delete.assert_called_once_with("mon_1")


def test_monitor_control_rejects_unknown_action(client: MagicMock) -> None:
    with pytest.raises(ValueError, match="Unknown monitor action"):
        client.hook.monitor_control("mon_1", "explode")


def test_ai_visibility_search_and_sources(client: MagicMock) -> None:
    client.hook.ai_visibility_search("what is anakin?", sources=["chatgpt"])
    client.ai_visibility.search.assert_called_once_with(
        "what is anakin?", sources=["chatgpt"], country="us", poll_timeout=None
    )
    client.hook.ai_visibility_sources()
    client.ai_visibility.sources.assert_called_once_with()


# ─── browser task (direct REST call; no SDK method) ──────────────────────────


def _browser_task_hook(handler) -> tuple[AnakinHook, list[httpx.Request]]:
    """A hook whose httpx.Client is served by ``handler`` (no network)."""
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    real_client = httpx.Client

    def fake_client(**kwargs):
        return real_client(transport=httpx.MockTransport(record), **kwargs)

    hook = AnakinHook()
    patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask_secret")
    ).start()
    patch("httpx.Client", side_effect=fake_client).start()
    patch("anakin_provider.hooks.anakin.BROWSER_TASK_POLL_INTERVAL", 0).start()
    return hook, seen


@pytest.fixture(autouse=True)
def _stop_patches():
    yield
    patch.stopall()


def test_browser_task_submits_then_polls_until_completed() -> None:
    polls = iter([{"status": "running"}, {"status": "completed", "result": {"ok": True}}])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"workflow_id": "wf_1", "status": "running"})
        return httpx.Response(200, json=next(polls))

    hook, seen = _browser_task_hook(handler)
    result = hook.browser_task("find the cheapest TV", url="https://example.com")

    assert result == {"status": "completed", "result": {"ok": True}}
    submit = seen[0]
    assert submit.url == "https://api.anakin.io/v1/ai/evaluate"
    assert submit.headers["X-API-Key"] == "ask_secret"
    assert json.loads(submit.content) == {
        "prompt": "find the cheapest TV",
        "async": True,
        "url": "https://example.com",
    }
    assert [r.url.path for r in seen[1:]] == ["/v1/ai/jobs/wf_1", "/v1/ai/jobs/wf_1"]


def test_browser_task_raises_job_failed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"workflow_id": "wf_2"})
        return httpx.Response(200, json={"status": "failed", "error": "site blocked"})

    hook, _ = _browser_task_hook(handler)
    with pytest.raises(JobFailedError, match="site blocked"):
        hook.browser_task("do something")


def test_browser_task_surfaces_http_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"message": "Invalid API key"})

    hook, _ = _browser_task_hook(handler)
    with pytest.raises(AnakinError, match="Invalid API key"):
        hook.browser_task("do something")


def test_browser_task_needs_an_api_key() -> None:
    hook = AnakinHook()
    with (
        patch.object(hook, "get_connection", side_effect=AirflowNotFoundException("no such conn")),
        pytest.raises(ConfigurationError),
    ):
        hook.browser_task("do something")
