# airflow-provider-anakin — verification status

## Extension pass: full 21/21 API coverage (feat/full-tool-coverage)

The original build (`main`, `0.1.0`) covered 3 of Anakin's 21 API
capabilities: `scrape`, `search`, `agentic_search`. This branch adds the
remaining 18 as new Operators, bringing coverage to 21/21. `__version__`
bumped to `0.2.0` (feature addition, semver minor).

**Ground truth for this pass**, read directly, not recalled or guessed:

- `anakin-py/src/anakin/client.py` (the real, current `anakin-sdk` v0.1.0
  Python client) — for exactly which of the 18 already have a public SDK
  method, and their real signatures.
- `anakin-mcp/src/client.ts` and `anakin-mcp/src/tools/{wire,monitor,
  ai-visibility,browser-task,sessions}.ts` (Anakin's own MCP server) — for
  the REST endpoints, request bodies, and response shapes of the 18
  capabilities, since that server talks to the exact same
  `https://api.anakin.io/v1` this provider does.

**Two corrections to the task brief's own ground-truth summary, found by
reading the code rather than trusting the summary at face value:**

1. **`session_list`/`session_delete` ARE in `anakin-sdk`.** The brief
   stated these were confirmed missing from `client.py` "in earlier
   research this session." Reading `client.py` directly in *this* pass
   shows `Anakin.sessions` is a `SessionsClient` sub-namespace with
   `list(domain=None)` and `delete(session_id)` methods, both real and
   public. `AnakinHook.session_list()`/`session_delete()` delegate to them
   — no raw HTTP for these two, unlike the brief's suggested grouping with
   monitor/ai-visibility/browser-task.
2. **The "wire family" is one SDK method, not seven.** `Anakin.wire
   (action_id, params, poll_timeout=None)` is the only Wire method
   `anakin-sdk` v0.1.0 exposes — it covers `POST /wire/task` + polling
   `GET /wire/jobs/:id` (action *execution* only). `wire_discover`
   (`GET /wire/resolve`), `wire_catalog` (`GET /wire/catalog[/:slug]`),
   `wire_identities` (`GET /wire/identities`), `wire_login`
   (`POST /wire/login`), and `wire_build` (`POST /wire/build-request`) have
   **no** SDK method as of v0.1.0 — confirmed by reading `client.py`
   top-to-bottom, not by absence of a grep hit. `wire_read_action`/
   `wire_write_action` (both operators) delegate to the one real
   `Anakin.wire()` method — mirroring how `anakin-mcp`'s own
   `wire_read_action`/`wire_write_action` tools share one `runWireAction()`
   implementation and differ only in their safety annotation. The other
   five are raw HTTP.
3. **A real, surfaced SDK gap, not silently worked around:** the REST API
   (per `anakin-mcp/src/client.ts`'s `wireRun`) accepts `credential_id`/
   `identity_id` on Wire action execution, for actions whose `auth_mode` is
   `"required"`/`"optional"`. `anakin-sdk` v0.1.0's `Anakin.wire()` does
   not expose either parameter — its request body is only
   `{action_id, params}`. Rather than bypass the SDK with a parallel raw-
   HTTP path for just this one field (which would violate the "reuse the
   SDK, don't reimplement" instruction the moment an auth-required action
   was needed), `AnakinWireReadActionOperator`/`AnakinWireWriteActionOperator`
   currently only support `auth_mode: "none"` actions, documented in both
   operators' docstrings and the README. Revisit once `anakin-sdk` adds
   the parameters.

**Endpoints with no SDK method** (`wire_discover`, `wire_catalog`,
`wire_identities`, `wire_login`, `wire_build`, `monitor_create`,
`monitor_list`, `monitor_changes`, `monitor_control`,
`ai_visibility_sources`, `ai_visibility_search`, `browser_task` — 12 of the
18) are reached via `AnakinHook._raw_http()`, which returns
`anakin.Anakin()._http` — the SDK client's own internal, already-
authenticated `httpx`-backed `HttpClient` (auth header injection,
exponential-backoff retry on 429/5xx, `Retry-After` handling, and — for
`ai_visibility_search`/`browser_task` — its existing `poll()` helper reused
for the two endpoints' own async-job shapes). This is a deliberate,
documented reach into a `_`-prefixed internal (flagged with `# noqa: SLF001`
and explained in the hook's own docstring), not a second hand-rolled HTTP
client — made safe by this package's existing `anakin-sdk>=0.1.0,<0.2.0`
pin. Endpoint paths/bodies for all 12 were read from `anakin-mcp`'s
`client.ts`/`tools/*.ts`, cross-checked against each other (e.g.
`monitor_control`'s `run_now` action posts to `.../run`, not `.../run_now`
— confirmed from `client.ts`'s `monitorControl` switch statement, not
guessed from the action name).

**MCP-layer policy checks intentionally NOT replicated here:**
`anakin-mcp`'s `wire_write_action`/`wire_build`/`browser_task` tools run a
`financialBlockReason()` content filter (refusing payment/fund-transfer
instructions) because that server exposes these as tools to an autonomous
LLM agent acting on open-ended natural-language input. This provider's
operators are authored by a trusted engineer in a DAG file, not driven by
an LLM's own judgment call on each invocation — so that guardrail is an
MCP-specific safety layer, not part of the REST API contract itself, and
out of scope for an orchestration provider. Noted here rather than silently
dropped.

**Verification performed for this pass:**

- All 13 source files (`anakin_provider/**/*.py`, `example_dags/*.py`,
  `tests/*.py`) — the original 10 plus the new
  `example_dags/example_anakin_advanced.py` and the two edited test files —
  pass `python3 -m py_compile`.
- `get_provider_info()` re-executed standalone after every edit (zero
  Airflow/anakin-sdk imports preserved by design — see its own module
  docstring): confirmed `__version__` reads `"0.2.0"`,
  `info["versions"] == ["0.2.0"]`, and `operators`/`connection-types` still
  point at the real `anakin_provider.operators.anakin` /
  `anakin_provider.hooks.anakin.AnakinHook` paths (unchanged — new
  operators live in the same module, so no path-list update was needed
  there; only the `description` text was expanded to name the full
  capability set).
- 18 new operator classes, one per capability, added to
  `anakin_provider/operators/anakin.py`; 12 new hook methods (map, crawl,
  wire_read_action, wire_write_action are 4 of the 18 backed by existing
  SDK methods reused as-is) plus `_raw_http()` added to
  `anakin_provider/hooks/anakin.py`.
- Tests: extended `tests/test_hooks.py` (new tests for every new hook
  method, including exact-body assertions for the 12 raw-HTTP endpoints)
  and `tests/test_operators.py` (one wiring test per new operator, plus two
  for `AnakinAiVisibilitySearchOperator`'s `include_full_content` XCom-size
  guard) — same mocking pattern as the original 9 tests (patch
  `AnakinHook`/`Anakin`/`BaseHook.get_connection`, no network access
  needed). **Written but not run** for the same reason as the original
  pass — see "What's NOT verified" below, which still applies unchanged.

## Live-verified, not assumed

- `anakin-sdk` is **live on PyPI right now** (confirmed via
  `curl https://pypi.org/pypi/anakin-sdk/json` → 200, version `0.1.0`).
  `pyproject.toml` depends on it normally (`anakin-sdk>=0.1.0,<0.2.0`) — no
  local path override needed or present, same as `langchain-anakin`.
- **The Airflow provider structure was not invented — it was read directly
  from Airflow's real docs and a real reference implementation**, both
  fetched live in this pass, not recalled from memory:
  - `https://airflow.apache.org/docs/apache-airflow-providers/howto/create-custom-providers.html`
    confirms: the entry-point group is `apache_airflow_provider`
    (singular), it must resolve to a callable returning a dict validated
    against `airflow/provider_info.schema.json`, and Airflow auto-discovers
    providers at runtime by scanning installed packages for that entry
    point — no PR into `apache/airflow` needed, "custom providers... have
    exactly the same capability as the providers written by the community."
  - `astronomer/airflow-provider-sample` (Astronomer's own, current,
    real template repo for third-party providers) was fetched file-by-file
    from `raw.githubusercontent.com` — `pyproject.toml`,
    `sample_provider/__init__.py`, `sample_provider/hooks/sample.py`,
    `sample_provider/operators/sample.py` — and used as the structural
    reference for `AnakinHook`/`AnakinScrapeOperator`'s class-attribute
    conventions (`conn_name_attr`, `default_conn_name`, `conn_type`,
    `hook_name`, `get_connection_form_widgets`, `get_ui_field_behaviour`)
    and the `get_provider_info()` dict shape.
  - The `provider_info.schema.json` shape for `operators` (list of
    `{integration-name, python-modules}`) and `connection-types` (list of
    `{connection-type, hook-class-name, hook-name}`, only the first two
    required) was fetched via a Fossies mirror since GitHub raw 404'd on
    the exact path guessed first — cross-checked against a second web
    search before trusting it.
  - This confirms the **naming convention** too: real third-party,
    independently-published providers use PyPI name
    `airflow-provider-<name>` (this package, `airflow-provider-fivetran`,
    Astronomer's own `airflow-provider-sample`) — **not**
    `apache-airflow-providers-<name>`, which the task brief suggested as an
    option but which in practice denotes a provider living inside the
    `apache/airflow` monorepo itself. Using that prefix for an independent
    package would misrepresent it as Apache-blessed. This is a deliberate,
    evidence-backed deviation from the brief's suggested name, documented
    here rather than silently applied.
- All 10 source files (`anakin_provider/**/*.py`, `example_dags/*.py`,
  `tests/*.py`) pass `python3 -m py_compile` — syntactically valid.
  `get_provider_info()` was additionally **actually executed** (not just
  compiled) — it has zero Airflow/anakin-sdk imports by design (see below),
  so it runs in any bare Python 3.10+ interpreter. Ran it directly and
  asserted its `connection-types`/`operators` dotted paths match the real
  file layout; output and assertions both passed, shown in this session's
  transcript.
- Every field/method referenced against real source, not assumed:
  `Anakin.__init__(api_key=, *, base_url=, ...)`, `client.scrape()`,
  `client.search()`, `client.agentic_search()` signatures and defaults
  (`country="us"`, `use_browser=False`, `generate_json=False`,
  `force_fresh=False` for scrape; `limit=5` for search; `use_browser=True`
  for agentic_search — note this default is `True`, opposite of scrape's,
  and the operator/hook signatures preserve that distinction rather than
  flattening both to one default) all read directly from
  `anakin-py/src/anakin/client.py`. Response shapes (`Document`,
  `SearchResult`, `AgenticSearchResult`) read from
  `anakin-py/src/anakin/models.py` — both Pydantic v2 models, which is why
  operators call `.model_dump(mode="json")` rather than plain
  `.model_dump()`: the models contain `datetime | None` fields
  (`created_at`, `completed_at`) that plain `model_dump()` leaves as
  `datetime` objects — not JSON-serialisable, which would break Airflow's
  default JSON XCom backend silently at runtime. `mode="json"` converts
  them to ISO strings up front.
- Confirmed `POST /url-scraper`'s body includes `formats` (a list, default
  `("markdown",)` in the SDK) even though the task brief's endpoint summary
  didn't mention it — included it as an operator param anyway per the
  brief's own instruction to treat `client.py` as ground truth over any
  written description.
- `get_provider_info()`, `hooks/__init__.py`, and `operators/__init__.py`
  are deliberately import-light (no `airflow` or `anakin` imports at
  package scope) — this mirrors the real sample provider's design intent:
  Airflow's provider manager calls `get_provider_info()` at startup for
  *every* installed provider just to build its metadata cache, so a
  provider that eagerly imported its own Hook/Operator classes (and
  therefore `airflow.hooks.base`, `flask_appbuilder`, etc.) at that point
  would slow down every Airflow process start, not just DAGs that use it.

## What's NOT verified

**Never installed inside an actual Airflow instance** — that requires a
running Airflow deployment (metadata DB, webserver/scheduler, a real
Connection row), which this environment doesn't have. Nor was
`apache-airflow` itself ever `pip install`ed here (`python3 -c "import
airflow"` fails with `ModuleNotFoundError` — checked, not assumed) — same
network-restricted-sandbox pattern hit by every other Python package built
this session (`langchain-anakin`, `dagster-anakin`). `pytest` is not
installed either. So:

- `tests/test_hooks.py` and `tests/test_operators.py` (9 tests total) were
  **written but never run** — they mock `AnakinHook`/`BaseHook.get_connection`
  entirely and need no live network access once `apache-airflow` and
  `anakin-sdk` are installed, but that install itself wasn't attempted here.
- `tests/test_provider_info.py` (4 tests) is the one exception — it has no
  external dependencies, and its assertions were manually executed and
  passed (see above), though not through an actual `pytest` run.
- `example_dags/example_anakin.py` was `py_compile`d only — never loaded by
  a real Airflow DagBag, never scheduled, never run end-to-end against the
  live Anakin API.
- The `get_connection_form_widgets()`/`get_ui_field_behaviour()` methods on
  `AnakinHook` (which customize the Connections UI form) depend on
  `flask_appbuilder`/`flask_babel`/`wtforms` — those imports are deferred
  inside the method bodies (matching the real sample's pattern, since
  they're only needed when Airflow's webserver renders the form), so
  `py_compile` never touches them either. Never rendered in an actual
  webserver.
- `airflow providers get anakin-provider` / `airflow providers list` (the
  README's suggested verification commands) were never run — no Airflow
  CLI available here.

## Steps (needs the account owner)

1. In an environment with normal network access:
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install -e ".[test]"
   pytest
   ```
2. Stand up (or reuse) a real Airflow instance, `pip install -e .` this
   package into it, add an `anakin_default` Connection (conn type `anakin`,
   password = a real Anakin API key), run `airflow providers list` to
   confirm discovery, then trigger `example_dags/example_anakin.py`
   end-to-end.
3. Push as `Anakin-Inc/anakin-airflow-provider`, publish to PyPI as
   `airflow-provider-anakin`.
4. Optional, once live: submit to the Airflow Registry
   (`https://airflow.apache.org/registry/`), which lists third-party
   providers by crawling PyPI for packages with the
   `Framework :: Apache Airflow :: Provider` classifier (present in
   `pyproject.toml`'s `classifiers` here) — not independently confirmed how
   that crawl/submission process works beyond seeing the classifier
   referenced in the sample provider's own `pyproject.toml`.

## Not done

Never installed or run against a real Airflow deployment — verified by
close reading against real Airflow docs + a real reference provider's
source, `py_compile` on all files, and one direct execution of the
dependency-free `get_provider_info()` function. Treat as
reviewed-but-unexecuted, same status as `langchain-anakin` and
`dagster-anakin` before them.
