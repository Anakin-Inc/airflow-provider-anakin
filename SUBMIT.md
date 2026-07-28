# airflow-provider-anakin — verification status

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
