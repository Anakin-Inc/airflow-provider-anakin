# airflow-provider-anakin

An Apache Airflow provider for [Anakin](https://anakin.io) — full coverage of
the Anakin API (https://api.anakin.io/v1): web scraping, site mapping and
crawling, AI-powered search and multi-stage agentic research, Wire pre-built
site automations, website change monitoring, AI-visibility (LLM
answer-engine) search, browser-session management, and AI browser
automation. Built on the official
[`anakin-sdk`](https://github.com/Anakin-Inc/anakin-py) Python client
wherever it covers an endpoint (scrape, map, crawl, search, agentic-search,
Wire action execution, browser sessions) — this provider does not
reimplement HTTP calls, retries, or polling that already live in the SDK.
The handful of endpoints the SDK doesn't wrap yet (Wire discovery/catalog/
identities/login/build, Website Monitoring, AI Visibility, and the AI
browser-task endpoint) are reached via direct HTTP calls that reuse the
SDK's own authenticated, retrying transport rather than a second,
duplicate HTTP implementation — see `AnakinHook._raw_http()`.

Published independently to PyPI, in its own repository. This is the
upstream-endorsed path for third-party Airflow providers — Airflow's own
docs describe custom providers as having "exactly the same capability as
the providers written by the community" and discovered automatically once
installed, with no PR into `apache/airflow` required. Real third-party
providers (e.g. `airflow-provider-fivetran`, Astronomer's own
`airflow-provider-sample` template) ship this way, under an
`airflow-provider-<name>` PyPI name — not `apache-airflow-providers-<name>`,
which is reserved for providers living in the `apache/airflow` monorepo
itself.

## Install

```bash
pip install -U airflow-provider-anakin
```

Airflow auto-discovers the provider once installed — verify with:

```bash
airflow providers get anakin-provider  # or: airflow providers list
```

## Connection

Create an Airflow Connection with conn type **Anakin**:

- **Connection Id**: `anakin_default` (the hook's default; override per-task
  with `anakin_conn_id=...` if you need multiple Anakin accounts/keys)
- **Password**: your Anakin API key (`ask-...`, free at
  https://anakin.io/dashboard)
- **Extra** (optional): `{"base_url": "..."}` to override the API base URL

No Connection configured at all also works — the hook falls back to the
`ANAKIN_API_KEY` environment variable, the same fallback `anakin-sdk`
itself implements, so a Secrets-Backend-injected env var is enough.

## Operators

### Core: scrape, map, crawl, search

| Operator | What it does |
|---|---|
| `AnakinScrapeOperator` | Scrape a URL to markdown (+ optional formats / AI-extracted JSON) |
| `AnakinMapOperator` | Discover URLs on a site (sitemap + link traversal) |
| `AnakinCrawlOperator` | Crawl a site and return the scraped pages |
| `AnakinSearchOperator` | Synchronous AI-powered web search with citations |
| `AnakinAgenticSearchOperator` | Multi-stage research: search, scrape citations, extract structured data with a summary |

### Wire — pre-built site automations

| Operator | What it does |
|---|---|
| `AnakinWireDiscoverOperator` | Find Wire actions for a task from natural-language intent |
| `AnakinWireCatalogOperator` | Browse the Wire catalog (all sites, or one site's full action list) |
| `AnakinWireReadActionOperator` | Run a read-only Wire action (extract data) |
| `AnakinWireWriteActionOperator` | Run a state-changing Wire action (submit a form, add to cart, ...) |
| `AnakinWireIdentitiesOperator` | List saved Wire identities/credentials |
| `AnakinWireLoginOperator` | Sign in to a credentials-mode Wire site, get a `credential_id` |
| `AnakinWireBuildOperator` | Request a new Wire action for a site not yet in the catalog |

### Website monitoring

| Operator | What it does |
|---|---|
| `AnakinMonitorCreateOperator` | Create a scheduled website monitor (page/site/Wire scope) |
| `AnakinMonitorListOperator` | List website monitors |
| `AnakinMonitorChangesOperator` | Get a monitor's detected changes |
| `AnakinMonitorControlOperator` | Pause / resume / run-now / delete a monitor |

### AI visibility

| Operator | What it does |
|---|---|
| `AnakinAiVisibilitySearchOperator` | Ask multiple AI answer engines the same question and compare answers |
| `AnakinAiVisibilitySourcesOperator` | List the AI engines available to query |

### Browser sessions and automation

| Operator | What it does |
|---|---|
| `AnakinSessionListOperator` | List saved (authenticated) browser sessions |
| `AnakinSessionDeleteOperator` | Delete a saved browser session |
| `AnakinBrowserTaskOperator` | Run a natural-language task in an AI-driven cloud browser |

```python
from airflow import DAG
from anakin_provider.operators.anakin import AnakinScrapeOperator, AnakinSearchOperator

with DAG("my_dag", schedule=None, catchup=False) as dag:
    scrape = AnakinScrapeOperator(task_id="scrape", url="https://example.com")
    search = AnakinSearchOperator(task_id="search", prompt="latest AI agent news")
```

See `example_dags/example_anakin.py` for the original 3-operator example
(scrape/search/agentic-search) and `example_dags/example_anakin_advanced.py`
for a runnable example of all 18 additional operators above, grouped by
capability.

### A note on Wire action execution

`AnakinWireReadActionOperator`/`AnakinWireWriteActionOperator` delegate to
`anakin-sdk`'s `Anakin.wire(action_id, params)`. As of `anakin-sdk` v0.1.0,
that method does not yet accept `credential_id`/`identity_id` (the REST API
supports both, for actions whose `auth_mode` is `"required"`/`"optional"`) —
so these two operators currently only support actions with `auth_mode:
"none"`. This is a real, current SDK limitation, not an oversight in this
provider; it'll be lifted with no operator-level change once `anakin-sdk`
adds the parameters.

## Hook

`AnakinHook` (conn type `anakin`) can also be used directly, e.g. from a
`@task`/`PythonOperator`:

```python
from anakin_provider.hooks.anakin import AnakinHook

def my_task():
    hook = AnakinHook()  # or AnakinHook(anakin_conn_id="my_other_conn")
    doc = hook.scrape("https://example.com")
    return doc.markdown
```

`AnakinHook.test_connection()` (wired to the Connections UI's "Test"
button) only validates that an API key resolves — it deliberately makes
**no network call**, since `/search` and `/agentic-search` cost credits per
the Anakin API contract and a connection test button firing shouldn't spend
them.

## Development

This package depends on `anakin-sdk` (live on PyPI) and `apache-airflow`.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[test]"
pytest
```

## License

Apache-2.0.
