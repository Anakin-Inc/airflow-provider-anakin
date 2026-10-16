# airflow-provider-anakin

An [Apache Airflow](https://airflow.apache.org) provider for
[Anakin](https://anakin.io). Scrape pages, map and crawl sites, run web
search and multi-step research, run Wire actions on sites like Amazon or
Walmart, create website monitors, and drive an AI browser, all as Airflow
tasks. Each operator returns JSON-safe results to XCom.

Built on the official [`anakin-sdk`](https://pypi.org/project/anakin-sdk/)
Python client, which handles authentication, retries and job polling.

| | |
|---|---|
| **Operators** | 22 (parity with the [Anakin MCP server](https://anakin.io/docs/integrations/ai-agents/mcp-server)) |
| **Hook / connection type** | `AnakinHook` / `anakin` |
| **Requires** | Python 3.10+, Apache Airflow 2.10+ or 3.x |
| **License** | Apache-2.0 |

## Install

```bash
pip install airflow-provider-anakin
```

Airflow discovers the provider automatically. Check with:

```bash
airflow providers get airflow-provider-anakin
```

## Connection

Get an API key (starts with `ask_`) from the
[Anakin dashboard](https://anakin.io/dashboard). Sign-up is free and
includes 300 credits.

Create a connection with conn type **Anakin**:

- **Connection Id**: `anakin_default` (the default every operator uses; pass
  `anakin_conn_id=...` to use another)
- **API Key** (password field): your Anakin API key
- **Base URL** (optional extra): only for non-production environments

Or from the CLI:

```bash
airflow connections add anakin_default --conn-type anakin --conn-password "ask_..."
```

If no connection exists, the hook falls back to the `ANAKIN_API_KEY`
environment variable. The connection's **Test** button checks that a key
resolves; it makes no API call, so it never spends credits.

## Quick start

```python
import pendulum

from airflow.sdk import DAG  # Airflow 2: from airflow import DAG
from anakin_provider.operators.anakin import AnakinCrawlOperator, AnakinScrapeOperator

with DAG("anakin_quickstart", schedule="@daily", start_date=pendulum.datetime(2026, 1, 1)):
    pricing = AnakinScrapeOperator(
        task_id="scrape_pricing",
        url="https://example.com/pricing",
        generate_json=True,  # also extract structured JSON with AI
    )
    docs = AnakinCrawlOperator(
        task_id="crawl_docs",
        url="https://example.com/docs",
        max_pages=50,
        include_patterns=["/docs/*"],
    )
```

`example_dags/` has a runnable DAG for every operator.

## Operators

All operators live in `anakin_provider.operators.anakin`.

### Web data

| Operator | What it does |
|---|---|
| `AnakinScrapeOperator` | Scrape one URL to markdown, with optional HTML, screenshot or AI-extracted JSON |
| `AnakinSearchOperator` | Web search: URLs, titles and snippets, returned immediately |
| `AnakinMapOperator` | List the URLs on a site |
| `AnakinCrawlOperator` | Crawl a site and return each page as markdown |
| `AnakinAgenticSearchOperator` | Multi-stage research: search, read sources, return a summary plus structured data |

### Wire: pre-built site actions

| Operator | What it does |
|---|---|
| `AnakinWireDiscoverOperator` | Find actions for a task, e.g. "top phones on walmart" |
| `AnakinWireCatalogOperator` | List supported sites, or one site's actions and their parameters |
| `AnakinWireReadActionOperator` | Run an action that reads data |
| `AnakinWireWriteActionOperator` | Run an action that changes something on the site |
| `AnakinWireIdentitiesOperator` | List saved site logins and their `credential_id`s |
| `AnakinWireLoginOperator` | Sign in to a site and get a `credential_id` |
| `AnakinWireBuildOperator` | Request actions for a site Wire doesn't cover yet |
| `AnakinWireBuildStatusOperator` | Check a build request |

Wire action inputs go in `action_params` (`params` is reserved by Airflow).
Actions that need a login take `credential_id`.

### Website monitoring

| Operator | What it does |
|---|---|
| `AnakinMonitorCreateOperator` | Watch a page, a site or a Wire action on a schedule |
| `AnakinMonitorListOperator` | List monitors |
| `AnakinMonitorChangesOperator` | Get the changes a monitor detected |
| `AnakinMonitorControlOperator` | Pause, resume, run now or delete a monitor |

### AI visibility

| Operator | What it does |
|---|---|
| `AnakinAiVisibilitySearchOperator` | Ask ChatGPT, Gemini and Google AI Overview the same question and compare answers |
| `AnakinAiVisibilitySourcesOperator` | List the available answer engines |

### Browser

| Operator | What it does |
|---|---|
| `AnakinSessionListOperator` | List saved browser sessions, for pages behind a login |
| `AnakinSessionDeleteOperator` | Delete a saved browser session |
| `AnakinBrowserTaskOperator` | Have an AI agent drive a cloud browser through a multi-step task |

### Chaining tasks with XCom

Templated fields accept XCom pulls, so one task can feed the next:

```python
discover = AnakinWireDiscoverOperator(
    task_id="discover", q="search products on walmart", auth_mode="none"
)
run = AnakinWireReadActionOperator(
    task_id="run",
    action_id="{{ ti.xcom_pull(task_ids='discover')[0]['action_id'] }}",
    action_params={"query": "wireless headphones"},
)
discover >> run
```

### Long-running jobs

Scrape, map, crawl, agentic search, Wire actions and AI-visibility searches
are asynchronous jobs; the operator submits the job and polls until it
finishes. `poll_timeout` (seconds) caps the wait. Agentic search usually
takes 1 to 5 minutes and a browser task up to about 6.

## Hook

Use `AnakinHook` from your own tasks:

```python
from airflow.sdk import task
from anakin_provider.hooks.anakin import AnakinHook


@task
def headlines() -> str:
    doc = AnakinHook().scrape("https://example.com/news")
    return doc.markdown
```

`AnakinHook().get_conn()` returns the configured `anakin.Anakin` client
for anything else in the SDK.

## Errors

Failures raise the SDK's exceptions, all subclasses of `anakin.AnakinError`
(for example `AuthenticationError`, `InsufficientCreditsError`,
`RateLimitError`, `JobFailedError`, `JobTimeoutError`). Airflow marks the
task failed and applies its retry policy. Note that Wire write actions,
monitor and Wire build creation, and browser tasks are not idempotent;
think before setting `retries` on them.

## Development

```bash
uv venv && source .venv/bin/activate
uv pip install -e . --group test --group lint
pytest
ruff check . && ruff format --check . && mypy anakin_provider
```

## License

Apache-2.0
