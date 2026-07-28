# airflow-provider-anakin

An Apache Airflow provider for [Anakin](https://anakin.io) — web scraping,
AI-powered search, and multi-stage agentic research, built on the official
[`anakin-sdk`](https://github.com/Anakin-Inc/anakin-py) Python client (not a
separate HTTP implementation).

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

| Operator | What it does |
|---|---|
| `AnakinScrapeOperator` | Scrape a URL to markdown (+ optional formats / AI-extracted JSON) |
| `AnakinSearchOperator` | Synchronous AI-powered web search with citations |
| `AnakinAgenticSearchOperator` | Multi-stage research: search, scrape citations, extract structured data with a summary |

```python
from airflow import DAG
from anakin_provider.operators.anakin import AnakinScrapeOperator, AnakinSearchOperator

with DAG("my_dag", schedule=None, catchup=False) as dag:
    scrape = AnakinScrapeOperator(task_id="scrape", url="https://example.com")
    search = AnakinSearchOperator(task_id="search", prompt="latest AI agent news")
```

See `example_dags/example_anakin.py` for a full runnable example, including
`AnakinAgenticSearchOperator`.

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
