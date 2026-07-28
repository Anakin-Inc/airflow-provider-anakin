"""Example DAG for the Anakin provider.

Not part of the installable package (not under ``anakin_provider/``) —
provided for reference the same way community providers ship
``example_dags/``. Requires an Airflow Connection named ``anakin_default``
(conn type ``anakin``, password = your Anakin API key) or an
``ANAKIN_API_KEY`` environment variable available to the worker.
"""

from __future__ import annotations

import datetime

from airflow import DAG

from anakin_provider.operators.anakin import (
    AnakinAgenticSearchOperator,
    AnakinScrapeOperator,
    AnakinSearchOperator,
)

with DAG(
    dag_id="example_anakin",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["anakin", "example"],
) as dag:
    scrape = AnakinScrapeOperator(
        task_id="scrape_docs_homepage",
        url="https://anakin.io/docs",
    )

    search = AnakinSearchOperator(
        task_id="search_recent_ai_agent_news",
        prompt="latest developments in AI agents",
        limit=5,
    )

    research = AnakinAgenticSearchOperator(
        task_id="research_crm_pricing",
        prompt="Compare pricing across the top 5 CRM platforms",
        use_browser=True,
    )

    scrape >> search >> research
