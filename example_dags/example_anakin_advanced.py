"""Example DAG for the 18 operators added on top of the original
scrape/search/agentic-search trio — see ``example_dags/example_anakin.py``
for those.

Not part of the installable package (not under ``anakin_provider/``) —
provided for reference the same way community providers ship
``example_dags/``. Requires an Airflow Connection named ``anakin_default``
(conn type ``anakin``, password = your Anakin API key) or an
``ANAKIN_API_KEY`` environment variable available to the worker.

Grouped by capability, wired with harmless ``>>`` chains purely to keep the
DAG graph readable in the UI — there's no real data dependency between,
say, mapping a site and creating a website monitor. Split this into
separate DAGs per capability group in a real deployment; this file is a
single reference DAG for readability, not a suggested production shape.

Several tasks below reference IDs (``monitor_id``, ``session_id``,
``action_id``, ...) that don't exist until a prior run creates them — swap
in real IDs from your Anakin dashboard, or feed them via XCom from the
``*_create``/``*_list``/``*_discover`` tasks in the same DAG run.
"""

from __future__ import annotations

import datetime

from airflow import DAG

from anakin_provider.operators.anakin import (
    AnakinAiVisibilitySearchOperator,
    AnakinAiVisibilitySourcesOperator,
    AnakinBrowserTaskOperator,
    AnakinCrawlOperator,
    AnakinMapOperator,
    AnakinMonitorChangesOperator,
    AnakinMonitorControlOperator,
    AnakinMonitorCreateOperator,
    AnakinMonitorListOperator,
    AnakinSessionDeleteOperator,
    AnakinSessionListOperator,
    AnakinWireBuildOperator,
    AnakinWireCatalogOperator,
    AnakinWireDiscoverOperator,
    AnakinWireIdentitiesOperator,
    AnakinWireLoginOperator,
    AnakinWireReadActionOperator,
    AnakinWireWriteActionOperator,
)

with DAG(
    dag_id="example_anakin_advanced",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["anakin", "example", "advanced"],
) as dag:
    # ─── map / crawl ────────────────────────────────────────────────────────
    map_site = AnakinMapOperator(
        task_id="map_docs_site",
        url="https://anakin.io/docs",
        depth=2,
        limit=50,
    )

    crawl_site = AnakinCrawlOperator(
        task_id="crawl_docs_site",
        url="https://anakin.io/docs",
        max_pages=10,
        depth=1,
    )

    # ─── Wire: discovery, execution, identities ────────────────────────────
    discover_wire_action = AnakinWireDiscoverOperator(
        task_id="discover_wire_action",
        q="search products on walmart",
        limit=5,
    )

    browse_wire_catalog = AnakinWireCatalogOperator(
        task_id="browse_walmart_catalog",
        slug="walmart",
    )

    run_wire_read_action = AnakinWireReadActionOperator(
        task_id="run_wire_read_action",
        action_id="walmart.search_products",
        params={"query": "wireless headphones"},
    )

    run_wire_write_action = AnakinWireWriteActionOperator(
        task_id="run_wire_write_action",
        action_id="walmart.add_to_cart",
        params={"product_id": "PLACEHOLDER_SKU", "quantity": 1},
    )

    list_wire_identities = AnakinWireIdentitiesOperator(
        task_id="list_wire_identities",
    )

    wire_login = AnakinWireLoginOperator(
        task_id="wire_login",
        catalog_slug="neb",
        params={"email": "PLACEHOLDER_EMAIL", "password": "PLACEHOLDER_PASSWORD"},
    )

    request_new_wire_action = AnakinWireBuildOperator(
        task_id="request_new_wire_action",
        website_url="https://example.com",
        goal="Extract the product name, price, and stock status from a product page.",
    )

    # ─── Website monitoring ─────────────────────────────────────────────────
    create_monitor = AnakinMonitorCreateOperator(
        task_id="create_price_monitor",
        url="https://example.com/product/123",
        interval_minutes=60,
        watch_mode="specific_data",
        output_schema={"type": "object", "properties": {"price": {"type": "string"}}},
        ai_mode=True,
        ai_goal="only when the price drops or the item goes out of stock",
    )

    list_monitors = AnakinMonitorListOperator(task_id="list_monitors")

    get_monitor_changes = AnakinMonitorChangesOperator(
        task_id="get_monitor_changes",
        monitor_id="PLACEHOLDER_MONITOR_ID",
    )

    pause_monitor = AnakinMonitorControlOperator(
        task_id="pause_monitor",
        monitor_id="PLACEHOLDER_MONITOR_ID",
        action="pause",
    )

    # ─── AI visibility ──────────────────────────────────────────────────────
    list_ai_visibility_sources = AnakinAiVisibilitySourcesOperator(
        task_id="list_ai_visibility_sources",
    )

    compare_ai_answers = AnakinAiVisibilitySearchOperator(
        task_id="compare_ai_answers",
        query="What is Anakin's web scraping API?",
        country="us",
    )

    # ─── Browser sessions and AI browser automation ────────────────────────
    list_sessions = AnakinSessionListOperator(task_id="list_browser_sessions")

    delete_session = AnakinSessionDeleteOperator(
        task_id="delete_browser_session",
        session_id="PLACEHOLDER_SESSION_ID",
    )

    run_browser_task = AnakinBrowserTaskOperator(
        task_id="find_cheapest_product",
        prompt="Find the cheapest 65-inch TV on this site and list its specs.",
        url="https://example.com",
    )

    # Purely cosmetic ordering for the DAG graph view — see module docstring.
    map_site >> crawl_site
    discover_wire_action >> browse_wire_catalog >> run_wire_read_action
    list_wire_identities >> wire_login >> run_wire_write_action
    run_wire_write_action >> request_new_wire_action
    create_monitor >> list_monitors >> get_monitor_changes >> pause_monitor
    list_ai_visibility_sources >> compare_ai_answers
    list_sessions >> delete_session
    list_sessions >> run_browser_task
