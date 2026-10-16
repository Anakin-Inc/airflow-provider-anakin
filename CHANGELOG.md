# Changelog

## 0.2.0 (unreleased)

First release on PyPI.

- 22 operators covering the Anakin API: scrape, search, map, crawl, agentic
  search; Wire discover, catalog, read and write actions, identities, login,
  build and build status; website monitors (create, list, changes, control);
  AI visibility search and sources; browser sessions and AI browser tasks.
- `AnakinHook` and the `anakin` connection type, with connection form
  metadata for both the Airflow 2 and Airflow 3 UIs.
- Built on `anakin-sdk` 0.2. Wire actions accept `credential_id` and
  `identity_id`, so actions that need a login work.
- Wire action input is `action_params` and Wire login input is
  `login_params`, because `params` is reserved by Airflow.
- Every operator returns JSON-safe data for the default XCom backend.
- Supports Apache Airflow 2.10+ and 3.x on Python 3.10 to 3.13.
