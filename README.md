
# ga4bigquery

Minimal, project-agnostic helper for querying **Google Analytics 4 (GA4) BigQuery exports** for
**event counts/uniques** and **multi-step funnels** — with simple grouping and time bucketing.

> Built from a production utility; trimmed to two focused methods: `request_events` and `request_funnel`.

## Installation

```bash
pip install git+https://github.com/hortemo/ga4bigquery.git
```

Or clone and install locally:

```bash
git clone https://github.com/hortemo/ga4bigquery.git
cd ga4bigquery
pip install -e "."
```

### Local development

If you plan to modify the library or run the test suite, install the project in editable mode with
the development dependencies. Creating a virtual environment keeps the tooling isolated:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
pip install -e ".[dev]"
```

### Running tests

The tests use a lightweight stub that captures the generated SQL, so no Google Cloud access is
required. Once the development dependencies are installed, run:

```bash
pytest
```

## Quickstart

### Authentication

`GA4BigQuery` delegates credentials and project discovery to
[`google.cloud.bigquery.Client`](https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client).
The easiest way to get started is to rely on
[Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/provide-credentials-adc),
which the BigQuery client will pick up automatically. Common options include:

1. **Local development via gcloud** – run `gcloud auth application-default login`
   (requires the `gcloud` SDK) and the client will use your user credentials.
2. **Service account key file** – create a service account with BigQuery access,
   download its JSON key, and point the `GOOGLE_APPLICATION_CREDENTIALS`
   environment variable to the file.
3. **Google Cloud runtimes** – when the code runs on Cloud Run, Cloud Functions,
   Vertex, or GCE/GKE with an attached service account, ADC will automatically
   use that service account.

The service account or user must be able to run queries and read the GA4
export dataset. Granting the
[`roles/bigquery.jobUser`](https://cloud.google.com/bigquery/docs/access-control#bigquery.jobUser)
project role plus dataset-level `roles/bigquery.dataViewer` access is usually
enough. If you need to use explicit credentials, construct the BigQuery client
yourself and pass it to `GA4BigQuery`:

```python
from google.oauth2 import service_account
from google.cloud import bigquery

creds = service_account.Credentials.from_service_account_file("/path/key.json")
client = bigquery.Client(project="my-project", credentials=creds)
ga = GA4BigQuery(table_id=TABLE_ID, tz=TZ, user_id_col=USER_ID_COL, client=client)
```

Once the client is authenticated you can run the Quickstart example below or
any other queries supported by the library.

```python
from datetime import date
from google.cloud import bigquery
from ga4bigquery import GA4BigQuery, FunnelStep

# --- Your GA4 export details ---
TABLE_ID = "bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*"
TZ = "America/Los_Angeles"
USER_ID_COL = "user_pseudo_id"

client = bigquery.Client()
ga = GA4BigQuery(table_id=TABLE_ID, tz=TZ, user_id_col=USER_ID_COL, client=client)

# Daily page views by platform in the public ecommerce sample dataset
page_views = ga.request_events(
    events=["page_view"],
    start=date(2020, 11, 1),
    end=date(2020, 11, 2),
    measure="totals",
    group_by="platform",
    interval="day",
)
print(page_views.tail())
# Expected output:
# platform        WEB
# event_date
# 2020-11-01  11308.0
# 2020-11-02  17698.0

# Multi-step purchase funnel grouped by platform over the same period
steps = [
    FunnelStep(event_name="view_item"),
    FunnelStep(event_name="add_to_cart"),
    FunnelStep(event_name="purchase"),
]
purchase_funnel = ga.request_funnel(
    steps=steps,
    start=date(2020, 11, 1),
    end=date(2020, 11, 2),
    group_by="platform",
    interval="day",
)
print(purchase_funnel.tail())
# Expected output:
#                 1     2    3
# platform      WEB   WEB  WEB
# event_date
# 2020-11-01  607.0   4.0  3.0
# 2020-11-02  897.0  19.0  9.0
```

## Features

- **Events**: totals or uniques (distinct users), optional custom SQL formula
- **Funnels**: arbitrary steps, per-step time windows, grouped by dimensions
- **Group by**:
  - Raw columns like `platform`, `geo.country`, `app_info.version`
  - Nested keys via `event_params.<key>` and `user_properties.<key>`
- **Buckets**: `day`, `hour`, `week`, or `month`
- **Sharded tables**: works with `events_*` using `_TABLE_SUFFIX` bounds

## API

See docstrings in `src/ga4bigquery/core.py`.

## License

[MIT](LICENSE)
