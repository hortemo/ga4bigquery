# ga4bigquery

Minimal helper around Google Analytics 4 (GA4) BigQuery exports with focused helpers for
`request_events` and `request_funnel` queries.

## Installation

```bash
pip install git+https://github.com/hortemo/ga4bigquery.git
```

## Usage

```python
from datetime import date
from google.cloud import bigquery
from ga4bigquery import GA4BigQuery, FunnelStep

TABLE_ID = "bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*"
TZ = "America/Los_Angeles"
USER_ID_COL = "user_pseudo_id"

client = bigquery.Client()
ga = GA4BigQuery(table_id=TABLE_ID, tz=TZ, user_id_col=USER_ID_COL, client=client)

# Daily page views by platform
page_views = ga.request_events(
    events=["page_view"],
    start=date(2020, 11, 1),
    end=date(2020, 11, 2),
    measure="totals",
    group_by="platform",
    interval="day",
)

# Multi-step purchase funnel
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
```

Authentication is handled by `google.cloud.bigquery.Client`; Application Default Credentials work out
of the box.

## Features

- Event totals or uniques, optional custom formulas
- Arbitrary funnel steps with per-step windows
- Grouping by raw columns or GA4 parameter keys
- Bucketing by day, hour, week, or month
- Works with sharded `events_*` tables

## License

[MIT](LICENSE)
