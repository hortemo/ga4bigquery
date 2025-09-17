# ga4bigquery

Helper around Google Analytics 4 (GA4) BigQuery exports.

## Installation

```bash
pip install git+https://github.com/hortemo/ga4bigquery.git
```

## Usage

```python
from datetime import date
from google.cloud import bigquery
from ga4bigquery import GA4BigQuery, FunnelStep

ga = GA4BigQuery(
    table_id="bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*",
    tz="America/Los_Angeles",
)

page_views = ga.request_events(
    events=["page_view"],
    start=date(2020, 11, 1),
    end=date(2020, 11, 2),
    measure="totals",
    group_by="platform",
    interval="day",
)

purchase_funnel = ga.request_funnel(
    steps=[
        FunnelStep(event_name="view_item"),
        FunnelStep(event_name="add_to_cart"),
        FunnelStep(event_name="purchase"),
    ],
    start=date(2020, 11, 1),
    end=date(2020, 11, 2),
    group_by="platform",
    interval="day",
)
```

Authentication is handled by `google.cloud.bigquery.Client`.
