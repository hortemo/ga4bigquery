# ga4bigquery

`ga4bigquery` is a lightweight helper for working with Google Analytics 4 (GA4) exports stored in BigQuery.

## Installation

```bash
pip install ga4bigquery
```

## Usage

```python
from datetime import date
from ga4bigquery import GA4BigQuery, FunnelStep

ga = GA4BigQuery(table_id="bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*")

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
