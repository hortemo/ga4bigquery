# ga4bigquery

`ga4bigquery` is a lightweight helper for working with Google Analytics 4 (GA4) exports stored in
BigQuery. It focuses on the high-level questions analysts and product teams ask most often: "How
many events did we record?" and "How well is our funnel converting?"

## Installation

```bash
pip install ga4bigquery
```

## Features

- Typed, fluent interface for querying GA4 event exports stored in BigQuery.
- First-class support for time zones, date ranges, grouping dimensions, and aggregation intervals.
- Funnel analysis with ordered steps and automatic metric calculation.
- Works with the standard GA4 export schema available in the Google Analytics sample datasets.

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
