
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
pip install -e ".[dev]"
```

## Quickstart

```python
from datetime import date, timedelta
from google.cloud import bigquery
from ga4bigquery import GA4BigQuery, FunnelStep

# --- Your GA4 export details ---
PROJECT_ID = "photo-roulette-40b87"
DATASET_ID = "analytics_163566231"
TABLE_ID = "events_*"
TZ = "America/Los_Angeles"
USER_ID_COL = "user_id"  # or 'user_pseudo_id'

client = bigquery.Client()
ga = GA4BigQuery(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    tz=TZ,
    user_id_col=USER_ID_COL,
    client=client,
)

# Active users by version (Android only), last 7 days
active_android = ga.request_events(
    events=["game_started"],
    start=date.today() - timedelta(days=7),
    end=date.today(),
    measure="uniques",
    filters=[{"prop": "platform", "op": "IN", "values": ["ANDROID"]}],
    group_by="app_info.version",
    interval="day",
)
print(active_android.tail())

# New -> Activated within 60 min, grouped by platform
steps = [
    FunnelStep(event_name="continue_from_welcome_screen"),
    FunnelStep(event_name="game_started",
               conversion_window_gt=timedelta(minutes=0),
               conversion_window_lt=timedelta(minutes=60)),
]
funnel = ga.request_funnel(
    steps=steps,
    start=date.today() - timedelta(days=14),
    end=date.today(),
    group_by="platform",
    interval="day",
)
print(funnel.tail())
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
