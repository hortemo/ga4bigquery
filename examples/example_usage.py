
from datetime import date
from google.cloud import bigquery
from ga4bigquery import GA4BigQuery, FunnelStep

TABLE_ID = "bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*"
TZ = "America/Los_Angeles"
USER_ID_COL = "user_pseudo_id"

client = bigquery.Client()
ga = GA4BigQuery(table_id=TABLE_ID, tz=TZ, user_id_col=USER_ID_COL, client=client)

page_views = ga.request_events(
    events=["page_view"],
    start=date(2020, 11, 1),
    end=date(2020, 11, 7),
    measure="totals",
    group_by="platform",
    interval="day",
)
print(page_views.tail())

steps = [
    FunnelStep(event_name="view_item"),
    FunnelStep(event_name="add_to_cart"),
    FunnelStep(event_name="purchase"),
]
purchase_funnel = ga.request_funnel(
    steps=steps,
    start=date(2020, 11, 1),
    end=date(2020, 11, 7),
    group_by="platform",
    interval="day",
)
print(purchase_funnel.tail())
