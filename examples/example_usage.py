
from datetime import date, timedelta
from google.cloud import bigquery
from ga4bigquery import GA4BigQuery, FunnelStep

TABLE_ID = "photo-roulette-40b87.analytics_163566231.events_*"
TZ = "America/Los_Angeles"
USER_ID_COL = "user_id"

client = bigquery.Client()
ga = GA4BigQuery(table_id=TABLE_ID, tz=TZ, user_id_col=USER_ID_COL, client=client)

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

steps = [
    FunnelStep(event_name="continue_from_welcome_screen"),
    FunnelStep(
        event_name="game_started",
        conversion_window_gt=timedelta(minutes=0),
        conversion_window_lt=timedelta(minutes=60),
    ),
]
new_to_activated = ga.request_funnel(
    steps=steps,
    start=date.today() - timedelta(days=14),
    end=date.today(),
    group_by="platform",
    interval="day",
)
print(new_to_activated.tail())
