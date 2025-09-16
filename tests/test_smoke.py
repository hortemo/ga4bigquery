
import pandas as pd

from ga4bigquery import GA4BigQuery, FunnelStep

class _FakeResult:
    def to_dataframe(self):
        # Return minimal DF shape compatible with pivot paths
        return pd.DataFrame({
            "event_date": ["2025-01-01"],
            "event_name": ["test_event"],
            "value": [1],
        })

class _FakeQueryJob:
    def __init__(self):
        self._result = _FakeResult()
    def result(self):
        return self._result

class _FakeClient:
    def query(self, sql: str):
        assert "SELECT" in sql and "FROM" in sql
        return _FakeQueryJob()

def test_request_events_smoke():
    ga = GA4BigQuery(table_id="proj.dataset.events_*", tz="UTC", user_id_col="user_id", client=_FakeClient())
    out = ga.request_events(events=["x"], start=pd.Timestamp("2025-01-01").date(), end=pd.Timestamp("2025-01-01").date())
    assert isinstance(out, (pd.DataFrame, pd.Series))

def test_request_funnel_smoke():
    ga = GA4BigQuery(table_id="proj.dataset.events_*", tz="UTC", user_id_col="user_id", client=_FakeClient())
    steps = [FunnelStep(event_name="a"), FunnelStep(event_name="b")]
    out = ga.request_funnel(steps=steps, start=pd.Timestamp("2025-01-01").date(), end=pd.Timestamp("2025-01-02").date())
    assert isinstance(out, pd.DataFrame)
