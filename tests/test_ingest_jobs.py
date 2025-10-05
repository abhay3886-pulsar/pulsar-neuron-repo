from pulsar_neuron.ingest import fut_oi_job, market_job, options_job


def test_mock_jobs_run(monkeypatch):
    """Ensure the mock ingestion jobs run without raising exceptions."""

    monkeypatch.setattr(fut_oi_job, "upsert_many", lambda rows: None)
    monkeypatch.setattr(options_job, "upsert_many", lambda rows: None)
    monkeypatch.setattr(market_job, "upsert_one", lambda row: None)

    fut_oi_job.run("mock")
    options_job.run("mock")
    market_job.run("mock")
