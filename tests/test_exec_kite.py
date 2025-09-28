from pulsar_neuron.lib.exec.kite import place


def test_place_dry_run(monkeypatch):
    monkeypatch.setenv("MODE", "DRY_RUN")
    resp = place({"symbol": "NIFTY"})
    assert resp["status"] == "PLACED"
    assert resp["mode"] == "DRY_RUN"
