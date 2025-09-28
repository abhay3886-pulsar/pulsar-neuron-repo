from pulsar_neuron.lib.guard.enforce import enforce_hard_rules


def test_enforce_ok():
    d = {"decision": "take", "side": "long", "entry": 100.0, "sl": 99.0, "target": 102.0}
    ok, errs = enforce_hard_rules(d, {"trades_used": 0})
    assert ok and errs == []


def test_enforce_levels_order():
    d = {"decision": "take", "side": "long", "entry": 100.0, "sl": 101.0, "target": 102.0}
    ok, errs = enforce_hard_rules(d, {"trades_used": 0})
    assert not ok and "levels_order_long" in errs
