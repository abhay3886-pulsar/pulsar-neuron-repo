from pulsar_neuron.lib.guard.session_guard import SessionGuard, SessionStats


def test_can_enter_under_limit():
    guard = SessionGuard(max_trades=3)
    stats = SessionStats(trades_used=2)
    assert guard.can_enter(stats)
