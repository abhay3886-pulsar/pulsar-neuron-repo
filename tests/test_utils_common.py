from pulsar_neuron.lib.utils.common import lot_size, round_to_tick


def test_tick_round():
    assert round_to_tick(24150.03) == 24150.0
    assert round_to_tick(24150.04) == 24150.05


def test_lot_size():
    assert lot_size("NIFTY") == 50
    assert lot_size("BANKNIFTY") == 15
