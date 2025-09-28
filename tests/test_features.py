from pulsar_neuron.lib.features.indicators import vwap_distance


def test_vwap_dist_zero_vwap():
    assert vwap_distance(100.0, 0.0) == 0.0
