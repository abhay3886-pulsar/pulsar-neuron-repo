from pulsar_neuron.agentic.index_agent.brain import BrainConfig, IndexBrain


def test_brain_init_state():
    brain = IndexBrain(BrainConfig())
    assert brain.state.value == "IDLE"
