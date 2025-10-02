"""Strategy checklist skeleton tests."""

from __future__ import annotations

from pulsar_neuron.lib.schemas import StrategyCheck, Verdict
from pulsar_neuron.strategies import s_orb, s_trend_cont, s_vwap_reclaim


def _assert_stub(strategy_check: StrategyCheck) -> None:
    assert isinstance(strategy_check, StrategyCheck)
    assert strategy_check.score == 0
    assert strategy_check.overall == Verdict.mixed
    assert strategy_check.preconditions
    assert strategy_check.confirmers
    assert strategy_check.blockers


def test_orb_checklist() -> None:
    _assert_stub(s_orb.checklist({}))


def test_vwap_reclaim_checklist() -> None:
    _assert_stub(s_vwap_reclaim.checklist({}))


def test_trend_cont_checklist() -> None:
    _assert_stub(s_trend_cont.checklist({}))
