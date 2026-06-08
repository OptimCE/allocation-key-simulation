"""Golden-value tests for the ported simulation math.

Each case is hand-computed so a regression in ``run_simulation`` surfaces here
rather than silently in production numbers.
"""

import numpy as np
import pytest

from shared.data_loading import ConsumerColumnsError, load
from simulation.compute import SimulationInputError, run_simulation
from simulation.inputs import (
    SimulationConsumerInput,
    SimulationIterationInput,
    SimulationKeyInput,
)


def _key(iterations: list[SimulationIterationInput]) -> SimulationKeyInput:
    return SimulationKeyInput(name="k", description="d", iterations=iterations)


def _it(number: int, pct: float, consumers: list[tuple[str, float]]) -> SimulationIterationInput:
    return SimulationIterationInput(
        number=number,
        energy_allocated_percentage=pct,
        consumers=[
            SimulationConsumerInput(name=name, energy_allocated_percentage=p)
            for name, p in consumers
        ],
    )


def _va(production: list[float], n: int) -> np.ndarray:
    return np.tile(np.array(production, dtype=float), (n, 1))


def test_single_iteration_fixed():
    key = _key([_it(1, 1.0, [("A", 0.3), ("B", 0.5), ("C", 0.2)])])
    C = np.array([[160.0], [300.0], [350.0]])
    res = run_simulation(key, C, _va([1000.0], 3), ["A", "B", "C"])
    it = res.iterations[0]

    assert it.consumption_total == pytest.approx(810)
    assert it.energy_allocated_total == pytest.approx(1000)
    assert it.energy_allocated_consumed_total == pytest.approx(660)  # 160+300+200
    assert it.residual_volume_total == pytest.approx(150)  # C unmet
    assert it.surplus_total == pytest.approx(340)  # A 140 + B 200
    assert it.sharing_rate_total == pytest.approx(0.66)  # 660/1000
    assert it.self_sufficiency_rate_total == pytest.approx(660 / 810)

    cons_c = it.consumers[2]
    assert cons_c.energy_allocated_consumed_total == pytest.approx(200)
    assert cons_c.residual_volume_total == pytest.approx(150)
    assert cons_c.surplus_total == pytest.approx(0)

    # Single iteration -> key roll-ups mirror it.
    assert res.consumption_total == pytest.approx(810)
    assert res.energy_allocated_consumed_total == pytest.approx(660)
    assert res.surplus_total == pytest.approx(340)
    assert res.residual_volume_total == pytest.approx(150)
    assert res.sharing_rate_total == pytest.approx(0.66)
    assert res.self_sufficiency_rate_total == pytest.approx(660 / 810)


def test_single_iteration_prorata():
    key = _key([_it(1, 1.0, [("A", -1), ("B", -1)])])
    C = np.array([[200.0], [300.0]])
    res = run_simulation(key, C, _va([1000.0], 2), ["A", "B"])
    it = res.iterations[0]

    assert it.consumers[0].energy_allocated_total == pytest.approx(400)  # 1000*200/500
    assert it.consumers[1].energy_allocated_total == pytest.approx(600)  # 1000*300/500
    assert it.energy_allocated_consumed_total == pytest.approx(500)
    assert it.self_sufficiency_rate_total == pytest.approx(1.0)
    assert it.sharing_rate_total == pytest.approx(0.5)


def test_two_iterations_surplus_feedback():
    key = _key([_it(1, 0.5, [("A", 1.0)]), _it(2, 0.5, [("A", 1.0)])])
    C = np.array([[100.0]])
    res = run_simulation(key, C, _va([1000.0], 1), ["A"])
    it0, it1 = res.iterations

    assert it0.energy_allocated_total == pytest.approx(500)  # 1000*0.5
    assert it0.energy_allocated_consumed_total == pytest.approx(100)
    assert it0.surplus_total == pytest.approx(400)

    # iter1 production = 1000*0.5 + carried surplus 400 = 900; demand = prev residual = 0
    assert it1.energy_allocated_total == pytest.approx(900)
    assert it1.consumption_total == pytest.approx(0)
    assert it1.energy_allocated_consumed_total == pytest.approx(0)
    assert it1.surplus_total == pytest.approx(900)

    assert res.consumption_total == pytest.approx(100)  # from iteration 0
    assert res.energy_allocated_consumed_total == pytest.approx(100)  # summed cascade
    assert res.surplus_total == pytest.approx(900)  # final
    assert res.residual_volume_total == pytest.approx(0)  # final
    assert res.self_sufficiency_rate_total == pytest.approx(1.0)


def test_multi_timestep_fixed():
    key = _key([_it(1, 1.0, [("A", 0.3)])])
    C = np.array([[160.0, 80.0]])
    res = run_simulation(key, C, _va([1000.0, 500.0], 1), ["A"])
    it = res.iterations[0]

    assert it.energy_allocated == pytest.approx([1000.0, 500.0])
    assert it.consumers[0].energy_allocated == pytest.approx([300.0, 150.0])
    assert it.consumers[0].surplus == pytest.approx([140.0, 70.0])
    assert it.energy_allocated_consumed_total == pytest.approx(240)
    assert it.self_sufficiency_rate_total == pytest.approx(1.0)
    assert it.sharing_rate_total == pytest.approx(240 / 1500)


def test_zero_consumption_prorata_no_nan():
    key = _key([_it(1, 1.0, [("A", -1), ("B", -1)])])
    C = np.array([[0.0], [0.0]])
    res = run_simulation(key, C, _va([1000.0], 2), ["A", "B"])
    it = res.iterations[0]

    assert it.self_sufficiency_rate_total == 0.0
    assert it.sharing_rate_total == 0.0
    for cons in it.consumers:
        assert all(np.isfinite(cons.energy_allocated))
        assert cons.energy_allocated_total == pytest.approx(0.0)


def test_consumer_order_independent_of_iteration_listing():
    # Consumers listed in a different order than consumer_names must still align.
    key = _key([_it(1, 1.0, [("B", 0.5), ("A", 0.3)])])
    C = np.array([[160.0], [300.0]])  # row 0 = A, row 1 = B
    res = run_simulation(key, C, _va([1000.0], 2), ["A", "B"])
    by_name = {c.name: c for c in res.iterations[0].consumers}
    assert by_name["A"].energy_allocated_total == pytest.approx(300)  # 1000*0.3
    assert by_name["B"].energy_allocated_total == pytest.approx(500)  # 1000*0.5


def test_empty_iterations_raises():
    with pytest.raises(SimulationInputError):
        run_simulation(_key([]), np.zeros((1, 1)), np.zeros((1, 1)), ["A"])


def test_missing_consumer_in_iteration_raises():
    key = _key([_it(1, 1.0, [("A", 0.5)])])  # no B
    C = np.array([[1.0], [1.0]])
    with pytest.raises(SimulationInputError):
        run_simulation(key, C, _va([1.0], 2), ["A", "B"])


def test_data_loading_by_name_and_roundtrip():
    csv = b"prod,A,B\n1000,200,300\n"
    raw = load(csv, "data.csv", "prod", ["A", "B"])
    assert raw.consumer_names == ["A", "B"]
    assert raw.C.shape == (2, 1)
    res = run_simulation(
        _key([_it(1, 1.0, [("A", -1), ("B", -1)])]), raw.C, raw.VA, raw.consumer_names
    )
    assert res.energy_allocated_consumed_total == pytest.approx(500)


def test_data_loading_missing_consumer_column_raises():
    csv = b"prod,A\n1000,200\n"
    with pytest.raises(ConsumerColumnsError):
        load(csv, "data.csv", "prod", ["A", "B"])
