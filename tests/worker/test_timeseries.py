"""The time-series JSON the solver process writes (``worker.timeseries``).

The oracle is the serialisation the worker used before the series moved to the
solver process: ``json.dumps`` of the whole payload, kept below as
``_former_bytes``. Objects already in storage were written that way and the API
reads both, so the bytes must not change.
"""

from __future__ import annotations

import dataclasses
import io
import json
import math
import os
import pickle
import tempfile

import numpy as np
import pytest

from api.simulation.schemas import SimulationTimeseries
from simulation.compute import run_simulation
from simulation.inputs import (
    SimulationConsumerInput,
    SimulationIterationInput,
    SimulationKeyInput,
)
from simulation.result import ConsumerSimResult, IterationSimResult, KeySimResult
from worker import timeseries


def _former_bytes(result: KeySimResult) -> bytes:
    """worker/persistence.py::_timeseries_bytes, as it was."""
    payload = {
        "iterations": [
            {
                "number": it.number,
                "consumption": it.consumption,
                "energy_allocated": it.energy_allocated,
                "energy_allocated_consumed": it.energy_allocated_consumed,
                "residual_volume": it.residual_volume,
                "surplus": it.surplus,
                "sharing_rate": it.sharing_rate,
                "self_sufficiency_rate": it.self_sufficiency_rate,
                "consumers": [
                    {
                        "name": c.name,
                        "consumption": c.consumption,
                        "energy_allocated": c.energy_allocated,
                        "energy_allocated_consumed": c.energy_allocated_consumed,
                        "residual_volume": c.residual_volume,
                        "surplus": c.surplus,
                    }
                    for c in it.consumers
                ],
            }
            for it in result.iterations
        ]
    }
    return json.dumps(payload).encode()


def _written(result: KeySimResult) -> bytes:
    out = io.BytesIO()
    timeseries.write_json(result, out)
    return out.getvalue()


def _computed_result(seed: int, *, consumers: int = 4, timesteps: int = 96) -> KeySimResult:
    """A real run: FIXED and PRORATA consumers, three cascading iterations,
    and timesteps with no production or no consumption at all."""
    rng = np.random.default_rng(seed)
    names = [f"5414{j:014d}" for j in range(consumers)]
    consumption = rng.random((consumers, timesteps)) * rng.choice([0.0, 1.0, 1e-9, 7.5], timesteps)
    production = rng.random(timesteps) * 20
    production[::7] = 0.0
    key = SimulationKeyInput(
        name="k",
        description="d",
        iterations=[
            SimulationIterationInput(
                number=number,
                energy_allocated_percentage=share,
                consumers=[
                    SimulationConsumerInput(
                        name=name, energy_allocated_percentage=-1.0 if j % 2 else 1 / consumers
                    )
                    for j, name in enumerate(names)
                ],
            )
            for number, share in ((1, 0.6), (2, 0.3), (3, 0.1))
        ],
    )
    return run_simulation(key, consumption, np.tile(production, (consumers, 1)), names)


def _special_result() -> KeySimResult:
    """Values json.dumps writes in non-obvious ways, and names it must escape."""
    floats = [math.nan, math.inf, -math.inf, -0.0, 0.0, 5e-324, 1e300, 0.1 + 0.2, 1.0, -2.5]
    consumer = ConsumerSimResult(
        name='Zoë "B" \\ ☃',
        energy_allocated_percentage=-1.0,
        consumption=floats,
        consumption_total=1.0,
        energy_allocated=floats[::-1],
        energy_allocated_total=2.0,
        energy_allocated_consumed=[],
        energy_allocated_consumed_total=3.0,
        residual_volume=floats,
        residual_volume_total=4.0,
        surplus=floats,
        surplus_total=5.0,
    )
    iteration = IterationSimResult(
        number=0,
        energy_allocated_percentage=1.0,
        consumption=floats,
        consumption_total=1.0,
        energy_allocated=floats,
        energy_allocated_total=1.0,
        energy_allocated_consumed=floats,
        energy_allocated_consumed_total=1.0,
        residual_volume=floats,
        residual_volume_total=1.0,
        surplus=floats,
        surplus_total=1.0,
        sharing_rate=floats,
        sharing_rate_total=1.0,
        self_sufficiency_rate=floats,
        self_sufficiency_rate_total=1.0,
        consumers=[consumer, dataclasses.replace(consumer, name="plain")],
    )
    return KeySimResult(
        name="k",
        description="d",
        consumption_total=1.0,
        energy_allocated_total=1.0,
        energy_allocated_consumed_total=1.0,
        residual_volume_total=1.0,
        surplus_total=1.0,
        self_sufficiency_rate_total=1.0,
        sharing_rate_total=1.0,
        iterations=[iteration, dataclasses.replace(iteration, number=1, consumers=[])],
    )


def _empty_result() -> KeySimResult:
    return dataclasses.replace(_special_result(), iterations=[])


# ---------------------------------------------------------------------------
# write_json: the same bytes as before
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", range(5))
def test_a_computed_result_is_written_byte_for_byte_as_before(seed):
    result = _computed_result(seed)
    assert _written(result) == _former_bytes(result)


@pytest.mark.parametrize(
    "result",
    [_special_result(), _empty_result()],
    ids=["special-floats-and-names", "no-iterations"],
)
def test_edge_cases_are_written_byte_for_byte_as_before(result):
    assert _written(result) == _former_bytes(result)


def test_the_api_reads_what_is_written():
    result = _computed_result(0)
    parsed = SimulationTimeseries.model_validate_json(_written(result))

    assert [it.number for it in parsed.iterations] == [1, 2, 3]
    assert parsed.iterations[2].surplus == result.iterations[2].surplus
    consumer = parsed.iterations[1].consumers[3]
    assert consumer.name == result.iterations[1].consumers[3].name
    assert consumer.residual_volume == result.iterations[1].consumers[3].residual_volume


# ---------------------------------------------------------------------------
# to_temp_file / remove
# ---------------------------------------------------------------------------


def test_to_temp_file_writes_the_json_to_a_new_file(tmp_path):
    result = _computed_result(1)

    path = timeseries.to_temp_file(result)

    assert os.path.dirname(path) == str(tmp_path)  # tests/worker/conftest.py
    assert os.path.basename(path).startswith(timeseries.TEMP_FILE_PREFIX)
    with open(path, "rb") as written:
        assert written.read() == _former_bytes(result)
    timeseries.remove(path)
    assert not os.path.exists(path)


def test_to_temp_file_leaves_nothing_behind_when_writing_fails(tmp_path):
    result = _computed_result(2)
    unserialisable = dataclasses.replace(
        result.iterations[0], surplus=[object()]
    )  # json.dumps raises TypeError mid-document
    broken = dataclasses.replace(result, iterations=[result.iterations[0], unserialisable])

    with pytest.raises(TypeError):
        timeseries.to_temp_file(broken)

    assert os.listdir(tmp_path) == []


def test_remove_tolerates_a_file_already_gone(tmp_path):
    timeseries.remove(str(tmp_path / "never-written.json"))


def test_remove_leftovers_deletes_only_this_modules_files(tmp_path):
    stale = [timeseries.to_temp_file(_empty_result()) for _ in range(2)]
    unrelated = tmp_path / "other-service.json"
    unrelated.write_text("{}")

    assert timeseries.remove_leftovers() == 2

    assert not any(os.path.exists(path) for path in stale)
    assert unrelated.exists()
    assert tempfile.gettempdir() == str(tmp_path)


# ---------------------------------------------------------------------------
# without_series: what crosses back to the main process
# ---------------------------------------------------------------------------


_ITERATION_SERIES = (
    "consumption",
    "energy_allocated",
    "energy_allocated_consumed",
    "residual_volume",
    "surplus",
    "sharing_rate",
    "self_sufficiency_rate",
)
_CONSUMER_SERIES = _ITERATION_SERIES[:5]


def test_without_series_keeps_every_total_and_drops_every_series():
    result = _computed_result(3)
    expected = dataclasses.asdict(result)
    for iteration in expected["iterations"]:
        iteration.update({name: [] for name in _ITERATION_SERIES})
        for consumer in iteration["consumers"]:
            consumer.update({name: [] for name in _CONSUMER_SERIES})

    stripped = timeseries.without_series(result)

    assert dataclasses.asdict(stripped) == expected


def test_without_series_leaves_the_result_untouched():
    result = _computed_result(4)
    before = _former_bytes(result)

    timeseries.without_series(result)

    assert _former_bytes(result) == before


def test_without_series_is_small():
    # 4 participants x 96 timesteps x 3 iterations: the series are nearly all of it.
    result = _computed_result(0)
    assert len(pickle.dumps(timeseries.without_series(result))) * 20 < len(pickle.dumps(result))
