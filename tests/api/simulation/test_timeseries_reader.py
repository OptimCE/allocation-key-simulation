"""api/simulation/timeseries_reader.py against documents the worker writes.

The documents come from ``worker.timeseries.write_json``, the stored format,
fed in chunks of every size from one byte up, so that strings, escapes and
NaN / Infinity tokens are cut at every possible place.
"""

import io
import itertools
import json
import math
from collections.abc import AsyncIterator

import pytest
from pydantic import ValidationError

from api.simulation import timeseries_reader
from api.simulation.schemas import SimulationTimeseries
from core.api_response import ApiResponse
from simulation.result import ConsumerSimResult, IterationSimResult, KeySimResult
from worker.timeseries import write_json

NAN, INF = float("nan"), float("inf")

# Names that look like the document's structure from inside a string.
AWKWARD_NAMES = [
    'C"0',
    "back\\slash\\",
    '{"number": 9, "consumption": [1]}',
    "]}, NaN, -Infinity",
    "Elodie é 中",
    "\\",
]

_CONSUMER_SERIES = (
    "consumption",
    "energy_allocated",
    "energy_allocated_consumed",
    "residual_volume",
    "surplus",
)
_ITERATION_SERIES = (*_CONSUMER_SERIES, "sharing_rate", "self_sufficiency_rate")


def _series(seed: int, length: int) -> list[float]:
    return [round(seed * 0.37 + step * 1.25, 6) for step in range(length)]


def _result(
    timesteps: int = 7, iterations: int = 2, names: list[str] | None = None
) -> KeySimResult:
    names = names if names is not None else AWKWARD_NAMES
    seed = itertools.count()

    def consumer(name: str) -> ConsumerSimResult:
        values = {series: _series(next(seed), timesteps) for series in _CONSUMER_SERIES}
        return ConsumerSimResult(
            name=name,
            energy_allocated_percentage=0.1,
            consumption_total=0.0,
            energy_allocated_total=0.0,
            energy_allocated_consumed_total=0.0,
            residual_volume_total=0.0,
            surplus_total=0.0,
            **values,
        )

    def iteration(number: int) -> IterationSimResult:
        values = {series: _series(next(seed), timesteps) for series in _ITERATION_SERIES}
        return IterationSimResult(
            number=number,
            energy_allocated_percentage=0.5,
            consumption_total=0.0,
            energy_allocated_total=0.0,
            energy_allocated_consumed_total=0.0,
            residual_volume_total=0.0,
            surplus_total=0.0,
            sharing_rate_total=0.0,
            self_sufficiency_rate_total=0.0,
            consumers=[consumer(name) for name in names],
            **values,
        )

    return KeySimResult(
        name="key",
        description="",
        consumption_total=0.0,
        energy_allocated_total=0.0,
        energy_allocated_consumed_total=0.0,
        residual_volume_total=0.0,
        surplus_total=0.0,
        self_sufficiency_rate_total=0.0,
        sharing_rate_total=0.0,
        iterations=[iteration(number) for number in range(1, iterations + 1)],
    )


def _with_non_finite(result: KeySimResult) -> KeySimResult:
    first, last = result.iterations[0], result.iterations[-1]
    first.consumption[0] = NAN
    first.surplus[-1] = INF
    last.sharing_rate[1] = -INF
    first.consumers[0].consumption[2] = NAN
    last.consumers[-1].surplus[-1] = -INF
    return result


def _stored(result: KeySimResult) -> bytes:
    out = io.BytesIO()
    write_json(result, out)
    return out.getvalue()


async def _chunks(content: bytes, size: int) -> AsyncIterator[bytes]:
    for start in range(0, len(content), size):
        yield content[start : start + size]


async def _collect(chunks: AsyncIterator[bytes]) -> bytes:
    return b"".join([chunk async for chunk in chunks])


def _finite_or_none(values: list[float]) -> list[float | None]:
    return [value if math.isfinite(value) else None for value in values]


def _expected_document(result: KeySimResult) -> dict:
    # What json.loads makes of the stored document, non-finite numbers as None.
    def clean(node):
        if isinstance(node, dict):
            return {key: clean(value) for key, value in node.items()}
        if isinstance(node, list):
            return [clean(value) for value in node]
        if isinstance(node, float) and not math.isfinite(node):
            return None
        return node

    document: dict = clean(json.loads(_stored(result)))
    return document


# Every size up to 40 bytes cuts each string, escape and token everywhere.
CHUNK_SIZES = [*range(1, 41), 97, 4096, 1 << 30]


# ---------------------------------------------------------------------------
# as_api_response
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("size", CHUNK_SIZES)
async def test_pass_through_is_the_stored_document_byte_for_byte(size):
    content = _stored(_result())

    out = await _collect(timeseries_reader.as_api_response(_chunks(content, size)))

    assert out == b'{"data": ' + content + b', "error_code": 0}'


@pytest.mark.parametrize("size", CHUNK_SIZES)
async def test_pass_through_writes_null_for_non_finite_numbers_only(size):
    result = _with_non_finite(_result())
    content = _stored(result)
    assert b"NaN" in content and b"-Infinity" in content  # also inside a name

    out = await _collect(timeseries_reader.as_api_response(_chunks(content, size)))

    parsed = json.loads(out, parse_constant=pytest.fail)  # strict JSON: no NaN / Infinity
    assert parsed == {"data": _expected_document(result), "error_code": 0}
    names = [consumer["name"] for consumer in parsed["data"]["iterations"][0]["consumers"]]
    assert names == AWKWARD_NAMES  # "NaN" and "-Infinity" inside a string left alone


async def test_pass_through_is_valid_against_the_response_model():
    content = _stored(_with_non_finite(_result()))

    out = await _collect(timeseries_reader.as_api_response(_chunks(content, 1000)))

    response = ApiResponse[SimulationTimeseries].model_validate_json(out)
    assert response.data.iterations[0].consumption[0] is None
    assert response.data.stride == 1


async def test_pass_through_yields_large_pieces_not_one_per_string():
    content = _stored(_result(timesteps=3, names=[f"C{n}" for n in range(2_000)]))

    pieces = [piece async for piece in timeseries_reader.as_api_response(_chunks(content, 1 << 16))]

    assert len(pieces) <= len(content) // (1024 * 1024) + 2
    assert b"".join(pieces) == b'{"data": ' + content + b', "error_code": 0}'


async def test_pass_through_refuses_a_document_cut_inside_a_string():
    content = _stored(_result())
    cut = content.index(b'"name"') + 3

    with pytest.raises(ValueError, match="inside a string"):
        await _collect(timeseries_reader.as_api_response(_chunks(content[:cut], 5)))


# ---------------------------------------------------------------------------
# read_iterations
# ---------------------------------------------------------------------------


def _sampled(result: KeySimResult, max_points: int) -> tuple[list[dict], int]:
    timesteps = len(result.iterations[0].consumption)
    stride = max(1, math.ceil(timesteps / max_points))
    iterations = [
        {
            "number": iteration.number,
            **{
                series: _finite_or_none(getattr(iteration, series)[::stride])
                for series in _ITERATION_SERIES
            },
            "consumers": [],
        }
        for iteration in result.iterations
    ]
    return iterations, stride


@pytest.mark.parametrize("size", CHUNK_SIZES)
async def test_read_iterations_keeps_each_iterations_own_series(size):
    result = _with_non_finite(_result())

    iterations, stride = await timeseries_reader.read_iterations(
        _chunks(_stored(result), size), max_points=100
    )

    assert stride == 1
    assert [iteration.model_dump() for iteration in iterations] == _sampled(result, 100)[0]


@pytest.mark.parametrize(
    ("timesteps", "max_points", "stride"),
    [(7, 7, 1), (7, 6, 2), (7, 3, 3), (7, 2, 4), (7, 1, 7), (35_040, 1500, 24), (1, 1500, 1)],
)
async def test_read_iterations_samples_like_the_chart(timesteps, max_points, stride):
    # crm-frontend's chart: stride = ceil(length / MAX_POINTS), then index % stride === 0.
    result = _result(timesteps=timesteps, iterations=3, names=["C0", "C1"])

    iterations, got_stride = await timeseries_reader.read_iterations(
        _chunks(_stored(result), 65_536), max_points=max_points
    )

    assert got_stride == stride
    assert [iteration.model_dump() for iteration in iterations] == _sampled(result, max_points)[0]
    assert all(len(iteration.consumption) <= max_points for iteration in iterations)


@pytest.mark.parametrize(
    "dumps",
    [
        lambda doc: json.dumps(doc, separators=(",", ":")),
        lambda doc: json.dumps(doc, indent=2),
        lambda doc: json.dumps(doc, ensure_ascii=False),
    ],
    ids=["compact", "indented", "utf-8"],
)
@pytest.mark.parametrize("size", [1, 3, 64])
async def test_read_iterations_does_not_depend_on_layout(dumps, size):
    result = _result()
    document = json.loads(_stored(result))
    # Members in another order: consumers first, the number last.
    for index, iteration in enumerate(document["iterations"]):
        number = iteration.pop("number")
        document["iterations"][index] = {
            "consumers": iteration.pop("consumers"),
            **iteration,
            "number": number,
        }
    content = dumps(document).encode()

    iterations, stride = await timeseries_reader.read_iterations(
        _chunks(content, size), max_points=4
    )

    assert stride == 2
    assert [iteration.model_dump() for iteration in iterations] == _sampled(result, 4)[0]


async def test_read_iterations_with_no_consumers_and_no_iterations():
    empty_consumers = _result(names=[])
    iterations, _ = await timeseries_reader.read_iterations(
        _chunks(_stored(empty_consumers), 5), max_points=10
    )
    assert [iteration.model_dump() for iteration in iterations] == _sampled(empty_consumers, 10)[0]

    iterations, stride = await timeseries_reader.read_iterations(
        _chunks(b'{"iterations": []}', 5), max_points=10
    )
    assert (iterations, stride) == ([], 1)


async def test_read_iterations_refuses_an_incomplete_iteration():
    document = json.loads(_stored(_result()))
    del document["iterations"][1]["surplus"]

    with pytest.raises(ValidationError, match="surplus"):
        await timeseries_reader.read_iterations(
            _chunks(json.dumps(document).encode(), 50), max_points=10
        )
