"""Reading a run's stored time-series object while storage streams it.

The worker stores a run's per-timestep series as one JSON document
(``worker/timeseries.py``), 70 to 80 bytes per participant x timestep x
iteration: 350 to 400 MB at the size limit (``shared/problem_size.py``).
Parsed into ``SimulationTimeseries`` and re-serialised by FastAPI, it took
about 460 bytes of API memory per cell and 2.4 s per million cells. Neither
reader here holds the document:

* ``as_api_response`` passes it through as the ``data`` of an ``ApiResponse``;
* ``read_iterations`` keeps each iteration's own series, sampled for a chart,
  and skips the per-consumer ones, which are nearly all of the bytes.

Both split the document at its strings (``_pieces``), found with
``bytes.find``. Between two strings there are only numbers and punctuation,
so that is all the parsing it takes to skip the per-consumer series, and to
replace the ``NaN`` and ``Infinity`` that ``json.dumps`` writes for a run
computed from unusable input. JSON has neither, and Starlette refuses to send
them (a 500), so they become ``null``.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import AsyncIterable, AsyncIterator

from api.simulation.schemas import IterationTimeseries

# An iteration's own series: every member but its number and its consumers.
_ITERATION_SERIES = frozenset(IterationTimeseries.model_fields) - {"number", "consumers"}

_STRING_STOP = re.compile(rb'["\\]')

# What json.dumps writes for float("nan"), float("inf") and float("-inf").
_NON_FINITE = (b"-Infinity", b"Infinity", b"NaN")
_NON_FINITE_BYTES = frozenset(b"".join(_NON_FINITE))

_RESPONSE_PREFIX = b'{"data": '
_RESPONSE_SUFFIX = b', "error_code": 0}'
# The pass-through yields at least this much at a time (the last piece aside),
# rather than one piece per string of the document.
_FLUSH_BYTES = 1024 * 1024

_DECODER = json.JSONDecoder()


async def _pieces(chunks: AsyncIterable[bytes]) -> AsyncIterator[tuple[bool, bytes]]:
    """Split a JSON document, read in chunks, into its strings and what lies between them.

    Yields ``(True, string)`` for each string token, quotes included, whole
    even when it spans chunks, and ``(False, run)`` for the bytes between two
    strings as they arrive: a run that spans chunks comes in several pieces.
    """
    string: bytearray | None = None  # the string being read, from its opening quote
    escaped = False  # the string's last byte is a backslash escaping the next one
    async for chunk in chunks:
        position, size = 0, len(chunk)
        while position < size:
            if string is None:
                quote = chunk.find(b'"', position)
                if quote < 0:
                    yield False, chunk[position:]
                    break
                if quote > position:
                    yield False, chunk[position:quote]
                string = bytearray(b'"')
                position = quote + 1
            elif escaped:
                string.append(chunk[position])
                escaped = False
                position += 1
            else:
                stop = _STRING_STOP.search(chunk, position)
                if stop is None:
                    string += chunk[position:]
                    break
                string += chunk[position : stop.end()]
                position = stop.end()
                if stop.group() == b"\\":
                    escaped = True
                else:
                    yield True, bytes(string)
                    string = None
    if string is not None:
        raise ValueError("the stored time series ends inside a string")


def _nulls(run: bytes) -> bytes:
    for token in _NON_FINITE:
        run = run.replace(token, b"null")
    return run


async def as_api_response(chunks: AsyncIterable[bytes]) -> AsyncIterator[bytes]:
    """The stored document as the ``data`` of an ``ApiResponse``.

    Its non-finite numbers become ``null``. Holds about ``_FLUSH_BYTES`` plus
    one chunk at a time, whatever the document's size.
    """
    out = bytearray(_RESPONSE_PREFIX)
    held = b""  # the end of the last run, which may hold the start of a NaN or an Infinity
    async for is_string, piece in _pieces(chunks):
        if is_string:
            out += _nulls(held)
            held = b""
            out += piece
        else:
            run = held + piece
            # Keep back the letters the run ends with: a NaN or an Infinity that
            # the chunk boundary may have cut. Between strings, no other run of
            # these letters is longer than one of them.
            cut = len(run)
            while cut and run[cut - 1] in _NON_FINITE_BYTES:
                cut -= 1
            out += _nulls(run[:cut])
            held = run[cut:]
        if len(out) >= _FLUSH_BYTES:
            yield bytes(out)
            out.clear()
    out += _nulls(held)
    out += _RESPONSE_SUFFIX
    yield bytes(out)


class _IterationReader:
    """Collects each iteration's own series from the pieces of a stored document.

    An iteration is an object two levels deep (the document is the first): a
    string there is one of its keys, and the run after the key up to the next
    string holds the value. Braces never occur between strings except as
    objects, so counting them gives the depth.
    """

    def __init__(self, max_points: int) -> None:
        self.max_points = max_points
        self.stride: int | None = None
        self.depth = 0
        self.iterations: list[dict[str, object]] = []
        self.member: str | None = None  # the iteration member whose value is being read
        self.target: dict[str, object] = {}  # the iteration it belongs to
        self.value: list[bytes] = []  # the run after its key, so far

    def string(self, token: bytes) -> None:
        self._end_member()
        if self.depth == 2 and self.iterations:
            name = json.loads(token)
            if name == "number" or name in _ITERATION_SERIES:
                self.member = name
                self.target = self.iterations[-1]

    def run(self, piece: bytes) -> None:
        if self.member is not None:
            self.value.append(piece)
        start = 0
        while True:
            opening = piece.find(b"{", start)
            closing = piece.find(b"}", start)
            if opening < 0 and closing < 0:
                return
            if closing < 0 or 0 <= opening < closing:
                self.depth += 1
                start = opening + 1
                if self.depth == 2:
                    self.iterations.append({})
            else:
                self.depth -= 1
                start = closing + 1

    def finish(self) -> tuple[list[IterationTimeseries], int]:
        self._end_member()
        iterations = [
            IterationTimeseries.model_validate({**members, "consumers": []})
            for members in self.iterations
        ]
        return iterations, self.stride or 1

    def _end_member(self) -> None:
        if self.member is None:
            return
        name, text = self.member, b"".join(self.value).lstrip()
        self.member, self.value = None, []
        if not text.startswith(b":"):
            return  # a string value, not a key
        value, _end = _DECODER.raw_decode(text[1:].lstrip().decode())
        self.target[name] = value if name == "number" else self._sample(value)

    def _sample(self, values: list[float]) -> list[float | None]:
        # The chart's own sampling: every stride-th timestep from the first.
        if self.stride is None:
            self.stride = max(1, math.ceil(len(values) / self.max_points))
        return [value if math.isfinite(value) else None for value in values[:: self.stride]]


async def read_iterations(
    chunks: AsyncIterable[bytes], max_points: int
) -> tuple[list[IterationTimeseries], int]:
    """Each iteration's own series, every ``stride``-th timestep, at most ``max_points`` each.

    Returns the iterations, with no consumers, and the stride: a series' value
    ``i`` is timestep ``i * stride``. Holds one whole series at a time, whatever
    the number of participants.
    """
    reader = _IterationReader(max_points)
    async for is_string, piece in _pieces(chunks):
        if is_string:
            reader.string(piece)
        else:
            reader.run(piece)
    return reader.finish()
