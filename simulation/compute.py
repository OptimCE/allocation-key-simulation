"""Pure simulation computation — the math ported from the legacy SimulateurMS.

Stress-tests an existing allocation key against real consumption/production
data and returns per-consumer, per-iteration and key-level metrics. This is a
pure function of ``(key, C, VA, consumer_names)`` — no DB, no network I/O. The
worker loads the data and persists the result.

Ported from ``SimulateurMS/simulateur_module/{simulateur,computation_library}``.
Two intentional clarifications of the legacy behaviour, both documented inline:

* PRORATA weights are computed once from the original consumption and reused
  for a consumer across all iterations. The legacy only persisted the weights
  on iteration 0's consumer objects, so later iterations read an unset default;
  reusing the iteration-0 weights is the behaviour that branch clearly intended.
* The per-timestep ``sharing_rate`` / ``self_sufficiency_rate`` divisions are
  guarded against zero denominators. The legacy guarded only the scalar totals,
  leaving the per-timestep arrays able to emit inf/NaN at zero-production or
  zero-consumption timesteps.

Allocation modes (per consumer, per iteration), from
``energy_allocated_percentage``:
* FIXED (``>= 0``): ``allocated[t] = available_production[t] * pct``
* PRORATA (``== -1``): ``allocated[t] = available_production[t] * share[t]``
  where ``share`` is the consumer's fraction of total consumption.
"""

from __future__ import annotations

import numpy as np

from simulation.inputs import SimulationKeyInput
from simulation.result import ConsumerSimResult, IterationSimResult, KeySimResult


class SimulationInputError(ValueError):
    """Structurally invalid simulation inputs (a deterministic failure)."""


def _relu(x: np.ndarray) -> np.ndarray:
    """``max(0, x)`` element-wise, via the legacy ``(x + |x|) / 2`` form."""
    result: np.ndarray = (x + np.abs(x)) / 2.0
    return result


def _safe_divide(num: np.ndarray, den: np.ndarray) -> np.ndarray:
    """Element-wise division that yields 0 where the denominator is 0."""
    num = np.asarray(num, dtype=np.float64)
    den = np.asarray(den, dtype=np.float64)
    out = np.zeros(np.broadcast_shapes(num.shape, den.shape), dtype=np.float64)
    np.divide(num, den, out=out, where=den != 0)
    return out


def run_simulation(
    key: SimulationKeyInput,
    C: np.ndarray,
    VA: np.ndarray,
    consumer_names: list[str],
) -> KeySimResult:
    """Run the full multi-iteration simulation.

    ``C`` is the consumption matrix ``(n_consumers, T)`` whose rows are aligned
    to ``consumer_names``; ``VA`` is the production matrix ``(n_consumers, T)``
    with the production profile broadcast across every row.
    """
    if not key.iterations:
        raise SimulationInputError("key has no iterations")
    if not consumer_names:
        raise SimulationInputError("no consumers")

    C = np.asarray(C, dtype=np.float64)
    VA = np.asarray(VA, dtype=np.float64)
    n = len(consumer_names)
    if C.ndim != 2 or C.shape[0] != n:
        raise SimulationInputError(
            f"consumption matrix rows {C.shape} do not match consumer count {n}"
        )
    if VA.ndim != 2 or VA.shape[0] < 1 or VA.shape[1] != C.shape[1]:
        raise SimulationInputError(
            f"production matrix shape {VA.shape} incompatible with consumption {C.shape}"
        )

    timesteps = C.shape[1]
    production = VA[0]  # production series (all VA rows are identical)
    total_consumption_per_time = C.sum(axis=0)  # (T,)

    # PRORATA weights: consumer j's share of total consumption at each timestep,
    # computed ONCE from the original consumption and reused across iterations.
    prorata_weights = _safe_divide(C, total_consumption_per_time[None, :])  # (n, T)

    iteration_results: list[IterationSimResult] = []
    prev_residual_matrix: np.ndarray | None = None
    prev_surplus_series: np.ndarray | None = None

    for it in key.iterations:
        name_to_pct = {c.name: c.energy_allocated_percentage for c in it.consumers}
        try:
            pcts = np.array([name_to_pct[name] for name in consumer_names], dtype=np.float64)
        except KeyError as exc:
            raise SimulationInputError(
                f"iteration {it.number} is missing consumer {exc.args[0]!r}"
            ) from exc

        # Production available to this iteration: a fraction of the injection
        # plus (for iteration > 0) the surplus carried forward from the previous.
        if prev_surplus_series is None or prev_residual_matrix is None:
            va_series = production * it.energy_allocated_percentage
            consumption_matrix = C
        else:
            va_series = production * it.energy_allocated_percentage + prev_surplus_series
            consumption_matrix = prev_residual_matrix

        # Per-consumer allocation multiplier: the fixed fraction for FIXED
        # consumers, the prorata weight (per timestep) for PRORATA consumers.
        fixed_mask = pcts > -1
        multiplier = np.where(fixed_mask[:, None], pcts[:, None], prorata_weights)  # (n, T)
        allocated_matrix = va_series[None, :] * multiplier  # (n, T)

        residual_matrix = _relu(consumption_matrix - allocated_matrix)
        surplus_matrix = _relu(allocated_matrix - consumption_matrix)
        consumed_matrix = np.minimum(consumption_matrix, allocated_matrix)

        # Iteration aggregates (sum across consumers per timestep).
        consumption_series = consumption_matrix.sum(axis=0)
        consumed_series = consumed_matrix.sum(axis=0)
        residual_series = residual_matrix.sum(axis=0)
        surplus_series = surplus_matrix.sum(axis=0)

        energy_allocated_total = float(va_series.sum())
        consumed_total = float(consumed_series.sum())
        consumption_total = float(consumption_series.sum())

        # sharing_rate = consumed / available production; 0 if either total is 0.
        if energy_allocated_total == 0 or consumed_total == 0:
            sharing_rate_series = np.zeros(timesteps)
            sharing_rate_total = 0.0
        else:
            sharing_rate_series = _safe_divide(consumed_series, va_series)
            sharing_rate_total = consumed_total / energy_allocated_total

        # self_sufficiency_rate = consumed / consumption; 0 if either total is 0.
        if consumption_total == 0 or consumed_total == 0:
            self_sufficiency_series = np.zeros(timesteps)
            self_sufficiency_total = 0.0
        else:
            self_sufficiency_series = _safe_divide(consumed_series, consumption_series)
            self_sufficiency_total = consumed_total / consumption_total

        consumers_out = [
            ConsumerSimResult(
                name=consumer_names[j],
                energy_allocated_percentage=float(pcts[j]),
                consumption=consumption_matrix[j].tolist(),
                consumption_total=float(consumption_matrix[j].sum()),
                energy_allocated=allocated_matrix[j].tolist(),
                energy_allocated_total=float(allocated_matrix[j].sum()),
                energy_allocated_consumed=consumed_matrix[j].tolist(),
                energy_allocated_consumed_total=float(consumed_matrix[j].sum()),
                residual_volume=residual_matrix[j].tolist(),
                residual_volume_total=float(residual_matrix[j].sum()),
                surplus=surplus_matrix[j].tolist(),
                surplus_total=float(surplus_matrix[j].sum()),
            )
            for j in range(n)
        ]

        iteration_results.append(
            IterationSimResult(
                number=it.number,
                energy_allocated_percentage=it.energy_allocated_percentage,
                consumption=consumption_series.tolist(),
                consumption_total=consumption_total,
                energy_allocated=va_series.tolist(),
                energy_allocated_total=energy_allocated_total,
                energy_allocated_consumed=consumed_series.tolist(),
                energy_allocated_consumed_total=consumed_total,
                residual_volume=residual_series.tolist(),
                residual_volume_total=float(residual_series.sum()),
                surplus=surplus_series.tolist(),
                surplus_total=float(surplus_series.sum()),
                sharing_rate=sharing_rate_series.tolist(),
                sharing_rate_total=sharing_rate_total,
                self_sufficiency_rate=self_sufficiency_series.tolist(),
                self_sufficiency_rate_total=self_sufficiency_total,
                consumers=consumers_out,
            )
        )

        prev_residual_matrix = residual_matrix
        prev_surplus_series = surplus_series

    # Key-level roll-ups (headline metrics):
    #   consumption / available production come from the first iteration;
    #   self-consumption is summed across the cascade; the final surplus and
    #   residual (unmet demand) are taken from the last iteration.
    first = iteration_results[0]
    last = iteration_results[-1]
    consumed_total_all = float(sum(it.energy_allocated_consumed_total for it in iteration_results))
    consumption_total_key = first.consumption_total
    energy_allocated_total_key = first.energy_allocated_total
    self_sufficiency_total_key = (
        consumed_total_all / consumption_total_key if consumption_total_key else 0.0
    )
    sharing_total_key = (
        consumed_total_all / energy_allocated_total_key if energy_allocated_total_key else 0.0
    )

    return KeySimResult(
        name=key.name,
        description=key.description,
        consumption_total=consumption_total_key,
        energy_allocated_total=energy_allocated_total_key,
        energy_allocated_consumed_total=consumed_total_all,
        residual_volume_total=last.residual_volume_total,
        surplus_total=last.surplus_total,
        self_sufficiency_rate_total=self_sufficiency_total_key,
        sharing_rate_total=sharing_total_key,
        iterations=iteration_results,
    )
