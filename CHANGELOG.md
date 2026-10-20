# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `GET /{id}/timeseries?max_points=N` (1 to 10,000) answers each iteration's own
  series, every `stride`-th timestep so that each has at most N values (new
  `stride` field, 1 otherwise), without the per-consumer series. It reads the
  stored object as it streams in, skipping the per-consumer series: under a
  second and a few hundred kB at the 5,000,000-cell limit, where the whole
  object is 350 to 430 MB. The web app's chart asks for this.

### Changed

- `GET /{id}/timeseries` streams the stored object instead of parsing it into
  the response model and serialising it again: about 20 MB of API memory
  whatever its size, where it took about 470 bytes per cell (2.4 GB and 26 s at
  the 5,000,000-cell limit, measured in the API image). An object over 64 MiB
  (about 750,000 cells) is refused with error 2120: the gateway decodes a JSON
  response whole, at about 10 bytes of its memory per byte (a 430 MB object
  took it past 2 GB), and its 3 s timeout used to stop those with a 500. Series
  values may now be `null` (see Fixed).

- A simulation may now hold at most 5,000,000 cells, participants × timesteps ×
  iterations: 142 meters over a year with one iteration, or 47 with three. A
  larger one is refused with error 2119 by the CRM preview, `POST /from-crm` and
  `POST /`, before anything is queued, and by the worker on the parsed data.
  Larger runs used to be queued and could exhaust the worker's memory, and their
  results were too large for `GET /{id}/timeseries` to serve.

### Fixed

- An uploaded file with an empty cell in a consumer or the injection column made
  every total of the run NaN (pandas reads an empty cell as NaN; `inf` and `nan`
  cells did the same). The run still ended `SUCCESS`, and `GET /{id}` and
  `GET /{id}/timeseries` then answered 500, since JSON has no NaN. The worker now
  fails such a run with `parse_failed`, naming the column, how many such cells
  it has and the first data row. A non-finite CRM reading fails the same way
  (`crm_pivot_failed`).
- `GET /{id}/timeseries` sends the NaN and Infinity stored by such a run as
  `null` instead of failing with a 500.
- A simulation too large for the worker's memory no longer takes the whole
  worker down, to come back after `ack_wait` with no limit on its deliveries.
  The worker's main process used to receive the whole result from the solver
  process and serialise its time series with one `json.dumps`: over 5 GB for 142
  meters × a year × 3 iterations, with the event loop held for half a minute.
  Reading 5,000,000 CRM readings alone took 2.4 GB. Now the solver process
  writes the time series to a temporary file that the worker streams to storage,
  the CRM read needs half the memory, and when the container runs out of memory
  the kernel kills a solver process first, whose crash is retried a bounded
  number of times. The stored time-series object is unchanged, byte for byte.
- A simulation still running when the consumer's `ack_wait` expired (10
  minutes, or 2 minutes on a consumer created before that was raised, such as
  the dev one) was delivered again and run a second time, once the solver slot
  freed up. The worker now tells JetStream every 20 seconds that the simulation
  is still in progress, including while it waits for a free solver process.
- A solver process that is OOM-killed or crashes no longer makes the
  simulation loop forever. The crash was redelivered on every delivery without
  limit, a full run each time, holding up the queue. It is still retried, since
  the crash breaks the whole pool; a crash on a simulation's third delivery or
  later marks it failed with `solver_crashed (likely out of memory)`.
- The worker's 10-minute `ack_wait` and its `max_ack_pending` (the solver pool
  size) now reach the JetStream consumer. nats-py applies them only when it
  creates the durable, so a consumer created before they were introduced, such
  as the dev one, kept a 2-minute `ack_wait` and let JetStream hand out 1000
  simulations at once. The worker now updates the durable in place when it
  starts and logs the old and new values.
- A simulation whose deliveries keep ending without a result is no longer
  delivered forever. A transient failure (object storage or the CRM database
  unreachable, a failed write of the result) was retried every 30 seconds
  without limit, and a worker that died or was restarted mid-run got the
  simulation back every 10 minutes, also without limit. The 10th delivery now
  marks the simulation failed with `delivery_limit_reached` without running it
  again, so an outage longer than about four and a half minutes fails the
  simulations waiting on it. As a backstop, the `worker-simulation` consumer now
  has `max_deliver` 11 instead of unlimited, set in place on the existing
  consumer when the worker starts.
