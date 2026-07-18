<p align="center">
  <img src="docs/logo.svg" alt="OptimCE logo" width="160">
</p>

# OptimCE — Simulation Key

[![Website](https://img.shields.io/badge/Website-optimce.be-2e7d32.svg)](https://www.optimce.be/en/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![en](https://img.shields.io/badge/lang-en-43a047.svg)](README.md)
[![fr](https://img.shields.io/badge/lang-fr-lightgrey.svg)](docs/README.fr.md)
[![de](https://img.shields.io/badge/lang-de-lightgrey.svg)](docs/README.de.md)
[![nl](https://img.shields.io/badge/lang-nl-lightgrey.svg)](docs/README.nl.md)

**Simulation Key** is the energy-sharing **simulation** microservice of the
OptimCE platform. Given an existing allocation key (read from the CRM database)
and an uploaded consumption/production file, it replays the key against the
measured data and reports, per consumer and per iteration, the **surplus**,
**self-consumption**, **self-sufficiency rate**, and **sharing rate** — as
scalar totals and as per-timestep time series.

OptimCE is an open-source platform for managing renewable energy communities,
built for the Belgian energy-sharing context. To learn more about the project,
visit [www.optimce.be](https://www.optimce.be/en/). This service is normally run
as part of the full platform: see the
[development monorepo](https://github.com/OptimCE/monorepo), which aggregates all
OptimCE services and provides the Docker Compose environment to run them
together.

## How It Works

The service is split into two deployables built from the same codebase:

- **API** (`main:app`) — a [FastAPI](https://fastapi.tiangolo.com/) HTTP
  application. It validates the request, stores the uploaded file, records a
  `PENDING` run, and publishes a job.
- **Worker** (`python -m worker.main`) — a background consumer. It reads the
  allocation key, parses the file, runs the (CPU-bound) computation, and
  persists the results.

They communicate over **NATS JetStream** (stream `SIMULATIONS`, subject
`optimce.simulation.run`). Uploaded files and per-timestep result series are kept
in an **S3-compatible object store** (MinIO in development). Two **PostgreSQL**
databases are used: the CRM database (read-only source for allocation keys,
communities, and subscriptions) and a local database for simulation runs and
their scalar results. Traces, metrics, and logs are emitted through
**OpenTelemetry**.

A run flows through the system like this:

1. `POST` an allocation-key id plus a consumption/production file.
2. The API uploads the file to object storage, writes a `PENDING` run, and
   publishes a `optimce.simulation.run` event.
3. The worker claims the job, matches the file's columns to the key's consumers
   by name, and runs the solver (`simulation/compute.py`, a pure function) off
   the event loop.
4. Scalar totals are written to the local database; the per-timestep series is
   uploaded as a JSON object. The run is marked `SUCCESS` (or `FAILED`).

## Repository Structure

| Path | Description |
|---|---|
| `api/` | HTTP layer — health and simulation routes, request/response schemas, service and repository logic |
| `simulation/` | Pure domain logic — the solver (`compute.py`), input/result models, CRM key mapping |
| `worker/` | Background worker — NATS subscription, dispatch, and result persistence |
| `shared/` | Code shared by API and worker — file loading, CRM repository, data models, helpers |
| `core/` | Cross-cutting infrastructure — config, database, queue, storage, security, middleware, i18n, tracing, metrics, logging |
| `tests/` | Test suite (pytest) |
| `locales/` | Translated API error messages (en, fr, nl, de) |
| `scripts/` | Utilities — OpenAPI export and SQL schema |

## API

All endpoints require authentication and an active community subscription (see
[Authentication](#authentication)). The gateway supplies the external
`/simulation` prefix, so the in-service paths are relative:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | List simulations (paginated) with their status |
| `GET` | `/{id}` | Get one simulation with its scalar result tree |
| `GET` | `/{id}/timeseries` | Get the per-timestep result series for charting |
| `POST` | `/` | Start a simulation (`multipart/form-data`: `file`, `name`, `id_key`, `injection_name`) |
| `DELETE` | `/{id}` | Delete a simulation and its stored results |

Health endpoints are served under `/health`:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health/liveness` | Process is alive |
| `GET` | `/health/readiness` | Dependencies (CRM database, NATS) are reachable |
| `GET` | `/health/health` | Alias of readiness |

Interactive OpenAPI docs (`/docs`, `/redoc`, `/openapi.json`) are enabled only
when `ENV=local`.

### Authentication

The service performs no login of its own. In the OptimCE platform a
[KrakenD](https://www.krakend.io/) gateway and [Keycloak](https://www.keycloak.org/)
authenticate the request and inject identity headers (`x-user-id`,
`x-community-id`, `x-user-role`, `x-user-orgs`). Access to the simulation
feature is gated by an active community subscription.

## Getting Started

### Prerequisites

- Docker and Docker Compose (recommended), **or** Python 3.12 for standalone
  local development

### Running via the OptimCE Stack (recommended)

This service depends on NATS, MinIO, and PostgreSQL. The simplest way to run it
with everything wired together is the development monorepo:

```bash
git clone --recurse-submodules https://github.com/OptimCE/monorepo.git
cd monorepo
./docker-stack.sh start
```

The service runs as `simulation-key` and is reachable on host port `8003`:

```bash
curl http://localhost:8003/health/readiness
```

### Running Standalone

```bash
git clone https://github.com/OptimCE/allocation-key-simulation.git
cd allocation-key-simulation
python -m venv .venv
# Windows: .venv\Scripts\activate  |  Unix: source .venv/bin/activate
pip install -r requirements/testing.txt
cp .env.exemple .env
```

Start the API and the worker (each still needs reachable NATS, MinIO, and
PostgreSQL — the monorepo stack is the easiest way to provide them):

```bash
uvicorn main:app --reload      # API on http://localhost:8000
python -m worker.main          # background worker
```

## Configuration

Configuration is read from the environment; `.env.exemple` documents every
variable. The main groups are:

- **CRM database** (`CRM_DATABASE_URL`, `CRM_DB_*` pool settings)
- **Local database** (`LOCAL_DATABASE_URL`, `LOCAL_DB_*` pool settings)
- **Messaging** (`NATS_URL`)
- **Object storage** (`STORAGE_ENDPOINT`, `STORAGE_BUCKET`, `STORAGE_ACCESS_KEY`,
  `STORAGE_SECRET_KEY`, `STORAGE_REGION`)
- **CORS** (`ALLOW_ORIGIN`)
- **Observability** (`LOGGING_TOKEN`, `LOGGING_TRACES_URL`, `LOGGING_LOGS_URL`,
  `LOGGING_METRICS_URL`)
- **Environment selector** (`ENV`: `local`, `test`, `staging`, `production`)

## Testing

The test suite uses [pytest](https://docs.pytest.org/); a PostgreSQL container
is started automatically via `pytest-docker`:

```bash
pytest             # run the test suite
ruff check .       # lint
ruff format --check .
mypy .             # type checking
```

## Internationalization

API error messages are translated under `locales/` in **English**, **French**,
**Dutch**, and **German**. The response language is selected from the request's
`Accept-Language` header.

## Contributing

Contributions are welcome! Please read the
[contributing guidelines](CONTRIBUTING.md) and our
[Code of Conduct](CODE_OF_CONDUCT.md) before opening an issue or pull request.

## Security

To report a security vulnerability, please follow the
[security policy](SECURITY.md) — do not open a public issue.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
