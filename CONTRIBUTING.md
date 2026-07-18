# Contributing to OptimCE Simulation Key

Thank you for your interest in contributing! Issues and pull requests are
welcome from everyone. By participating in this project, you agree to abide by
our [Code of Conduct](CODE_OF_CONDUCT.md).

This repository is the **simulation** microservice of the OptimCE platform. It
takes an existing energy-sharing allocation key and stress-tests it against
measured consumption/production data. It is one of several repositories under
the [OptimCE organization](https://github.com/OptimCE); the full platform is
assembled in the [monorepo](https://github.com/OptimCE/monorepo).

## Setting Up a Development Environment

This service depends on NATS (JetStream), an S3-compatible object store
(MinIO), and PostgreSQL. The easiest way to run it with all of its dependencies
is through the **OptimCE development stack**, which wires everything together
with Docker Compose:

```bash
git clone --recurse-submodules https://github.com/OptimCE/monorepo.git
cd monorepo
./docker-stack.sh start
```

In that stack this service runs as `simulation-key` and is reachable on host
port `8003` (see the monorepo README for the full setup).

For working on the service code in isolation, you need **Python 3.12**:

```bash
git clone https://github.com/OptimCE/allocation-key-simulation.git
cd allocation-key-simulation
python -m venv .venv
# Windows: .venv\Scripts\activate  |  Unix: source .venv/bin/activate
pip install -r requirements/testing.txt
cp .env.exemple .env
```

The API and the worker are two entry points from the same codebase:

```bash
uvicorn main:app --reload      # API
python -m worker.main          # background worker
```

Both still need reachable NATS, MinIO, and PostgreSQL instances — the monorepo
stack is the simplest way to provide them.

## Reporting Bugs and Suggesting Features

Open a
[GitHub issue](https://github.com/OptimCE/allocation-key-simulation/issues).
For bugs, include what you did, what you expected, and what happened instead —
logs and reproduction steps help a lot.

For security vulnerabilities, **do not open a public issue**; follow the
[security policy](SECURITY.md) instead.

## Submitting Pull Requests

1. Fork the repository and create a feature branch from `main`.
2. Make your changes. Keep each pull request focused on a single topic.
3. Run the checks below and make sure they pass.
4. Open a pull request against `main`, describing **what** you changed and
   **why**.

### Checks Before Opening a Pull Request

These mirror the continuous integration in `.github/workflows/`:

```bash
pytest            # test suite (spins up PostgreSQL via pytest-docker)
ruff check .      # linting
ruff format --check .
mypy .            # type checking
```

Small documentation fixes are welcome as direct pull requests; for larger
changes, opening an issue first to discuss the approach can save you time.

## Commit Messages

Use short, imperative commit messages, preferably following the
[Conventional Commits](https://www.conventionalcommits.org/) style:

```
feat: add prorata allocation mode to the solver
fix: guard sharing rate against zero production
chore: bump numpy to 1.26.4
docs: document the timeseries endpoint
```

## License

This project is licensed under the [Apache License 2.0](LICENSE). By
contributing, you agree that your contributions will be licensed under the same
license.
