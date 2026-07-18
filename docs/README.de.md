<p align="center">
  <img src="logo.svg" alt="OptimCE-Logo" width="160">
</p>

# OptimCE — Simulation Key

[![Website](https://img.shields.io/badge/Website-optimce.be-2e7d32.svg)](https://www.optimce.be)
[![Lizenz](https://img.shields.io/badge/Lizenz-Apache%202.0-blue.svg)](../LICENSE)
[![en](https://img.shields.io/badge/lang-en-lightgrey.svg)](../README.md)
[![fr](https://img.shields.io/badge/lang-fr-lightgrey.svg)](README.fr.md)
[![de](https://img.shields.io/badge/lang-de-43a047.svg)](README.de.md)
[![nl](https://img.shields.io/badge/lang-nl-lightgrey.svg)](README.nl.md)

**Simulation Key** ist der **Simulations**-Microservice für das Teilen von
Energie in der OptimCE-Plattform. Ausgehend von einem vorhandenen
Aufteilungsschlüssel (aus der CRM-Datenbank gelesen) und einer hochgeladenen
Verbrauchs-/Erzeugungsdatei spielt er den Schlüssel gegen die gemessenen Daten
durch und berechnet je Verbraucher und je Iteration den **Überschuss**, den
**Eigenverbrauch**, die **Autarkierate** und die **Teilungsrate** — als skalare
Summen und als Zeitreihen pro Zeitschritt.

OptimCE ist eine Open-Source-Plattform zur Verwaltung von
Erneuerbare-Energie-Gemeinschaften, konzipiert für den belgischen Kontext des
Energieteilens. Mehr über das Projekt erfahren Sie auf
[www.optimce.be](https://www.optimce.be). Dieser Dienst wird normalerweise als
Teil der Gesamtplattform betrieben: siehe das
[Entwicklungs-Monorepo](https://github.com/OptimCE/monorepo), das alle
OptimCE-Dienste bündelt und die Docker-Compose-Umgebung bereitstellt, um sie
gemeinsam auszuführen.

## Funktionsweise

Der Dienst besteht aus zwei aus demselben Code erstellten Anwendungen:

- **API** (`main:app`) — eine
  [FastAPI](https://fastapi.tiangolo.com/)-HTTP-Anwendung. Sie validiert die
  Anfrage, speichert die hochgeladene Datei, legt einen `PENDING`-Lauf an und
  veröffentlicht einen Auftrag.
- **Worker** (`python -m worker.main`) — ein Hintergrund-Consumer. Er liest den
  Aufteilungsschlüssel, verarbeitet die Datei, führt die (CPU-intensive)
  Berechnung aus und speichert die Ergebnisse.

Sie kommunizieren über **NATS JetStream** (Stream `SIMULATIONS`, Subject
`optimce.simulation.run`). Hochgeladene Dateien und Ergebnis-Zeitreihen pro
Zeitschritt werden in einem **S3-kompatiblen Objektspeicher** (MinIO in der
Entwicklung) abgelegt. Zwei **PostgreSQL**-Datenbanken werden verwendet: die
CRM-Datenbank (schreibgeschützte Quelle für Aufteilungsschlüssel,
Gemeinschaften und Abonnements) und eine lokale Datenbank für die
Simulationsläufe und ihre skalaren Ergebnisse. Traces, Metriken und Logs werden
über **OpenTelemetry** ausgegeben.

Ein Lauf durchläuft das System wie folgt:

1. `POST` einer Aufteilungsschlüssel-ID zusammen mit einer
   Verbrauchs-/Erzeugungsdatei.
2. Die API lädt die Datei in den Objektspeicher hoch, schreibt einen
   `PENDING`-Lauf und veröffentlicht ein `optimce.simulation.run`-Ereignis.
3. Der Worker übernimmt den Auftrag, ordnet die Spalten der Datei den
   Verbrauchern des Schlüssels über den Namen zu und führt den Solver
   (`simulation/compute.py`, eine reine Funktion) außerhalb der Event-Loop aus.
4. Die skalaren Summen werden in die lokale Datenbank geschrieben; die Zeitreihe
   pro Zeitschritt wird als JSON-Objekt hochgeladen. Der Lauf wird als `SUCCESS`
   (oder `FAILED`) markiert.

## Projektstruktur

| Pfad | Beschreibung |
|---|---|
| `api/` | HTTP-Schicht — Health- und Simulations-Routen, Anfrage-/Antwort-Schemas, Service- und Repository-Logik |
| `simulation/` | Reine Domänenlogik — der Solver (`compute.py`), Eingabe-/Ergebnismodelle, Mapping der CRM-Schlüssel |
| `worker/` | Hintergrund-Worker — NATS-Abonnement, Dispatch und Ergebnis-Persistenz |
| `shared/` | Von API und Worker gemeinsam genutzter Code — Dateiladen, CRM-Repository, Datenmodelle, Hilfsfunktionen |
| `core/` | Querschnittsinfrastruktur — Konfiguration, Datenbank, Queue, Speicher, Sicherheit, Middleware, i18n, Tracing, Metriken, Logging |
| `tests/` | Testsuite (pytest) |
| `locales/` | Übersetzte API-Fehlermeldungen (en, fr, nl, de) |
| `scripts/` | Werkzeuge — OpenAPI-Export und SQL-Schema |

## API

Alle Endpunkte erfordern eine Authentifizierung und ein aktives
Gemeinschaftsabonnement (siehe [Authentifizierung](#authentifizierung)). Das
Gateway stellt das externe Präfix `/simulation` bereit; die dienstinternen Pfade
sind daher relativ:

| Methode | Pfad | Zweck |
|---|---|---|
| `GET` | `/` | Simulationen (paginiert) mit ihrem Status auflisten |
| `GET` | `/{id}` | Eine Simulation mit ihrem skalaren Ergebnisbaum abrufen |
| `GET` | `/{id}/timeseries` | Die Ergebnis-Zeitreihe pro Zeitschritt für Diagramme abrufen |
| `POST` | `/` | Eine Simulation starten (`multipart/form-data`: `file`, `name`, `id_key`, `injection_name`) |
| `DELETE` | `/{id}` | Eine Simulation und ihre gespeicherten Ergebnisse löschen |

Health-Endpunkte werden unter `/health` bereitgestellt:

| Methode | Pfad | Zweck |
|---|---|---|
| `GET` | `/health/liveness` | Der Prozess läuft |
| `GET` | `/health/readiness` | Die Abhängigkeiten (CRM-Datenbank, NATS) sind erreichbar |
| `GET` | `/health/health` | Alias für readiness |

Die interaktive OpenAPI-Dokumentation (`/docs`, `/redoc`, `/openapi.json`) ist
nur aktiviert, wenn `ENV=local`.

### Authentifizierung

Der Dienst führt keine eigene Anmeldung durch. In der OptimCE-Plattform
authentifizieren ein [KrakenD](https://www.krakend.io/)-Gateway und
[Keycloak](https://www.keycloak.org/) die Anfrage und fügen Identitäts-Header
(`x-user-id`, `x-community-id`, `x-user-role`, `x-user-orgs`) hinzu. Der Zugriff
auf die Simulationsfunktion ist an ein aktives Gemeinschaftsabonnement gebunden.

## Erste Schritte

### Voraussetzungen

- Docker und Docker Compose (empfohlen), **oder** Python 3.12 für eigenständige
  lokale Entwicklung

### Ausführung über den OptimCE-Stack (empfohlen)

Dieser Dienst benötigt NATS, MinIO und PostgreSQL. Am einfachsten führen Sie ihn
mit all seinen Abhängigkeiten über das Entwicklungs-Monorepo aus:

```bash
git clone --recurse-submodules https://github.com/OptimCE/monorepo.git
cd monorepo
./docker-stack.sh start
```

Der Dienst läuft als `simulation-key` und ist über den Host-Port `8003`
erreichbar:

```bash
curl http://localhost:8003/health/readiness
```

### Eigenständige Ausführung

```bash
git clone https://github.com/OptimCE/allocation-key-simulation.git
cd allocation-key-simulation
python -m venv .venv
# Windows: .venv\Scripts\activate  |  Unix: source .venv/bin/activate
pip install -r requirements/testing.txt
cp .env.exemple .env
```

Starten Sie die API und den Worker (beide benötigen erreichbare Instanzen von
NATS, MinIO und PostgreSQL — der Monorepo-Stack ist der einfachste Weg, diese
bereitzustellen):

```bash
uvicorn main:app --reload      # API unter http://localhost:8000
python -m worker.main          # Hintergrund-Worker
```

## Konfiguration

Die Konfiguration wird aus der Umgebung gelesen; `.env.exemple` dokumentiert jede
Variable. Die wichtigsten Gruppen sind:

- **CRM-Datenbank** (`CRM_DATABASE_URL`, `CRM_DB_*`-Pool-Einstellungen)
- **Lokale Datenbank** (`LOCAL_DATABASE_URL`, `LOCAL_DB_*`-Pool-Einstellungen)
- **Messaging** (`NATS_URL`)
- **Objektspeicher** (`STORAGE_ENDPOINT`, `STORAGE_BUCKET`, `STORAGE_ACCESS_KEY`,
  `STORAGE_SECRET_KEY`, `STORAGE_REGION`)
- **CORS** (`ALLOW_ORIGIN`)
- **Observability** (`LOGGING_TOKEN`, `LOGGING_TRACES_URL`, `LOGGING_LOGS_URL`,
  `LOGGING_METRICS_URL`)
- **Umgebungsauswahl** (`ENV`: `local`, `test`, `staging`, `production`)

## Tests

Die Testsuite verwendet [pytest](https://docs.pytest.org/); ein
PostgreSQL-Container wird automatisch über `pytest-docker` gestartet:

```bash
pytest             # Testsuite ausführen
ruff check .       # Linting
ruff format --check .
mypy .             # Typprüfung
```

## Internationalisierung

Die API-Fehlermeldungen sind unter `locales/` auf **Englisch**, **Französisch**,
**Niederländisch** und **Deutsch** übersetzt. Die Sprache der Antwort wird aus
dem `Accept-Language`-Header der Anfrage bestimmt.

## Mitwirken

Beiträge sind willkommen! Bitte lesen Sie die
[Richtlinien für Beiträge](../CONTRIBUTING.md) und unseren
[Verhaltenskodex](../CODE_OF_CONDUCT.md), bevor Sie ein Issue oder einen Pull
Request eröffnen.

## Sicherheit

Um eine Sicherheitslücke zu melden, folgen Sie bitte der
[Sicherheitsrichtlinie](../SECURITY.md) — öffnen Sie kein öffentliches Issue.

## Lizenz

Dieses Projekt steht unter der [Apache-Lizenz 2.0](../LICENSE).
