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
   Jede Zelle dieser Spalten und der Einspeisungsspalte muss eine Zahl enthalten:
   eine leere Zelle lässt den Lauf scheitern (`parse_failed`, mit der Spalte und
   ihrer ersten betroffenen Datenzeile).
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
| `GET` | `/{id}/timeseries` | Die Ergebnis-Zeitreihe pro Zeitschritt abrufen; `?max_points=N` für ein Diagramm (die eigenen Reihen jeder Iteration, ausgedünnt) |
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

## Ressourcenbemessung

Der Worker rechnet in einem Prozesspool, einem Prozess pro zugewiesener CPU minus
eins (die Compose-Dateien geben ihm 2 CPUs, also einen Rechenprozess). Was eine
Simulation kostet, wächst mit ihren **Zellen**: Teilnehmer × Zeitschritte ×
Iterationen. Ihr Ergebnis enthält fünf Werte pro Zelle; eine Viertelstundenreihe
hat 96 Zeitschritte pro Tag.

Gemessen im Worker-Image mit 2 CPUs, an der unten genannten Grenze:

| Eingabe | Zellen | Hauptprozess | Rechenprozess | Container¹ |
|---|---|---|---|---|
| CRM, 142 Zähler × ein Jahr, 1 Iteration | 4.975.680 | 1,25 GB | 1,46 GB | 1,92 GB |
| CRM, derselbe Zeitraum, 47 der Zähler, 3 Iterationen | 4.940.640 | 1,25 GB | 1,23 GB | 1,85 GB |
| 50-MB-CSV (240 Zähler × ein Jahr), 142 davon, 1 Iteration | 4.975.680 | 0,40 GB | 1,46 GB | 1,70 GB |
| dieselbe Datei, 47 der Zähler, 3 Iterationen | 4.940.640 | 0,36 GB | 1,23 GB | 1,69 GB |

¹ Einschließlich des Seitencaches der Ergebnisdatei, 350 bis 400 MB, den der
Kernel zurückholen kann. Jeder Lauf dauerte weniger als 70 Sekunden.

- **Grenze.** Eine Simulation mit mehr als **5.000.000** Zellen wird mit Fehler
  2119 abgelehnt: von der CRM-Vorschau und `POST /from-crm` (bei den anderen
  Blockern, bevor Messwerte gelesen werden), von `POST /` für eine hochgeladene
  Datei (anhand ihrer Zeilen gezählt) und erneut vom Worker auf den geparsten
  Daten. Das sind 142 Zähler über ein Jahr mit einer Iteration oder 47 Zähler
  mit drei. Der Wert steht in `shared/problem_size.py`; erhöhen Sie ihn nur
  zusammen mit dem Speicher des Workers. Das CRM-Lesen selbst bleibt auf
  5.000.000 Messwerte begrenzt (Fehler 2116).
- **Wohin der Speicher geht.** Der Hauptprozess des Workers hält die Eingabe:
  das CRM-Lesen und seinen Pivot, bis zu 1,25 GB bei 5.000.000 Messwerten, oder
  die geparste Datei. Der Rechenprozess hält das Ergebnis. Er schreibt die
  Zeitreihen in eine temporäre JSON-Datei und gibt nur die Summen zurück, und der
  Hauptprozess streamt diese Datei in den Objektspeicher: Das Ergebnis geht nie
  durch ihn hindurch.
- **Containerspeicher.** Die Compose-Dateien des Monorepos und der Produktion
  setzen `mem_limit: 2560m`, mit Swap auf denselben Wert begrenzt. Jeder
  Rechenprozess macht sich zur ersten Wahl des Kernels, wenn dem Container der
  Speicher ausgeht (`oom_score_adj` 1000): Die Berechnung scheitert und wird
  wiederholt, während ein beendeter Hauptprozess den ganzen Worker stoppen würde
  und seine Nachricht alle 10 Minuten zurückkäme, bis zur unten beschriebenen
  Zustellgrenze. Die Bemessung setzt voraus, dass `max_ack_pending` des
  Consumers der Zahl der Rechenprozesse entspricht, wie der Worker es einstellt,
  damit neben einer Berechnung keine zweite Eingabe gelesen wird.
- **Abstürze der Berechnung.** Ein wegen Speichermangels beendeter (oder
  abgestürzter) Rechenprozess bricht den ganzen Pool. Jede Simulation, die er
  berechnete, wird erneut zugestellt; ein Absturz bei ihrer dritten Zustellung
  oder später markiert sie als fehlgeschlagen mit `solver_crashed (likely out of
  memory)`.
- **Zustellgrenze.** Eine Simulation, deren Zustellungen immer wieder ohne
  Ergebnis enden, wird bei ihrer 10. Zustellung mit `delivery_limit_reached`
  als fehlgeschlagen markiert, ohne erneut zu laufen. Das betrifft
  vorübergehende Fehler (Objektspeicher oder CRM-Datenbank nicht erreichbar,
  alle 30 Sekunden wiederholt, sodass die ersten 9 Zustellungen etwa
  viereinhalb Minuten Ausfall überbrücken) und einen Worker, der mitten im Lauf
  gestorben ist oder neu gestartet wurde. Das `max_deliver` des Consumers ist
  11, eine Absicherung, die der Worker beim Start auf den bestehenden Consumer
  anwendet: JetStream stoppt eine Nachricht dort, auch wenn ihr Fehlschlag nicht
  gespeichert werden konnte, und die Simulation bleibt dann ausstehend.
- **Ein Ergebnis zurücklesen.** `GET /{id}/timeseries` leitet das gespeicherte
  Objekt weiter, ohne es zu parsen, mit etwa 20 MB API-Speicher unabhängig von
  seiner Größe, und lehnt ein Objekt über 64 MiB (etwa 750.000 Zellen) mit
  Fehler 2120 ab: das Gateway dekodiert eine JSON-Antwort vollständig, mit etwa
  10 Byte seines Speichers pro Byte. Ein Diagramm fragt stattdessen
  `?max_points=N` an: die eigenen Reihen jeder Iteration, jeden `stride`-ten
  Zeitschritt (in der Antwort angegeben), ohne die Reihen pro Verbraucher. An der
  Grenze liest das das 430-MB-Objekt in unter einer Sekunde und antwortet mit
  einigen hundert kB. Das Objekt zu parsen und neu zu serialisieren, wie es die
  API früher tat, brauchte etwa 470 Byte API-Speicher pro Zelle: 2,4 GB an der
  Grenze.

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
