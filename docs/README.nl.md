<p align="center">
  <img src="logo.svg" alt="OptimCE-logo" width="160">
</p>

# OptimCE — Simulation Key

[![Website](https://img.shields.io/badge/Website-optimce.be-2e7d32.svg)](https://www.optimce.be)
[![Licentie](https://img.shields.io/badge/Licentie-Apache%202.0-blue.svg)](../LICENSE)
[![en](https://img.shields.io/badge/lang-en-lightgrey.svg)](../README.md)
[![fr](https://img.shields.io/badge/lang-fr-lightgrey.svg)](README.fr.md)
[![de](https://img.shields.io/badge/lang-de-lightgrey.svg)](README.de.md)
[![nl](https://img.shields.io/badge/lang-nl-43a047.svg)](README.nl.md)

**Simulation Key** is de **simulatie**-microservice voor energiedeling van het
OptimCE-platform. Op basis van een bestaande verdeelsleutel (gelezen uit de
CRM-database) en een geüpload verbruiks-/productiebestand speelt hij de sleutel
opnieuw af op de gemeten gegevens en berekent hij, per verbruiker en per
iteratie, het **overschot**, het **zelfverbruik**, de
**zelfvoorzieningsgraad** en de **deelgraad** — als scalaire totalen en als
tijdreeksen per tijdstap.

OptimCE is een opensourceplatform voor het beheer van hernieuwbare-energie­
gemeenschappen, gebouwd voor de Belgische context van energiedeling. Meer over
het project vindt u op [www.optimce.be](https://www.optimce.be). Deze service
draait normaal gezien als onderdeel van het volledige platform: zie de
[ontwikkelings-monorepo](https://github.com/OptimCE/monorepo), die alle
OptimCE-services samenbrengt en de Docker Compose-omgeving levert om ze samen uit
te voeren.

## Werking

De service bestaat uit twee uitvoerbare onderdelen die uit dezelfde code worden
gebouwd:

- **API** (`main:app`) — een
  [FastAPI](https://fastapi.tiangolo.com/)-HTTP-toepassing. Ze valideert het
  verzoek, slaat het geüploade bestand op, registreert een `PENDING`-uitvoering
  en publiceert een taak.
- **Worker** (`python -m worker.main`) — een consumer op de achtergrond. Hij
  leest de verdeelsleutel, verwerkt het bestand, voert de (CPU-intensieve)
  berekening uit en slaat de resultaten op.

Ze communiceren via **NATS JetStream** (stream `SIMULATIONS`, subject
`optimce.simulation.run`). Geüploade bestanden en resultaatreeksen per tijdstap
worden bewaard in een **S3-compatibele objectopslag** (MinIO in ontwikkeling).
Er worden twee **PostgreSQL**-databases gebruikt: de CRM-database
(alleen-lezenbron voor verdeelsleutels, gemeenschappen en abonnementen) en een
lokale database voor de simulatie-uitvoeringen en hun scalaire resultaten.
Traces, metrieken en logs worden verzonden via **OpenTelemetry**.

Een uitvoering doorloopt het systeem als volgt:

1. `POST` van een verdeelsleutel-id samen met een verbruiks-/productiebestand.
2. De API uploadt het bestand naar de objectopslag, schrijft een
   `PENDING`-uitvoering en publiceert een `optimce.simulation.run`-gebeurtenis.
3. De worker neemt de taak op, koppelt de kolommen van het bestand aan de
   verbruikers van de sleutel op naam, en voert de solver
   (`simulation/compute.py`, een pure functie) uit buiten de event-loop. Elke
   cel van die kolommen en van de injectiekolom moet een getal bevatten: een lege
   cel laat de uitvoering mislukken (`parse_failed`, met de kolom en de eerste
   betrokken gegevensrij).
4. De scalaire totalen worden naar de lokale database geschreven; de reeks per
   tijdstap wordt als JSON-object geüpload. De uitvoering wordt gemarkeerd als
   `SUCCESS` (of `FAILED`).

## Repositorystructuur

| Pad | Beschrijving |
|---|---|
| `api/` | HTTP-laag — health- en simulatieroutes, verzoek-/antwoordschema's, service- en repositorylogica |
| `simulation/` | Pure domeinlogica — de solver (`compute.py`), invoer-/resultaatmodellen, mapping van CRM-sleutels |
| `worker/` | Achtergrondworker — NATS-abonnement, dispatch en persistentie van resultaten |
| `shared/` | Door API en worker gedeelde code — bestanden laden, CRM-repository, datamodellen, hulpfuncties |
| `core/` | Overkoepelende infrastructuur — configuratie, database, queue, opslag, beveiliging, middleware, i18n, tracing, metrieken, logging |
| `tests/` | Testsuite (pytest) |
| `locales/` | Vertaalde API-foutmeldingen (en, fr, nl, de) |
| `scripts/` | Hulpmiddelen — OpenAPI-export en SQL-schema |

## API

Alle eindpunten vereisen authenticatie en een actief gemeenschapsabonnement (zie
[Authenticatie](#authenticatie)). De gateway levert het externe voorvoegsel
`/simulation`; de interne paden zijn dus relatief:

| Methode | Pad | Doel |
|---|---|---|
| `GET` | `/` | Simulaties (gepagineerd) met hun status opsommen |
| `GET` | `/{id}` | Eén simulatie met haar scalaire resultaatboom ophalen |
| `GET` | `/{id}/timeseries` | De resultaatreeks per tijdstap ophalen; `?max_points=N` voor een grafiek (de eigen reeksen van elke iteratie, bemonsterd) |
| `POST` | `/` | Een simulatie starten (`multipart/form-data`: `file`, `name`, `id_key`, `injection_name`) |
| `DELETE` | `/{id}` | Een simulatie en de opgeslagen resultaten verwijderen |

Health-eindpunten worden aangeboden onder `/health`:

| Methode | Pad | Doel |
|---|---|---|
| `GET` | `/health/liveness` | Het proces leeft |
| `GET` | `/health/readiness` | De afhankelijkheden (CRM-database, NATS) zijn bereikbaar |
| `GET` | `/health/health` | Alias van readiness |

De interactieve OpenAPI-documentatie (`/docs`, `/redoc`, `/openapi.json`) is
alleen ingeschakeld wanneer `ENV=local`.

### Authenticatie

De service voert geen eigen aanmelding uit. In het OptimCE-platform
authenticeren een [KrakenD](https://www.krakend.io/)-gateway en
[Keycloak](https://www.keycloak.org/) het verzoek en voegen ze
identiteitsheaders toe (`x-user-id`, `x-community-id`, `x-user-role`,
`x-user-orgs`). Toegang tot de simulatiefunctie is afhankelijk van een actief
gemeenschapsabonnement.

## Aan de slag

### Vereisten

- Docker en Docker Compose (aanbevolen), **of** Python 3.12 voor zelfstandige
  lokale ontwikkeling

### Uitvoeren via de OptimCE-stack (aanbevolen)

Deze service is afhankelijk van NATS, MinIO en PostgreSQL. De eenvoudigste manier
om hem met al zijn afhankelijkheden uit te voeren is de ontwikkelings-monorepo:

```bash
git clone --recurse-submodules https://github.com/OptimCE/monorepo.git
cd monorepo
./docker-stack.sh start
```

De service draait als `simulation-key` en is bereikbaar op hostpoort `8003`:

```bash
curl http://localhost:8003/health/readiness
```

### Zelfstandig uitvoeren

```bash
git clone https://github.com/OptimCE/allocation-key-simulation.git
cd allocation-key-simulation
python -m venv .venv
# Windows: .venv\Scripts\activate  |  Unix: source .venv/bin/activate
pip install -r requirements/testing.txt
cp .env.exemple .env
```

Start de API en de worker (beide hebben bereikbare instanties van NATS, MinIO en
PostgreSQL nodig — de monorepo-stack is de eenvoudigste manier om die te
leveren):

```bash
uvicorn main:app --reload      # API op http://localhost:8000
python -m worker.main          # achtergrondworker
```

## Configuratie

De configuratie wordt uit de omgeving gelezen; `.env.exemple` documenteert elke
variabele. De belangrijkste groepen zijn:

- **CRM-database** (`CRM_DATABASE_URL`, `CRM_DB_*`-poolinstellingen)
- **Lokale database** (`LOCAL_DATABASE_URL`, `LOCAL_DB_*`-poolinstellingen)
- **Berichtenverkeer** (`NATS_URL`)
- **Objectopslag** (`STORAGE_ENDPOINT`, `STORAGE_BUCKET`, `STORAGE_ACCESS_KEY`,
  `STORAGE_SECRET_KEY`, `STORAGE_REGION`)
- **CORS** (`ALLOW_ORIGIN`)
- **Observability** (`LOGGING_TOKEN`, `LOGGING_TRACES_URL`, `LOGGING_LOGS_URL`,
  `LOGGING_METRICS_URL`)
- **Omgevingskeuze** (`ENV`: `local`, `test`, `staging`, `production`)

## Dimensionering van middelen

De worker rekent in een pool van processen, één per toegewezen CPU min één (de
compose-bestanden geven hem 2 CPU's, dus één rekenproces). Wat een simulatie
kost, groeit met haar aantal **cellen**: deelnemers × tijdstappen × iteraties.
Haar resultaat bevat vijf waarden per cel; een kwartierreeks telt 96 tijdstappen
per dag.

Gemeten in de worker-image met 2 CPU's, op de grens hieronder:

| Invoer | Cellen | Hoofdproces | Rekenproces | Container¹ |
|---|---|---|---|---|
| CRM, 142 meters × een jaar, 1 iteratie | 4.975.680 | 1,25 GB | 1,46 GB | 1,92 GB |
| CRM, dezelfde periode, 47 van de meters, 3 iteraties | 4.940.640 | 1,25 GB | 1,23 GB | 1,85 GB |
| CSV van 50 MB (240 meters × een jaar), 142 ervan, 1 iteratie | 4.975.680 | 0,40 GB | 1,46 GB | 1,70 GB |
| hetzelfde bestand, 47 van de meters, 3 iteraties | 4.940.640 | 0,36 GB | 1,23 GB | 1,69 GB |

¹ Inclusief de paginacache van het resultaatbestand, 350 tot 400 MB, die de
kernel kan terugwinnen. Elke run duurde minder dan 70 seconden.

- **Grens.** Een simulatie van meer dan **5.000.000** cellen wordt geweigerd met
  fout 2119: door de CRM-voorvertoning en `POST /from-crm` (bij de andere
  blokkades, voordat er meetwaarden worden gelezen), door `POST /` voor een
  geüpload bestand (geteld aan de hand van zijn rijen), en opnieuw door de worker
  op de ingelezen gegevens. Dat is 142 meters over een jaar met één iteratie, of
  47 meters met drie. De waarde staat in `shared/problem_size.py`; verhoog ze
  alleen samen met het geheugen van de worker. Het lezen uit het CRM zelf blijft
  begrensd op 5.000.000 meetwaarden (fout 2116).
- **Waar het geheugen naartoe gaat.** Het hoofdproces van de worker houdt de
  invoer vast: het lezen uit het CRM en de pivot daarvan, tot 1,25 GB bij
  5.000.000 meetwaarden, of het ingelezen bestand. Het rekenproces houdt het
  resultaat vast. Het schrijft de tijdreeksen naar een tijdelijk JSON-bestand en
  geeft alleen de totalen terug, en het hoofdproces streamt dat bestand naar de
  objectopslag: het resultaat gaat er nooit doorheen.
- **Containergeheugen.** De compose-bestanden van de monorepo en de productie
  zetten `mem_limit: 2560m`, met swap op dezelfde waarde begrensd. Elk
  rekenproces maakt zichzelf de eerste keuze van de kernel wanneer de container
  geen geheugen meer heeft (`oom_score_adj` 1000): de berekening mislukt en wordt
  opnieuw geprobeerd, terwijl een gestopt hoofdproces de hele worker zou
  neerhalen en zijn bericht elke 10 minuten zou terugkomen, tot de
  leveringslimiet hieronder. De dimensionering gaat ervan uit dat
  `max_ack_pending` van de consumer gelijk is aan het aantal rekenprocessen,
  zoals de worker het instelt, zodat er naast een berekening geen tweede invoer
  wordt gelezen.
- **Crashes van de berekening.** Een rekenproces dat wegens geheugengebrek wordt
  gestopt (of crasht) breekt de hele pool. Elke simulatie die het berekende,
  wordt opnieuw geleverd; een crash bij haar derde levering of later markeert ze
  als mislukt met `solver_crashed (likely out of memory)`.
- **Leveringslimiet.** Een simulatie waarvan de leveringen telkens zonder
  resultaat eindigen, wordt bij haar 10e levering als mislukt gemarkeerd met
  `delivery_limit_reached`, zonder opnieuw te lopen. Dat dekt tijdelijke fouten
  (objectopslag of CRM-database onbereikbaar, elke 30 seconden opnieuw
  geprobeerd, zodat de eerste 9 leveringen ongeveer vierenhalve minuut storing
  overbruggen) en een worker die midden in de run stierf of herstartte. De
  `max_deliver` van de consumer is 11, een vangnet dat de worker bij het starten
  op de bestaande consumer toepast: JetStream stopt een bericht daar, ook als de
  mislukking niet kon worden opgeslagen, en de simulatie blijft dan in
  afwachting.
- **Een resultaat teruglezen.** `GET /{id}/timeseries` stuurt het opgeslagen
  object door zonder het in te lezen, met ongeveer 20 MB API-geheugen ongeacht de
  grootte, en weigert een object boven 64 MiB (ongeveer 750.000 cellen) met fout
  2120: de gateway decodeert een JSON-antwoord in zijn geheel, met ongeveer 10
  bytes van zijn geheugen per byte. Een grafiek vraagt in plaats daarvan
  `?max_points=N`: de eigen reeksen van elke iteratie, elke `stride`-de tijdstap
  (vermeld in het antwoord), zonder de reeksen per verbruiker. Op de grens leest
  dat het object van 430 MB in minder dan een seconde en antwoordt het enkele
  honderden kB. Het object inlezen en opnieuw serialiseren, zoals de API vroeger
  deed, kostte ongeveer 470 bytes API-geheugen per cel: 2,4 GB op de grens.

## Testen

De testsuite gebruikt [pytest](https://docs.pytest.org/); een
PostgreSQL-container wordt automatisch gestart via `pytest-docker`:

```bash
pytest             # de testsuite uitvoeren
ruff check .       # linting
ruff format --check .
mypy .             # typecontrole
```

## Internationalisatie

De API-foutmeldingen zijn vertaald onder `locales/` in het **Engels**, **Frans**,
**Nederlands** en **Duits**. De taal van het antwoord wordt bepaald op basis van
de `Accept-Language`-header van het verzoek.

## Bijdragen

Bijdragen zijn welkom! Lees de
[bijdragerichtlijnen](../CONTRIBUTING.md) en onze
[Gedragscode](../CODE_OF_CONDUCT.md) voordat u een issue of pull request opent.

## Beveiliging

Om een beveiligingslek te melden, volg het
[beveiligingsbeleid](../SECURITY.md) — open geen openbaar issue.

## Licentie

Dit project is gelicentieerd onder de [Apache-licentie 2.0](../LICENSE).
