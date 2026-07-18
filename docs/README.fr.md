<p align="center">
  <img src="logo.svg" alt="Logo OptimCE" width="160">
</p>

# OptimCE — Simulation Key

[![Site web](https://img.shields.io/badge/Site%20web-optimce.be-2e7d32.svg)](https://www.optimce.be)
[![Licence](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](../LICENSE)
[![en](https://img.shields.io/badge/lang-en-lightgrey.svg)](../README.md)
[![fr](https://img.shields.io/badge/lang-fr-43a047.svg)](README.fr.md)
[![de](https://img.shields.io/badge/lang-de-lightgrey.svg)](README.de.md)
[![nl](https://img.shields.io/badge/lang-nl-lightgrey.svg)](README.nl.md)

**Simulation Key** est le microservice de **simulation** de partage d'énergie de
la plateforme OptimCE. À partir d'une clé de répartition existante (lue dans la
base de données du CRM) et d'un fichier de consommation/production téléversé, il
rejoue la clé sur les données mesurées et calcule, par consommateur et par
itération, le **surplus**, l'**autoconsommation**, le **taux
d'autosuffisance** et le **taux de partage** — sous forme de totaux scalaires et
de séries temporelles par pas de temps.

OptimCE est une plateforme open source de gestion des communautés d'énergie
renouvelable, conçue pour le contexte belge du partage d'énergie. Pour en savoir
plus sur le projet, consultez [www.optimce.be](https://www.optimce.be). Ce
service est normalement exécuté au sein de la plateforme complète : voir le
[monorepo de développement](https://github.com/OptimCE/monorepo), qui agrège
tous les services OptimCE et fournit l'environnement Docker Compose pour les
exécuter ensemble.

## Fonctionnement

Le service se compose de deux exécutables construits à partir du même code :

- **API** (`main:app`) — une application HTTP
  [FastAPI](https://fastapi.tiangolo.com/). Elle valide la requête, stocke le
  fichier téléversé, enregistre une exécution `PENDING` et publie une tâche.
- **Worker** (`python -m worker.main`) — un consommateur en arrière-plan. Il lit
  la clé de répartition, analyse le fichier, exécute le calcul (gourmand en
  CPU) et enregistre les résultats.

Ils communiquent via **NATS JetStream** (flux `SIMULATIONS`, sujet
`optimce.simulation.run`). Les fichiers téléversés et les séries de résultats par
pas de temps sont conservés dans un **stockage objet compatible S3** (MinIO en
développement). Deux bases de données **PostgreSQL** sont utilisées : la base du
CRM (source en lecture seule des clés de répartition, communautés et
abonnements) et une base locale pour les exécutions de simulation et leurs
résultats scalaires. Les traces, métriques et journaux sont émis via
**OpenTelemetry**.

Une exécution traverse le système ainsi :

1. `POST` d'un identifiant de clé de répartition et d'un fichier de
   consommation/production.
2. L'API téléverse le fichier vers le stockage objet, écrit une exécution
   `PENDING` et publie un événement `optimce.simulation.run`.
3. Le worker prend la tâche, associe les colonnes du fichier aux consommateurs
   de la clé par leur nom, et exécute le solveur (`simulation/compute.py`, une
   fonction pure) en dehors de la boucle d'événements.
4. Les totaux scalaires sont écrits dans la base locale ; la série par pas de
   temps est téléversée comme objet JSON. L'exécution est marquée `SUCCESS` (ou
   `FAILED`).

## Structure du dépôt

| Chemin | Description |
|---|---|
| `api/` | Couche HTTP — routes de santé et de simulation, schémas requête/réponse, logique de service et de dépôt |
| `simulation/` | Logique métier pure — le solveur (`compute.py`), les modèles d'entrée/résultat, le mappage des clés du CRM |
| `worker/` | Worker en arrière-plan — abonnement NATS, répartition et persistance des résultats |
| `shared/` | Code partagé par l'API et le worker — chargement de fichiers, dépôt CRM, modèles de données, utilitaires |
| `core/` | Infrastructure transversale — configuration, base de données, file d'attente, stockage, sécurité, middleware, i18n, traçage, métriques, journalisation |
| `tests/` | Suite de tests (pytest) |
| `locales/` | Messages d'erreur de l'API traduits (en, fr, nl, de) |
| `scripts/` | Utilitaires — export OpenAPI et schéma SQL |

## API

Tous les points d'accès nécessitent une authentification et un abonnement actif
à la communauté (voir [Authentification](#authentification)). La passerelle
fournit le préfixe externe `/simulation` ; les chemins internes sont donc
relatifs :

| Méthode | Chemin | Objet |
|---|---|---|
| `GET` | `/` | Lister les simulations (paginées) avec leur statut |
| `GET` | `/{id}` | Obtenir une simulation avec son arbre de résultats scalaires |
| `GET` | `/{id}/timeseries` | Obtenir la série de résultats par pas de temps pour les graphiques |
| `POST` | `/` | Démarrer une simulation (`multipart/form-data` : `file`, `name`, `id_key`, `injection_name`) |
| `DELETE` | `/{id}` | Supprimer une simulation et ses résultats stockés |

Les points d'accès de santé sont servis sous `/health` :

| Méthode | Chemin | Objet |
|---|---|---|
| `GET` | `/health/liveness` | Le processus est vivant |
| `GET` | `/health/readiness` | Les dépendances (base CRM, NATS) sont accessibles |
| `GET` | `/health/health` | Alias de readiness |

La documentation OpenAPI interactive (`/docs`, `/redoc`, `/openapi.json`) n'est
activée que lorsque `ENV=local`.

### Authentification

Le service n'effectue aucune connexion propre. Dans la plateforme OptimCE, une
passerelle [KrakenD](https://www.krakend.io/) et
[Keycloak](https://www.keycloak.org/) authentifient la requête et injectent des
en-têtes d'identité (`x-user-id`, `x-community-id`, `x-user-role`,
`x-user-orgs`). L'accès à la fonctionnalité de simulation est conditionné par un
abonnement actif à la communauté.

## Démarrage

### Prérequis

- Docker et Docker Compose (recommandé), **ou** Python 3.12 pour un
  développement local autonome

### Exécution via la stack OptimCE (recommandé)

Ce service dépend de NATS, MinIO et PostgreSQL. Le plus simple pour l'exécuter
avec toutes ses dépendances est le monorepo de développement :

```bash
git clone --recurse-submodules https://github.com/OptimCE/monorepo.git
cd monorepo
./docker-stack.sh start
```

Le service tourne sous le nom `simulation-key` et est accessible sur le port
hôte `8003` :

```bash
curl http://localhost:8003/health/readiness
```

### Exécution autonome

```bash
git clone https://github.com/OptimCE/allocation-key-simulation.git
cd allocation-key-simulation
python -m venv .venv
# Windows : .venv\Scripts\activate  |  Unix : source .venv/bin/activate
pip install -r requirements/testing.txt
cp .env.exemple .env
```

Démarrez l'API et le worker (chacun a besoin d'instances accessibles de NATS,
MinIO et PostgreSQL — la stack du monorepo est le moyen le plus simple de les
fournir) :

```bash
uvicorn main:app --reload      # API sur http://localhost:8000
python -m worker.main          # worker en arrière-plan
```

## Configuration

La configuration est lue depuis l'environnement ; `.env.exemple` documente
chaque variable. Les principaux groupes sont :

- **Base de données CRM** (`CRM_DATABASE_URL`, réglages de pool `CRM_DB_*`)
- **Base de données locale** (`LOCAL_DATABASE_URL`, réglages de pool `LOCAL_DB_*`)
- **Messagerie** (`NATS_URL`)
- **Stockage objet** (`STORAGE_ENDPOINT`, `STORAGE_BUCKET`, `STORAGE_ACCESS_KEY`,
  `STORAGE_SECRET_KEY`, `STORAGE_REGION`)
- **CORS** (`ALLOW_ORIGIN`)
- **Observabilité** (`LOGGING_TOKEN`, `LOGGING_TRACES_URL`, `LOGGING_LOGS_URL`,
  `LOGGING_METRICS_URL`)
- **Sélecteur d'environnement** (`ENV` : `local`, `test`, `staging`,
  `production`)

## Tests

La suite de tests utilise [pytest](https://docs.pytest.org/) ; un conteneur
PostgreSQL est démarré automatiquement via `pytest-docker` :

```bash
pytest             # exécuter la suite de tests
ruff check .       # linting
ruff format --check .
mypy .             # vérification des types
```

## Internationalisation

Les messages d'erreur de l'API sont traduits sous `locales/` en **anglais**,
**français**, **néerlandais** et **allemand**. La langue de la réponse est
choisie à partir de l'en-tête `Accept-Language` de la requête.

## Contribuer

Les contributions sont les bienvenues ! Veuillez lire les
[directives de contribution](../CONTRIBUTING.md) et notre
[Code de conduite](../CODE_OF_CONDUCT.md) avant d'ouvrir une issue ou une pull
request.

## Sécurité

Pour signaler une faille de sécurité, veuillez suivre la
[politique de sécurité](../SECURITY.md) — n'ouvrez pas d'issue publique.

## Licence

Ce projet est distribué sous la [licence Apache 2.0](../LICENSE).
