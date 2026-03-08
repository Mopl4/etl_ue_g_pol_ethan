# TP dbt + DuckDB — GitHub Analytics (ELT)

Ce dépôt contient un TP de data engineering : construire un pipeline **ELT** avec **dbt** sur **DuckDB** à partir de données GitHub (CSV). Le but est de produire une architecture **Bronze / Silver / Gold**, un **scoring** comparant 10 repos, des **tests**, de l’**incremental** et un **snapshot** (SCD2).

## Où est le projet dbt ?

- Projet dbt : `github_analytics/github_analytics/`
- Données CSV : `github_analytics/data/raw/` (historique) et `github_analytics/data/incremental/` (nouveaux fichiers pour simuler l’arrivée de données)
- Ingestion Bronze (CSV → DuckDB) : `github_analytics/scripts/load_bronze.py`

## Stack

- Python (script de chargement Bronze)
- dbt + adaptateur DuckDB
- DuckDB (fichier `.duckdb` local)

## Exécuter le pipeline (full)

Depuis `github_analytics/github_analytics/` :

```bash
python ../scripts/load_bronze.py
dbt run
dbt test
dbt docs generate
```

## Étapes du TP (résumé)

### Step 1 — Bronze (load)

Objectif : charger les CSV “raw” dans DuckDB, schéma `bronze`, pour conserver une base brute traçable (référence) sur laquelle dbt pourra travailler.

- Tables : `bronze.raw_repositories`, `bronze.raw_commits`, `bronze.raw_pull_requests`, `bronze.raw_issues`
- Script : `github_analytics/scripts/load_bronze.py`

Une fois le script exécuté, les commandes suivantes permettent de vérifier que les 4 tables sont dans DuckDB :
- dbt show --inline "select count (*) as nb from bronze.raw_repositories"
- dbt show --inline "select count (*) as nb from bronze.raw_commits"
- dbt show --inline "select count (*) as nb from bronze.raw_pull_requests"
- dbt show --inline "select count (*) as nb from bronze.raw_issues"

### Step 2 — Silver (staging)

Objectif : nettoyer et typer les données (casting, normalisation, colonnes dérivées simples) afin d’obtenir des tables cohérentes et réutilisables pour la suite.

- Modèles : `github_analytics/github_analytics/models/silver/stg_*.sql`
- Run les modèles silver : `dbt run --select silver`

### Step 3 — Gold (star schema)

Objectif : construire un modèle décisionnel en étoile pour analyser l’activité des repos (dimensions = contexte, table de faits = mesures au bon grain).

- Dimensions : `dim_repository`, `dim_date`, `dim_contributor`
- Fait : `fact_repo_activity`
- Run les modèles gold : `dbt run --select gold`

### Step 4 — Scoring (0–100)

Objectif : produire un classement “business” comparable entre repos en normalisant les métriques (NTILE) puis en combinant des sous-scores pondérés.

- Modèle : `github_analytics/github_analytics/models/gold/scoring_repositories.sql`
- Run le modèle : `dbt run –-select scoring_repositories`

### Step 5 — Tests & documentation

Objectif : sécuriser le pipeline (qualité des données + règles métier) et documenter le lineage pour rendre le projet compréhensible et vérifiable.

Première liste de tests pour vérifier la qualité des données présentes dans les différentes tables (vérification de l'unicité ou la non nullité de certains attributs, des valeurs autorisés, etc.).
- YAML : `github_analytics/github_analytics/models/silver/_schema.yml`, `github_analytics/github_analytics/models/gold/_schema.yml`

Seconde liste de tests pour vérifier la logique SQL mise en place. On vérifie ici que les scores (step 4) sont bien appliqués, les ordres de grandeurs des attributs (ex : totals_prs >= merged_pres), etc.
- Tests SQL : `github_analytics/github_analytics/tests/*.sql`

Exécuter les tests et générer une documentation :
- Tests : `dbt test`
- Docs : `dbt docs generate` (puis `dbt docs serve` si besoin)

### Step 6 — Incremental (Silver)

Objectif : simuler l’arrivée de nouvelles données et adapter certains modèles Silver pour ne traiter que le delta (réduire le temps de run en “production-like” et le coût d'exécution qui pourrait devenir énorme en fonction de la quantité de données stockées).

Nous avons décidé de adapter trois modèles Silver pour qu'ils fonctionnent en mode incrémental. Pour chacun de ces modèles nous avons identifié la méthode la plus adaptée au vu de l'utilisation de chaque table et nous avons modifé le fichier .sql en conséquence :
- **stg_commits** : La stratégie *append* nous semble être la plus logique ici. C'est la méthode la plus adapté aux logs / événements et c'est ce qu'il se passe ici avec la liste des commits des repos étudiés ;
- **stg_issues** : Nous choisissons ici d'utiliser la stratégie *merge* puisqu'une issue n'est pas un évènement immuable comme un commit. Une même issue peut évoluer dans le temps. Ici la clé unique choisie se compose des attributs ['repo_id', 'issue_number'] ; 
- **stg_pull_requests** : Dans ce troisième cas la stratégie *merge* nous semble être la mieux puisque les pull requests sont des objets avec un cycle de vie (created_at, updated_at, closed_at, etc.). La clé unique choisie est ['repo_id', 'pr_number'].

Pour conclure sur les choix faits ici, le choix de la stratégie incrémentale dépend de la nature des données :
- Commits : Un commit Git est immuable : une fois créé, il ne peut pas être modifié sans changer son SHA. Les commits peuvent donc être traités comme des événements append-only.

- Issues et Pull Requests : Les issues et pull requests ont un cycle de vie : leur état peut évoluer (par exemple open → closed ou merged). Il faut donc pouvoir mettre à jour les lignes existantes.

- Couche Gold : Rendre la couche Gold incrémentale est possible mais plus complexe, car les agrégations peuvent être affectées par des mises à jour tardives dans la couche Silver. 

Workflow (extrait du PDF) :

```bash
# A) état initial
python ../scripts/load_bronze.py --full-refresh
dbt run --select silver --full-refresh
dbt show --inline "select count(*) as nb from main_silver.stg_commits"

# B) simuler une nouvelle arrivée (copie incremental -> raw)
cp ../data/incremental/raw_commits_2026-01-01.csv ../data/raw/
cp ../data/incremental/raw_pull_requests_2026-01-01.csv ../data/raw/
cp ../data/incremental/raw_issues_2026-01-01.csv ../data/raw/

python ../scripts/load_bronze.py

# C) run incremental + 
dbt run --select silver
dbt show --inline "select count(*) as nb from main_silver.stg_commits"

# D) verifier l'idempotence
dbt run --select silver
```

Remarque : `raw_repositories` est un “snapshot d’état” (1 ligne par repo par date). Le chargement Bronze garde la version la plus récente par `full_name` si plusieurs fichiers existent dans `data/raw/` (sinon on dupliquerait les repos).

### Step 7 — Snapshots (SCD Type 2)

Objectif : historiser l’évolution de `raw_repositories` (stars, forks, etc.) afin de pouvoir analyser les changements dans le temps (SCD Type 2). Au vu des colonnes présentes dans le fichier `raw_repositorities_history.csv` il est logique d'utiliser la méthode Timestamp, puis de spécifier une colonne temporelle qui compose le fichier .csv. Dans notre cas nous avons choisi la colonne **snapshot_date** qui indique directement la date du snapshot du repository. dbt n'a ainsi qu'à comparer les différentes valeurs entre deux snapshots du même repository.

- Config : `github_analytics/github_analytics/snapshots/snap_repositories.yml`

Commande :

```bash
dbt snapshot
dbt show --inline "select full_name, stargazers_count, dbt_valid_from, dbt_valid_to from main_snapshots.snap_repositories order by full_name, dbt_valid_from"
```

## Ce qu’on retient du TP

- ELT vs ETL : ici on charge d’abord “brut” (Bronze), puis on transforme dans le moteur SQL (Silver/Gold). Ça facilite le debug (on peut revenir au raw) et ça rend les transformations rejouables.
- Architecture médaillon :
  - Bronze = ingestion (peu/pas de logique métier).
  - Silver = nettoyage/typage/normalisation (clés stables, timestamps cohérents, dérivés simples).
  - Gold = tables “métier” (modèle en étoile + métriques).
- dbt comme cadre de travail : séparer les modèles, gérer les dépendances (`source()` vs `ref()`), et obtenir un lineage clair via `dbt docs`.
- Modèle en étoile : dimension = contexte (repo, date, contributor), fait = mesures au bon grain (activité par repo et par jour). Ça rend les analyses plus simples (joins “propres”, agrégations cohérentes).
- Scoring : normaliser des métriques hétérogènes (stars vs temps de réponse) avec `NTILE` pour comparer “relativement” entre 10 repos, puis agréger avec des pondérations pour obtenir un score global.
- Tests dbt :
  - Qualité “structurelle” : `not_null`, `unique`, `relationships`, `accepted_values`.
  - Logique métier : tests SQL (cohérence chronologique, complétude du scoring, ratios dans [0,1], ranking sans doublons).
- Incremental : ce n’est pas juste “plus rapide”, il faut choisir une stratégie selon la donnée :
  - événements immuables (commits) → `append` + anti-dup.
  - entités modifiables (issues/PR qui changent d’état) → `delete+insert` (ou merge) + clé stable.
  - et surtout : bien définir “qu’est-ce qu’une nouvelle donnée ?” (clé + timestamp), sinon on rate des lignes.
- Snapshots (SCD2) : utile pour historiser une table “état courant” (ex: `raw_repositories`) et pouvoir analyser l’évolution (stars/forks) avec `dbt_valid_from/dbt_valid_to`.
- En pratique : une pipeline “propre” = commandes reproductibles (`load_bronze.py`, `dbt run`, `dbt test`, `dbt docs`, `dbt snapshot`) + un dépôt qui n’embarque pas les artefacts générés (`.gitignore`).

## Notes

- DuckDB peut verrouiller le fichier `.duckdb` si deux commandes tournent en même temps (éviter de lancer `dbt` en parallèle).
