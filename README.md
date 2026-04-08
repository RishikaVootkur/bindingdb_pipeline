# ChEMBL Small Molecule-Protein Interaction Pipeline

An end-to-end data engineering pipeline that ingests, transforms, validates,
and loads large-scale small molecule-protein binding interaction data from
[ChEMBL](https://www.ebi.ac.uk/chembl/) into a normalized PostgreSQL database,
with analytical SQL queries for downstream analysis.

## Architecture

ChEMBL API (public bioactivity database)
↓
src/extract.py — fetches binding activity data via ChEMBL API
↓
src/transform.py — cleans, normalizes, adds derived columns
↓
src/validate.py — 6 data quality checks before loading
↓
src/load.py — designs schema, loads into PostgreSQL
↓
src/queries.py — analytical SQL queries on loaded data
↓
main.py — orchestrates full pipeline end to end

## Database Schema

Three normalized tables:

- **targets** — unique protein targets (name, organism, ChEMBL ID)
- **compounds** — unique small molecules (SMILES, ChEMBL ID, name)
- **interactions** — binding measurements linking targets to compounds
  (IC50, Ki, Kd, EC50, pChEMBL value, potency category, assay details)

## Tech Stack

- Python 3.11
- PostgreSQL 15 (Docker)
- pandas, SQLAlchemy, psycopg2
- ChEMBL REST API (chembl-webresource-client)
- pytest (18 tests, 100% passing)
- Docker + docker-compose

## Quickstart

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/bindingdb_pipeline.git
cd bindingdb_pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Start PostgreSQL

```bash
docker-compose up -d
```

### 4. Run the pipeline

```bash
python3 main.py
```

### 5. Run tests

```bash
python3 -m pytest tests/ -v
```

## Sample Query Results

After running the pipeline on 500 records:

- **153** unique protein targets loaded
- **286** unique small molecule compounds loaded
- **500** binding interaction records loaded
- **Top target:** D(2) dopamine receptor with 40 interactions
- **230** highly potent compounds (≤ 10 nM)
- **Affinity breakdown:** 588 IC50, 396 Ki, 10 Kd, 6 EC50 measurements
- **Most potent hit:** Lanosterol 14-alpha demethylase inhibitor at 0.01 nM

## Project Structure

bindingdb_pipeline/
├── src/
│ ├── extract.py # ChEMBL API ingestion
│ ├── transform.py # Cleaning and normalization
│ ├── validate.py # Data quality checks
│ ├── load.py # PostgreSQL schema + loading
│ └── queries.py # Analytical SQL queries
├── tests/
│ ├── test_transform.py
│ └── test_validate.py
├── data/`
│ ├── raw/ # Raw API cache (gitignored)
│ └── processed/ # Cleaned CSVs (gitignored)
├── logs/ # Pipeline logs (gitignored)
├── docker-compose.yml
├── requirements.txt
└── main.py

## Key Engineering Decisions

- **Normalized schema** — targets, compounds, and interactions are stored in
  separate tables linked by foreign keys, avoiding data duplication
- **Validation layer** — pipeline halts automatically if data quality checks fail,
  preventing bad data from reaching the database
- **Caching** — raw API responses are saved locally so the pipeline can re-run
  without re-fetching data
- **Indexed queries** — indexes on target_id, affinity_value_nm, and
  affinity_category ensure fast analytical queries at scale
