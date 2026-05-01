# Primary vs Secondary Real Estate Price Prediction

## Project Goal

This project predicts the price of the primary real estate market using the secondary market as a proxy. The pipeline fuses multiple sources of real estate data, computes geographic and pricing descriptors, and supports reproducible model development.

## What this repository contains

- `src/` — source code for data cleaning, geocoding, feature engineering, and report generation
- `notebooks/` — exploratory data analysis and modeling notebooks
- `data/` — raw, interim, and processed datasets tracked with DVC
- `dvc.yaml` — pipeline stages and dependencies

## Key points for engineers

- Use `data/interim/` as the working data directory for pipeline outputs.
- Do not edit or depend on `data/processed/final.csv` or final artifacts directly.
- The pipeline is designed to be reproducible with DVC and should be run from the project root.

## Environment setup

1. Activate the virtual environment:

```bash
source env/bin/activate
```

2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Data setup

The repository uses DVC to manage large datasets on remote storage. Pull the required data before running anything:

```bash
dvc pull
```

If you need geocoding, add credentials under `secrets/geocoding/ya_api_keys.csv`. In most cases the dataset is already available via DVC and no manual secret setup is required.

## Running the pipeline

To run the full pipeline:

```bash
dvc repro
```

This will execute only stages that need updating and reuse cached results for unchanged stages.

## Typical engineer workflow

1. Pull data: `dvc pull`
2. Run the pipeline: `dvc repro`
3. Inspect output in `data/interim/`
4. Update code in `src/` or parameters in `params.yaml`
5. Commit code changes with Git and dataset changes with DVC if needed:

```bash
dvc commit
dvc push
```

## Infrastructure notes

- CI is run on NixOS.
- The project uses GitHub Actions for automated testing and pipeline validation.
- GitHub runner executes the pipeline and checks the repository state for reproducibility.

## Why this architecture

- DVC provides reproducible data pipelines and version control for large files.
- NixOS and GitHub Actions keep builds and CI runs consistent.