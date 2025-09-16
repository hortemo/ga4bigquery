
# Contributing

Thanks for helping improve **ga4bigquery**!

## Development setup
```bash
python -m venv .venv && source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"
```

## Running tests
```bash
pytest -q
```

## Code style
- Run `ruff check` and `ruff format` before committing (or use your IDE’s formatter).
- Type hints are encouraged.

## Releasing
- Update `CHANGELOG.md` and bump `version` in `src/ga4bigquery/__init__.py` and `pyproject.toml`.
