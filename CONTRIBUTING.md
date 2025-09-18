
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
1. Update `CHANGELOG.md` with the highlights of the release.
2. Bump the `version` field in `pyproject.toml` (the package reads it dynamically at runtime).
3. Commit, tag the release, and push to the default branch.
4. Build and validate the distribution:
   ```bash
   python -m build
   twine check dist/*
   ```
5. Upload the artifacts to PyPI (or TestPyPI) with `twine upload`.
