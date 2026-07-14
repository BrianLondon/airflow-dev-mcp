# Releasing

Releases publish to PyPI automatically via **Trusted Publishing** (OIDC) — no API
token is stored in the repo or in GitHub secrets. The
[`.github/workflows/publish.yml`](.github/workflows/publish.yml) workflow builds and
uploads whenever a GitHub Release is published.

## One-time setup on PyPI (before the first release)

1. Go to <https://pypi.org/manage/account/publishing/> and add a **pending publisher**
   (works even though the project doesn't exist on PyPI yet):
   - **PyPI Project Name:** `airflow-dev-mcp`
   - **Owner:** `BrianLondon` · **Repository:** `airflow-dev-mcp`
   - **Workflow name:** `publish.yml`
   - **Environment name:** `pypi`
2. In the GitHub repo, create an Environment named `pypi`
   (*Settings → Environments*). Optionally add required reviewers there to gate
   uploads behind a manual approval.

## Cutting a release

1. Bump `__version__` in `src/airflow_dev_mcp/__init__.py` and commit.
2. Tag and push: `git tag v0.2.0 && git push origin main --tags`.
3. Create a GitHub Release for that tag. The workflow builds, runs `twine check`,
   and publishes to PyPI.
