# Deployment environments

This project uses one Databricks workspace with isolated development and production bundle targets.

## Development

- Deploy locally with `databricks bundle deploy --target dev`.
- Development mode gives each deployer a separate bundle identity and prefixes resources with the current user's short name.
- Development schedules and triggers are paused by development mode.
- Development code defaults to the `wonik_dev` catalog.
- A pull request is not required to deploy a personal development target.

## Production

- Production uses the `wonik_poc` catalog.
- Production resources have a `[prod]` prefix and use a restricted production bundle root.
- Production is deployed only by GitHub Actions after changes are merged to `main`.
- Configure approval rules for the GitHub `production` environment and replace token authentication with a service principal when available.

## Dashboard limitation

The dashboard source currently contains SQL queries that reference `wonik_poc` directly. Bundle variables apply to bundle configuration, not arbitrary SQL text inside the dashboard JSON. Do not use the deployed development dashboard to test writes or assume that it reads `wonik_dev` until a separate development dashboard source or dashboard parameters are introduced.

## Local workflow

1. Create a feature branch.
2. Edit and test locally or deploy to the personal `dev` target.
3. Commit and open a pull request.
4. Review validation checks.
5. Merge to `main`; GitHub Actions deploys the shared `prod` target.
