# Scripts

This directory is gitignored except for `prod-psql.sh` (ad-hoc SQL against production with time limits),
`backup/` (the nightly host backup), `python-deps/` (dependency-upgrade checks) and
`generate-swagger-spec.ts` (runs after `pnpm build`).

What each one does, which `package.json` commands depend on local-only scripts, and how to run a script
against production: [docs/development/scripts.md](../docs/development/scripts.md).
