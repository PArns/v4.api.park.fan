# Changelog fragments

One file per pull request. Write your entry here, **not** into
`docs/changelog.md`.

```
docs/changelog.d/PAR-257.md
```

```markdown
### Fixed — a park merge keeps the losing park's per-ride schedule

What changed, why it was wrong before, and the number that says so.
```

## The rules

- **File name:** `PAR-<issue>.md`. The issue number is the whole name, so two
  pull requests never write the same file and git never has to merge them.
- **First line:** `### <Type> — <title>`, with an em dash. `<Type>` is one of
  `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`,
  `Documented`, `Performance`.
- **Below it:** the entry, in the same voice as the entries already in
  `docs/changelog.md`.
- **No `# ` or `## ` heading** — a fragment is one entry, not a section. Inside
  a fenced code block both are fine; the fence has to be closed.

`pnpm changelog:check` reports what it can read and what it rejects.
`changelog-fragments.util.spec.ts` runs the same rules over this directory in
`pnpm test`, so a fragment the merge would drop fails the suite instead.

## At release time

```bash
pnpm changelog:merge
```

folds every fragment in here under `## [Unreleased]` in `docs/changelog.md`,
newest issue number first, and deletes the files it folded in. Nothing already
in `docs/changelog.md` is rewritten.

## Why

Every entry used to be written directly under `## [Unreleased]`, so two pull
requests open at the same time always touched the same line. On 2026-09-15 and
2026-09-16 seven merges were sent back for a rebase and not one of them had a
conflict outside `docs/changelog.md` (PAR-257).
