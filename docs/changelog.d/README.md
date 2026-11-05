# Changelog fragments

One file per pull request. Write your entry here, **not** into
`docs/changelog.md`: entries written straight under `## [Unreleased]` made any
two open pull requests conflict on the same line (PAR-257).

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
- **No `# ` or `## ` heading** — a fragment is one entry, not a section.
- **No horizontal rule** — no line of only `-` or `=`. Under a paragraph that
  is a heading (`text` over `---` is an `<h2>`), and telling the two apart takes
  a Markdown parser, so the check refuses both. No entry in `docs/changelog.md`
  uses one; if you want to separate something, use a blank line.
- Inside a fenced code block both are fine. The fence has to be closed.
- **Links are written relative to `docs/`**, where the merge puts the entry:
  `[caching](architecture/caching-strategy.md)`, not `../architecture/…`. The
  merge does not rewrite them, so a link relative to this directory breaks in
  `docs/changelog.md`. Point at the page that holds the subject now, not at a §
  number.

`pnpm changelog:check` reports what it can read and what it rejects.
`changelog-fragments.util.spec.ts` runs the same rules over this directory in
`pnpm test`, so a fragment the merge would drop fails the suite instead.

## At release time

```bash
pnpm changelog:merge
```

folds every fragment in here under `## [Unreleased]` in `docs/changelog.md`,
newest issue number first, and deletes the files it folded in. The entries
already in `docs/changelog.md` are carried over as they stand; the only thing
that changes is the run of blank lines directly under the heading, which becomes
one.