/**
 * `pnpm changelog:check` and `pnpm changelog:merge`.
 *
 * `check` reads every fragment in `docs/changelog.d/` and says how many it read;
 * `merge` folds them into `docs/changelog.md` under `## [Unreleased]` and
 * deletes the files it folded in. Neither takes an argument.
 *
 * The rules live in `changelog-fragments.util.ts` so the spec next to it can
 * measure them without a filesystem. This file is the part that touches disk.
 *
 * It sits under `src/` and not next to the repository's other ts-node scripts
 * because `.gitignore:41` is `scripts/*`: a file added there is not committed
 * without `git add -f` and never shows up in `git status` for the next person.
 * `scripts/generate-swagger-spec.ts` is tracked only because it predates that
 * rule. Here it is linted, type-checked and covered by the same commands as the
 * rest of `src`; it is excluded from `collectCoverageFrom` like `main.ts`, for
 * the same reason — an entry point is run, not unit-tested.
 */
import { readFileSync, rmSync, writeFileSync } from "fs";
import { join, resolve } from "path";

import {
  CHANGELOG_FILE,
  FRAGMENT_DIR,
  FragmentProblem,
  UNRELEASED_HEADING,
  readFragmentDirectory,
  spliceIntoChangelog,
} from "./changelog-fragments.util";

/** The repository root, three directories above `src/common/utils`. */
const ROOT = resolve(__dirname, "..", "..", "..");

const readDirectory = () => readFragmentDirectory(join(ROOT, FRAGMENT_DIR));

function report(problems: FragmentProblem[]): void {
  for (const { file, problem } of problems) {
    console.error(`✗ ${FRAGMENT_DIR}/${file}: ${problem}`);
  }
}

/**
 * The one precondition of `merge` that is not a fragment: `spliceIntoChangelog`
 * throws without the heading, and `check` would otherwise report a clean run
 * right up to the release that fails.
 */
function missingHeading(): string | null {
  const changelog = readFileSync(join(ROOT, CHANGELOG_FILE), "utf8");
  return /^## \[Unreleased\][^\n]*$/m.test(changelog)
    ? null
    : `${CHANGELOG_FILE} has no "${UNRELEASED_HEADING}" heading to fold into`;
}

function check(): number {
  const { fragments, problems } = readDirectory();
  report(problems);

  const heading = missingHeading();
  if (heading) {
    console.error(`✗ ${heading}`);
  }

  console.log(
    `${fragments.length} changelog fragment(s) ready, ${problems.length} rejected`,
  );
  return problems.length > 0 || heading ? 1 : 0;
}

function merge(): number {
  const { fragments, problems } = readDirectory();
  if (problems.length > 0) {
    report(problems);
    console.error("nothing merged — fix the fragments above first");
    return 1;
  }
  if (fragments.length === 0) {
    console.log(`no fragments in ${FRAGMENT_DIR}, ${CHANGELOG_FILE} unchanged`);
    return 0;
  }

  const changelog = join(ROOT, CHANGELOG_FILE);
  writeFileSync(
    changelog,
    spliceIntoChangelog(readFileSync(changelog, "utf8"), fragments),
    "utf8",
  );

  for (const fragment of fragments) {
    rmSync(join(ROOT, FRAGMENT_DIR, fragment.file));
  }

  console.log(
    `merged ${fragments.length} fragment(s) into ${CHANGELOG_FILE}: ${fragments
      .map((fragment) => fragment.file)
      .join(", ")}`,
  );
  return 0;
}

function main(): number {
  const command = process.argv[2] ?? "check";
  if (command === "check") {
    return check();
  }
  if (command === "merge") {
    return merge();
  }
  console.error(`unknown command "${command}" — expected "check" or "merge"`);
  return 1;
}

if (require.main === module) {
  process.exitCode = main();
}
