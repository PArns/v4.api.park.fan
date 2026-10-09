"""Read-only export of the production tables the benchmark needs.

Standard library ONLY: this module runs on the database host (celestrial), whose
system Python has no third-party packages, and talks to Postgres exclusively
through ``scripts/prod-psql.sh`` — the wrapper that puts ``statement_timeout``,
``idle_in_transaction_session_timeout`` and ``lock_timeout`` on the session
(docs/troubleshooting/db-health-runbook.md §0). Every query is one
``COPY (SELECT …) TO STDOUT`` in autocommit, so no transaction is ever held
open between queries, and queries run strictly one after another.

Why the raw change log and not server-side 15-minute slots: ``queue_data`` is a
change log (a row only when a value changes), and STANDBY rows number
~130–190 k per day (measured 2026-10-09: 127 k on 03-15, 171 k on 07-15). One
UTC day is exactly one hypertable chunk, so a day-bounded query touches one
chunk and returns in well under a second, while the forward-fill expansion to
slots (≈ 4–10× more rows) is cheap in DuckDB on the benchmark side and costs the
production CPU nothing. Expanding server-side would also have needed a window
function over every ride of a park-month — the long, lock-holding kind of query
the runbook warns about.

Output: ``<out>/raw/*.csv.gz`` (+ ``manifest.json``). Files are written to
``*.part`` and renamed when complete, and existing files are skipped, so an
interrupted export resumes where it stopped.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import subprocess
import sys
import time
from pathlib import Path

US = "(extract(epoch from {c}) * 1000000)::bigint"


def _us(col: str) -> str:
    return US.format(c=col)


STATIC_QUERIES: dict[str, str] = {
    "parks": """
        SELECT id, slug, name, timezone, continent, "countryCode" AS country_code,
               "regionCode" AS region_code, "influencingRegions"::text AS influencing_regions,
               latitude::float8 AS latitude, longitude::float8 AS longitude, park_type
        FROM parks""",
    "attractions": f"""
        SELECT a.id, a."parkId" AS park_id, a.name, a.slug, a."attractionType" AS attraction_type,
               a.attraction_kind, a.is_seasonal, a.open_with_park,
               a.latitude::float8 AS latitude, a.longitude::float8 AS longitude,
               coalesce(a.curated_land_name, a.land_name) AS land,
               {_us('a.retired_at')} AS retired_at_us,
               h.tier AS headliner_tier, (h."attractionId" IS NOT NULL) AS is_headliner
        FROM attractions a
        LEFT JOIN headliner_attractions h ON h."attractionId" = a.id""",
    # Park-level schedule only (attractionId IS NULL). schedule_entries keeps the
    # LATEST published version of a day (updatedAt), never the version known at an
    # origin, so the benchmark flags every window as `published_final`.
    "schedule": f"""
        SELECT "parkId" AS park_id, date, "scheduleType"::text AS schedule_type,
               {_us('"openingTime"')} AS opening_us, {_us('"closingTime"')} AS closing_us,
               "isHoliday" AS is_holiday, "isBridgeDay" AS is_bridge_day,
               {_us('"updatedAt"')} AS updated_us
        FROM schedule_entries
        WHERE "attractionId" IS NULL AND date >= DATE '2025-01-01'""",
    "holidays": """
        SELECT country, date, region, "holidayType"::text AS holiday_type,
               "isNationwide" AS is_nationwide, name
        FROM holidays""",
    # Daily weather only — there is no hourly table, and `forecast` rows are
    # overwritten by every sync (only the current 16-day window exists), so there
    # is NO forecast archive: the benchmark can only use actuals, flagged ORACLE.
    "weather": f"""
        SELECT "parkId" AS park_id, date, "dataType"::text AS data_type,
               "temperatureMax"::float8 AS temp_max, "temperatureMin"::float8 AS temp_min,
               "precipitationSum"::float8 AS precip_sum, "rainSum"::float8 AS rain_sum,
               "snowfallSum"::float8 AS snow_sum, "weatherCode" AS weather_code,
               "windSpeedMax"::float8 AS wind_max, {_us('"updatedAt"')} AS updated_us
        FROM weather_data""",
}


def queue_query(day: dt.date) -> str:
    """One UTC day of the STANDBY change log = one hypertable chunk."""
    nxt = day + dt.timedelta(days=1)
    return f"""
        SELECT "attractionId" AS attraction_id, {_us('"timestamp"')} AS ts_us,
               status::text AS status, "waitTime" AS wait, is_heartbeat
        FROM queue_data
        WHERE "timestamp" >= TIMESTAMPTZ '{day.isoformat()} 00:00:00+00'
          AND "timestamp" <  TIMESTAMPTZ '{nxt.isoformat()} 00:00:00+00'
          AND "queueType" = 'STANDBY'"""


def forecast_query(table: str, start: dt.date, end: dt.date, max_lead: int) -> str:
    """Daily-level forecasts by forecast_date month (no index on forecast_date, so a
    month-sized sequential scan per query; ~5 M rows each)."""
    return f"""
        SELECT attraction_id, target_date, forecast_date, predicted_peak, model_version,
               {_us('created_at')} AS created_us
        FROM {table}
        WHERE forecast_date >= DATE '{start.isoformat()}' AND forecast_date < DATE '{end.isoformat()}'
          AND target_date - forecast_date BETWEEN 0 AND {max_lead}"""


def month_starts(first: dt.date, last: dt.date) -> list[tuple[dt.date, dt.date]]:
    out = []
    cur = first.replace(day=1)
    while cur <= last:
        nxt = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        out.append((cur, nxt))
        cur = nxt
    return out


class Psql:
    """Runs one COPY through prod-psql.sh on the database host."""

    def __init__(self, script: Path, timeout: str = "5min"):
        self.script = script
        self.timeout = timeout

    def _cmd(self, sql: str) -> list[str]:
        copy = f"COPY ({' '.join(sql.split())}) TO STDOUT WITH (FORMAT csv, HEADER true)"
        return ["bash", str(self.script), "-X", "-q", "-v", "ON_ERROR_STOP=1", "-c", copy]

    def copy_to(self, sql: str, dest: Path) -> int:
        """Stream the COPY into ``dest`` (gzip). Returns data rows written."""
        tmp = dest.with_suffix(dest.suffix + ".part")
        env = dict(os.environ, PARKFAN_PSQL_STATEMENT_TIMEOUT=self.timeout)
        lines = 0
        with gzip.open(tmp, "wb", compresslevel=5) as out:
            proc = subprocess.Popen(self._cmd(sql), stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, env=env)
            assert proc.stdout is not None
            for chunk in iter(lambda: proc.stdout.read(1 << 20), b""):
                lines += chunk.count(b"\n")
                out.write(chunk)
            err = proc.stderr.read().decode() if proc.stderr else ""
            rc = proc.wait()
        if rc != 0:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"psql failed ({rc}) for {dest.name}: {err.strip()}")
        tmp.rename(dest)
        return max(lines - 1, 0)  # minus header


def git_sha(repo: Path) -> str:
    try:
        return subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"],
                                       text=True).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def run_export(args: argparse.Namespace) -> dict:
    out = Path(args.out)
    raw = out / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    repo = Path(args.repo).resolve()
    psql = Psql(repo / "scripts" / "prod-psql.sh")
    manifest_path = out / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    files = manifest.setdefault("files", {})
    manifest.update({
        "export_started_utc": manifest.get("export_started_utc")
        or dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "git_sha": args.git_sha or git_sha(repo),
        "queue_from": args.queue_from,
        "queue_to": args.queue_to,
        "notes": {
            "schedule": "latest published version per day (no history) -> flagged published_final",
            "weather": "daily actuals only; no forecast archive exists -> ORACLE",
            "headliners": "headliner_attractions as of export time (548-day window, not ex-ante)",
            "queue_data": "STANDBY change log, all statuses; truth built offline (build step)",
        },
    })

    def job(name: str, sql: str) -> None:
        dest = raw / f"{name}.csv.gz"
        if dest.exists() and name in files:
            return
        t0 = time.monotonic()
        n = psql.copy_to(sql, dest)
        files[name] = {"rows": n, "seconds": round(time.monotonic() - t0, 2),
                       "bytes": dest.stat().st_size}
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
        print(f"{name}: {n} rows in {files[name]['seconds']} s", flush=True)
        time.sleep(args.pause)

    tables = set(args.tables.split(",")) if args.tables else None
    want = (lambda t: tables is None or t in tables)

    for name, sql in STATIC_QUERIES.items():
        if want(name):
            job(name, sql)

    for table in ("tft_forecasts", "catboost_daily_forecasts"):
        if not want(table):
            continue
        first = dt.date.fromisoformat(args.forecast_from)
        last = dt.date.fromisoformat(args.queue_to)
        for start, end in month_starts(first, last):
            job(f"{table}_{start:%Y-%m}", forecast_query(table, start, end, args.max_lead))

    if want("queue"):
        day = dt.date.fromisoformat(args.queue_from)
        last = dt.date.fromisoformat(args.queue_to)
        while day <= last:
            job(f"queue_{day.isoformat()}", queue_query(day))
            day += dt.timedelta(days=1)

    manifest["export_finished_utc"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest


def add_args(p: argparse.ArgumentParser) -> None:
    today = dt.datetime.now(dt.timezone.utc).date()
    p.add_argument("--out", required=True, help="export directory (e.g. /data/parkfan/ml-bench/exports/<id>)")
    p.add_argument("--repo", default=str(Path(__file__).resolve().parents[2]),
                   help="repo root containing scripts/prod-psql.sh")
    p.add_argument("--git-sha", default=None, help="record this SHA (when the code is not a git checkout)")
    p.add_argument("--queue-from", default="2025-12-24")
    p.add_argument("--queue-to", default=(today - dt.timedelta(days=1)).isoformat(),
                   help="last COMPLETE UTC day (inclusive)")
    p.add_argument("--forecast-from", default="2026-03-01")
    p.add_argument("--max-lead", type=int, default=120)
    p.add_argument("--tables", default=None,
                   help="comma list subset: parks,attractions,schedule,holidays,weather,"
                        "tft_forecasts,catboost_daily_forecasts,queue")
    p.add_argument("--pause", type=float, default=0.3, help="seconds between queries")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="mlbench export", description=__doc__.split("\n")[0])
    add_args(p)
    run_export(p.parse_args(argv))
    return 0


if __name__ == "__main__":
    sys.exit(main())
