"""Bind uuid ids so Postgres can use the index on the uuid column (PAR-871).

The rule: never cast an indexed uuid column, cast the parameter.

    WRONG  qd."attractionId"::text = ANY(:ids)        -- no index, full chunk reads
    RIGHT  qd."attractionId" = ANY(CAST(:ids AS uuid[]))

Casting the column to text makes the predicate opaque to the planner. On the
compressed `queue_data` chunks that meant a sequential scan of every chunk in
the window instead of an index scan on the segment-by column, and the two
serving reads in predict.py alone were ~80 % of all database time.

Casting the parameter has one failure mode the old shape did not: a single
value that is not a uuid makes the CAST raise, and the whole query fails. The
old text comparison just matched nothing for that value. So the ids are
filtered first, and only values that the old comparison COULD have matched are
kept: `uuid::text` is always the lowercase hyphenated form, so anything else
(an uppercase id, braces, no hyphens, garbage) never matched before and is
dropped now. Same rows, before and after.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterable

logger = logging.getLogger(__name__)

# Values already reported, so a caller that keeps sending the same bad id
# logs it once per process instead of once per request.
_reported: set = set()


def _canonical(value) -> str | None:
    """The id as `uuid::text` would print it, or None if the old text
    comparison could never have matched it."""
    if value is None:
        return None
    text_value = str(value)
    try:
        parsed = uuid.UUID(text_value)
    except (ValueError, AttributeError, TypeError):
        return None
    canonical = str(parsed)
    return canonical if canonical == text_value else None


def uuid_list(ids: Iterable, context: str) -> list[str]:
    """Keep the ids that are canonical uuid strings, in order.

    Bind the result with `= ANY(CAST(:ids AS uuid[]))`. Dropped values are
    logged once per process, naming `context` (the query they were meant for).
    """
    kept: list[str] = []
    dropped: list[str] = []
    for value in ids:
        canonical = _canonical(value)
        if canonical is None:
            dropped.append(repr(value))
        else:
            kept.append(canonical)
    new = [d for d in dropped if (context, d) not in _reported]
    if new:
        _reported.update((context, d) for d in new)
        logger.warning(
            "%s: dropped %d id(s) that are not canonical uuids (they could "
            "never match a uuid column): %s",
            context,
            len(new),
            ", ".join(new[:5]) + (" …" if len(new) > 5 else ""),
        )
    return kept


def uuid_or_none(value, context: str) -> str | None:
    """Single-id form of uuid_list: the canonical id, or None (logged once)."""
    kept = uuid_list([value], context)
    return kept[0] if kept else None
