# The works period on a ride (`worksPeriod`)

**What it answers:** is this ride closed for a rebuild or a refit somebody
scheduled, and until when.

Present on the attraction detail payload and on every ride in the park payload,
so a ride list can badge without a second request.

```jsonc
"worksPeriod": {
  "from": "2026-01-16",   // first park-local day, inclusive
  "to": "2026-03-03",     // last park-local day, inclusive. Absent while nobody knows.
  "toUncertain": false    // true: `to` is our estimate, not a date the park published
}
```

Curated by hand under `/admin/attractions/<id>`, in the group "Umbaupause". No
feed carries it — ThemeParks.wiki passes `REFURBISHMENT` through with no start,
no end and no announcement, and does not use it for every closure that really is
planned work.

## It is not an outage, and must never be rendered as one

`outage` says a ride stopped working and the site noticed. This says a ride was
taken out of service on purpose, months ahead, by people who scheduled it.

The two never contradict each other in a payload, because the API reports **no
outage at all** inside a running works period — the feed cannot tell a rebuild
from a breakdown, so the curated window is the only thing that can. A page that
folded them together would tell a visitor the ride broke while it is being
rebuilt on schedule.

## Rendering

These are the wordings for a window that is **running today**, which is a
question you answer first — see the next section. A window that has not started
says "ab 16. Januar" and one that is over says nothing at all; "seit" and the
present tense are claims about now, and this block makes none.

The two halves are independent, so read them as two decisions rather than a
table of cases. `to` and `toUncertain` decide how the end is worded:

```
to, toUncertain false → "bis 3. März"
to, toUncertain true  → "voraussichtlich bis 3. März"
no to                 → no end clause at all
```

and `from` only decides whether the sentence opens with a start:

```
from    → "Umbaupause seit 16. Januar …"
no from → "Umbaupause …"
```

**`toUncertain` is independent of `from`.** A window with an end and no start is
ordinary — somebody wrote it down after the work had begun — and it carries the
hedge exactly as a complete one does. Dropping the hedge there would print an
estimate as a firm date, which is the thing the field exists to prevent.

Two things the API does guarantee, so nobody builds a dead branch:

- **`toUncertain` is never `true` without a `to`** — a flag with no date to
  qualify is dropped on the way out.
- **At least one of `from` and `to` is always there.** Both empty is not an
  empty block, it is no block: `worksPeriod` itself is absent. A bare
  "Umbaupause" with no date is a state this endpoint cannot produce.

**A missing `to` is not "closed for good".** It is the ordinary state while work
is running and the park has not said when it ends — the park usually names a
season before it names a day, which is what `toUncertain` exists for once
somebody writes the day down.

## Whether it is running today is your comparison, not ours

The block is a fact about a date range and says nothing about today. A window
that ended in March stays on the row; a rebuild starting next winter looks the
same from here.

Both bounds are park-local `YYYY-MM-DD` and both are **inclusive**, so compare
them against the park's own calendar day — `timezone` on the park payload — and
never against the reader's clock. For a park eight hours away the window would
otherwise open and close on the wrong day.

Deliberately not served as a computed `active` flag: the endpoints that carry
this are cached, and a boolean computed at render time would be wrong for every
reader who gets the cached copy on the other side of park-local midnight.

## The absence

`worksPeriod` is absent for nearly every ride, and its absence says nothing at
all. No feed announces a rebuild, so "no works period here" means nobody has
curated one — never that the ride is running. Do not render a "läuft normal"
line from it.

`from` and `to` are stripped from the JSON when they are null, like every other
null key outside `/v1/admin/*` (`ExcludeNullInterceptor`), so read them as
optional even though the published schema documents both.
