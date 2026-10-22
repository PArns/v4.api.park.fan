# Retired parks (`retiredAt` on a park)

A park that closed for good — Six Flags America, last day 2025-11-02 — is
**retired**, not deleted. It keeps its URL and its history, and the park
endpoint says it is closed:

```json
{
  "retiredAt": "2025-11-02T00:00:00.000Z",
  "retiredReason": "Closed permanently after the 2025 season … Source: https://…"
}
```

Both fields are `null` while the park operates. Render a retired park the way
a ride retired as closed is rendered: closed permanently since that date, the
reason with its source, no live wait times, no crowd level, no forecast, no
opening hours. Its rides are retired on the same day, so every ride page of the
park reads closed as well.

## What leaves, what stays

| Where | Retired park |
| --- | --- |
| Park page (`/v1/parks/{continent}/{country}/{city}/{park}`) | stays, with `retiredAt` |
| `findAll` — and every background job that loops parks (wait times, schedule, weather, ML, stats) | gone |
| Geo lists (continent / country / city), discovery, search, nearby | gone |

`retiredAt` is the **operator's last operating day**, not the day somebody
noticed — the same rule as for rides
([absent facts](../rules/absent-facts.md)). Nothing syncs it; it is written by
hand through `POST /v1/admin/retire-parks`:

```json
{ "retirements": [{ "parkId": "…", "retiredAt": "2025-11-02", "reason": "… Source: https://…" }] }
```

The reason is user-facing and carries its source.
