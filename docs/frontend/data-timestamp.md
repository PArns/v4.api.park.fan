# When a park's live data was read (`dataTimestamp`)

The contract for the "as of" line on a park page. Open it when rendering how fresh the live data is.

```jsonc
// GET /v1/parks/:continent/:country/:city/:park
{ "id": "…", "status": "OPERATING", "dataTimestamp": "2026-08-29T10:35:00.000Z", … }
```

Set in `ParkIntegrationService.rebuildIntegratedResponse`, just before the payload reads its live
data (queue rows, shows, restaurants, statistics).

- **It is the time the payload was built**, in UTC. The payload is cached in Redis
  (`park:integrated:<id>`, rebuilt about every five minutes while the park is open) and at the edge,
  so it is how old the answer you hold is, whichever cache it came from.
- **It is not the newest queue reading.** A park whose rides are all closed carries an old
  `queues[].lastUpdated` and a fresh `dataTimestamp`: the data was read just now and said nothing
  had changed. `max(queues[].lastUpdated)` is not needed for the "as of" line any more.
- **Absent on a payload built before this field existed**, until that cache entry is rebuilt. Fall
  back to the request's own time then.

## When a ride's wait was last read (`queues[].lastObserved`)

`queues[].lastUpdated` is when the posted wait last **changed**. `queues[].lastObserved` (ISO 8601)
is when a source last **delivered** the ride, changed or not: the "Aktualisiert" time to show next
to a wait. Served wherever `lastUpdated` is: park page, attraction page, favorites, park wait times.

- Derived from the Redis key `attraction:last-seen:<attractionId>` that the wait-times sync stamps
  on every poll in which a real feed lists the ride (14-day TTL). The hourly heartbeat and
  reverse-reconciliation never stamp it, so a ride whose source stopped answering keeps its old
  time. One `MGET` per response build, no extra write.
- Never earlier than the row's own `lastUpdated`; without a last-seen it equals `lastUpdated`.
  On a heartbeat row (which carries `lastUpdated = now` without a reading) it is the last-seen or
  `null`.
- Optional: absent on a payload built before this field existed (cached up to ~5 minutes) and null
  on a heartbeat row without a last-seen. Fall back to `lastUpdated` then.
