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
