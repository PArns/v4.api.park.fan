# Data Ingestion Architecture

## Overview
The API aggregates data from multiple external sources to ensure high availability and coverage. A single park (e.g., Europa-Park) might be tracked by multiple providers.

## The Orchestrator
**Service**: `MultiSourceOrchestrator` (`src/external-apis/data-sources/multi-source-orchestrator.service.ts`)

Instead of fetching from a single API, we define a **Strategy** for each park.

### Strategies
1.  **Single Source**: Just use one reliability provider (e.g., `THEMEPARKS`).
2.  **Fallback**: Try Primary -> if fail/empty -> Try Secondary (e.g., `THEMEPARKS` -> `QUEUE_TIMES`).
3.  **Merge**: Fetch from all, combine unique attractions (e.g., Source A tracks rides, Source B tracks shows).

## Conflict Resolution
**Service**: `ConflictResolverService`

When two sources report different wait times for the *same* attraction:
- We prioritize "Trusted" sources (configured in metadata).
- **Sanity Check**: We discard data that looks "stuck" (unchanged for > 2 hours) if a fresher source is available.
- **Normalization**: All statuses are mapped to our internal enum (`OPERATING`, `DOWN`, `CLOSED`).

### Wait times are stored in five-minute steps
Every STANDBY, SINGLE_RIDER and PAID_STANDBY wait is rounded with `roundToNearest5Minutes`
in `QueueDataService.buildQueueCandidates`, the one place every source's row passes through.
The one exception is 13: Disney posts it for a walk-on, so `roundToNearest5Minutes` returns
exactly 13 unchanged (`WALK_ON_WAIT_MINUTES`) and it is never stored or served as 15.
The park, attraction and favorites payloads serve `queue_data.waitTime` as stored, so a value
that reaches the table off the grid reaches the user off the grid.

The rounding in `ConflictResolverService` does not cover this. It writes the merged entity's
`waitTime`, while `WaitTimesProcessor.adaptEntityLiveData` stores the source's `queue` object
whenever one is present (ThemeParks.wiki always, Wartezeiten for a ride with a wait). Most feeds
post multiples of five, but not all: Movie Park's rides read 61 on 2026-10-04, and the wiki sent
Europa-Park 1, 3 and 6 on 2026-10-05 (PAR-715). A consequence of the same split: with several
sources, the stored wait is the queue object's figure, not the resolver's consensus.

## Reverse Reconciliation (stale attraction auto-close)
Upstream sources sometimes stop reporting an attraction entirely (e.g. seasonal Halloween mazes after the event ends). Without a counter-signal the last-known `OPERATING` status would remain indefinitely. See [`reverse-reconciliation.md`](./reverse-reconciliation.md) for the mechanism that writes a `CLOSED` entry once an attraction has not been seen in any source for >24h.

## Supported Sources

### 1. ThemeParks.wiki (Direct API)
- **Location**: `src/external-apis/themeparks`
- **Description**: Direct client for the [ThemeParks.wiki REST API](https://api.themeparks.wiki/docs/v1/). 
- **Role**: **Primary** source for most major supported parks (Disney, Universal, etc.).
- **Data**: Wait Times, Status, Opening Hours, Forecasts.

### 2. Queue-Times.com
- **Location**: `src/external-apis/queue-times`
- **Description**: Scraper/API client for Queue-Times.com.
- **Role**: **Fallback** or Primary for smaller parks not supported by the main library.

### 3. Wartezeiten
- **Location**: `src/external-apis/wartezeiten`
- **Role**: Specific scrapers for German/European parks.

### 4. Direct/Custom
- Some parks have custom implementations (e.g., `Efteling` if direct integration is needed).
