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

## Supported Sources

### 1. Themeparks (Library)
- **Location**: `src/external-apis/themeparks`
- **Description**: Wraps the popular `themeparks` Node.js library.
- **Role**: **Primary** source for most major supported parks (Disney, Universal, etc.).
- **Data**: Wait Times, Status, Opening Hours.

### 2. Queue-Times.com
- **Location**: `src/external-apis/queue-times`
- **Description**: Scraper/API client for Queue-Times.com.
- **Role**: **Fallback** or Primary for smaller parks not supported by the main library.

### 3. Wartezeiten
- **Location**: `src/external-apis/wartezeiten`
- **Role**: Specific scrapers for German/European parks.

### 4. Direct/Custom
- Some parks have custom implementations (e.g., `Efteling` if direct integration is needed).
