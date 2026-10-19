### Fixed — a shutdown during the boot chains stops them instead of logging errors

`QueueSchedulerService`, `QueueBootstrapService`, `TimescaleInitService` and
`DbSeedService` start `await` chains from `onModuleInit` that nothing could stop.
`onModuleDestroy` cancelled a timer that had not fired, but a chain that was
already running kept querying a closed DataSource and writing into closing
queues, and the result was logged as an `ERROR` that reads like a real defect.

The chains now run through `AbortableRuns` (`src/common/utils/abortable-runs.ts`).
On shutdown each one stops at its next checkpoint without a log line, and
`onModuleDestroy` waits for the step in flight, so connections close after the
last query and not under it. Aborting without waiting would leave that query
running into a closed pool; waiting for the whole chain would hold `app.close()`
for the scheduler's 53 repeatable entries. What the chains seed and when is
unchanged.
