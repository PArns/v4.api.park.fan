import { Module } from "@nestjs/common";
import { QueueTimesClient } from "./queue-times.client";
import { QueueTimesDataSource } from "./queue-times-data-source";

@Module({
  providers: [QueueTimesClient, QueueTimesDataSource],
  exports: [QueueTimesClient, QueueTimesDataSource],
})
export class QueueTimesModule {}
