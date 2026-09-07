import { Module } from "@nestjs/common";
import { RevalidationService } from "./revalidation.service";
import { ParkStatusRevalidationService } from "./park-status-revalidation.service";

/**
 * Provides the on-demand frontend revalidation webhook client.
 * Stateless + dependency-free, so it can be imported anywhere a background
 * batch needs to bust the frontend's cache after recomputing derived data.
 *
 * `ParkStatusRevalidationService` is the one stateful caller: it keeps the previous cycle's park
 * statuses in Redis so it can tell an opening from a park that was already open.
 */
@Module({
  providers: [RevalidationService, ParkStatusRevalidationService],
  exports: [RevalidationService, ParkStatusRevalidationService],
})
export class RevalidationModule {}
