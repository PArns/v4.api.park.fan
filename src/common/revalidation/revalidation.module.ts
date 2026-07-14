import { Module } from "@nestjs/common";
import { RevalidationService } from "./revalidation.service";

/**
 * Provides the on-demand frontend revalidation webhook client.
 * Stateless + dependency-free, so it can be imported anywhere a background
 * batch needs to bust the frontend's cache after recomputing derived data.
 */
@Module({
  providers: [RevalidationService],
  exports: [RevalidationService],
})
export class RevalidationModule {}
