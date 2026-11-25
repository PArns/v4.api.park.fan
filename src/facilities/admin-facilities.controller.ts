import { Get, Param, ParseUUIDPipe, Post, Query } from "@nestjs/common";
import { ApiOperation, ApiQuery } from "@nestjs/swagger";
import { AdminApi } from "../admin/auth/admin-api.decorator";
import {
  AdminMinRole,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { FacilitiesService, parseBoundaryRef } from "./facilities.service";

/**
 * Import and review of a park's OpenStreetMap facilities. The import runs
 * inside the request (two Overpass queries, seconds), so the answer is the
 * difference the reviewer is about to look at.
 * shortcut: no queue, so a slow Overpass holds the request for up to 90 s;
 * move to a Bull job when imports run for many parks at once.
 */
@AdminApi("admin/facilities", new NoCdnCacheInterceptor())
export class AdminFacilitiesController {
  constructor(private readonly facilities: FacilitiesService) {}

  @Get(":parkId")
  @ApiOperation({
    summary: "A park's imported facilities and their review state",
    description: "Every row, hidden ones included. Read-only.",
  })
  review(@Param("parkId", ParseUUIDPipe) parkId: string) {
    return this.facilities.review(parkId);
  }

  @Post(":parkId/import")
  @AdminMinRole("editor")
  @ApiOperation({
    summary: "Read this park's facilities from OpenStreetMap now",
    description:
      "Idempotent: new points in, changed ones updated, vanished ones out, " +
      "hidden flags kept. A new or visibly changed point puts an approved " +
      "park back to `imported` until someone approves it again.",
  })
  @ApiQuery({
    name: "boundary",
    required: false,
    description:
      "The park's outline in OSM (`way/31052062`, `relation/123`) when the " +
      "name match finds none or the wrong one. Remembered for later imports.",
  })
  import(
    @Param("parkId", ParseUUIDPipe) parkId: string,
    @Query("boundary") boundary?: string,
  ) {
    return this.facilities.importPark(
      parkId,
      boundary ? parseBoundaryRef(boundary) : undefined,
    );
  }

  @Post(":parkId/approve")
  @AdminMinRole("editor")
  @ApiOperation({ summary: "Serve this park's facilities publicly" })
  approve(
    @Param("parkId", ParseUUIDPipe) parkId: string,
    @CurrentAdmin() admin: AdminPrincipal,
  ) {
    return this.facilities.setApproved(parkId, true, admin.email);
  }

  @Post(":parkId/withdraw")
  @AdminMinRole("editor")
  @ApiOperation({ summary: "Stop serving this park's facilities" })
  withdraw(
    @Param("parkId", ParseUUIDPipe) parkId: string,
    @CurrentAdmin() admin: AdminPrincipal,
  ) {
    return this.facilities.setApproved(parkId, false, admin.email);
  }

  @Post(":parkId/points/:facilityId/hide")
  @AdminMinRole("editor")
  @ApiOperation({
    summary: "Leave one point out of the public answer",
    description:
      "For a point that is right in OpenStreetMap and wrong for a visitor " +
      "(outside the gates). A point that is wrong is corrected in " +
      "OpenStreetMap and imported again.",
  })
  hide(
    @Param("parkId", ParseUUIDPipe) parkId: string,
    @Param("facilityId", ParseUUIDPipe) facilityId: string,
  ) {
    return this.facilities.setHidden(parkId, facilityId, true);
  }

  @Post(":parkId/points/:facilityId/show")
  @AdminMinRole("editor")
  @ApiOperation({ summary: "Serve a hidden point again" })
  show(
    @Param("parkId", ParseUUIDPipe) parkId: string,
    @Param("facilityId", ParseUUIDPipe) facilityId: string,
  ) {
    return this.facilities.setHidden(parkId, facilityId, false);
  }
}
