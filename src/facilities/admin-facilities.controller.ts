import {
  Body,
  ConflictException,
  BadRequestException,
  Get,
  Param,
  ParseUUIDPipe,
  Post,
  Query,
} from "@nestjs/common";
import { InjectQueue } from "@nestjs/bull";
import { ApiOperation, ApiQuery } from "@nestjs/swagger";
import type { Queue } from "bull";
import { AdminApi } from "../admin/auth/admin-api.decorator";
import {
  AdminMinRole,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { readBodyFlag } from "../admin/read-body-flag";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import {
  FACILITIES_IMPORT_JOB,
  FACILITIES_QUEUE,
  type FacilityBulkProgress,
  type FacilityImportJobData,
} from "../queues/processors/facilities-import.processor";
import { FacilitiesService, parseBoundaryRef } from "./facilities.service";

/** What the import queue is doing, for the admin page's status line. */
export interface FacilityQueueState {
  /** A bulk run that is waiting or running, with its tally so far. */
  bulk: {
    state: "waiting" | "active";
    progress: FacilityBulkProgress | null;
  } | null;
  /** Single parks waiting or running. */
  parkIds: string[];
  /** The last bulk run that stopped with an error, until the next one starts. */
  lastFailure: string | null;
}

/**
 * Import and review of the parks' OpenStreetMap facilities. Imports are jobs
 * on the `facilities` queue, one at a time; the routes here only add them.
 * docs/frontend/facilities.md.
 */
@AdminApi("admin/facilities", new NoCdnCacheInterceptor())
export class AdminFacilitiesController {
  constructor(
    private readonly facilities: FacilitiesService,
    @InjectQueue(FACILITIES_QUEUE)
    private readonly queue: Queue<FacilityImportJobData>,
  ) {}

  @Get()
  @ApiOperation({
    summary: "Every park with its facility import state and counts",
    description:
      "`queue` says whether a bulk run or single imports are waiting or " +
      "running. `bulkApprovable` marks the parks `approve-all` would take.",
  })
  async list() {
    return {
      queue: await this.queueState(),
      parks: await this.facilities.listParks(),
    };
  }

  @Post("import-all")
  @AdminMinRole("editor")
  @ApiOperation({
    summary: "Import every park's facilities from OpenStreetMap",
    description:
      "One job, park after park with a pause in between; about two Overpass " +
      "queries per park. Takes only the parks whose last import did not go " +
      "through, so starting it again resumes; `all: true` re-reads every " +
      "park. 409 while a bulk run is waiting or running.",
  })
  async importAll(@Body() body: { all?: unknown } = {}) {
    const all = body?.all == null ? false : readBodyFlag(body.all);
    if (all === undefined) {
      throw new BadRequestException("all must be true or false");
    }
    if ((await this.queueState()).bulk) {
      throw new ConflictException("A bulk import is already queued or running");
    }
    // One attempt: a retry would start the run again from its first park.
    await this.queue.add(FACILITIES_IMPORT_JOB, { all }, { attempts: 1 });
    return { message: "Facility import queued", queue: FACILITIES_QUEUE, all };
  }

  @Post("approve-all")
  @AdminMinRole("editor")
  @ApiOperation({
    summary: "Approve every park that passes the bulk rule",
    description:
      "The rule: imported and not yet approved, last import went through, " +
      "at least three visible points, all of them inside the outline. " +
      "`dryRun` defaults to true and only answers with the parks it would take.",
  })
  async approveAll(
    @Body() body: { dryRun?: unknown } = {},
    @CurrentAdmin() admin: AdminPrincipal,
  ) {
    const dryRun = body?.dryRun == null ? true : readBodyFlag(body.dryRun);
    if (dryRun === undefined) {
      throw new BadRequestException("dryRun must be true or false");
    }
    return this.facilities.approveAll(admin.email, dryRun);
  }

  @Get(":parkId")
  @ApiOperation({
    summary: "A park's imported facilities, its outline and review state",
    description: "Every row, hidden ones included. Read-only.",
  })
  review(@Param("parkId", ParseUUIDPipe) parkId: string) {
    return this.facilities.review(parkId);
  }

  @Post(":parkId/import")
  @AdminMinRole("editor")
  @ApiOperation({
    summary: "Queue this park's import from OpenStreetMap",
    description:
      "Idempotent: new points in, changed ones updated, vanished ones out, " +
      "hidden flags kept. A new or visibly changed point puts an approved " +
      "park back to `imported` until someone approves it again. The job " +
      "waits behind a running bulk import; the park's row says how it ended.",
  })
  @ApiQuery({
    name: "boundary",
    required: false,
    description:
      "The park's outline in OSM (`way/31052062`, `relation/123`) when the " +
      "name match finds none or the wrong one. Remembered for later imports.",
  })
  async import(
    @Param("parkId", ParseUUIDPipe) parkId: string,
    @Query("boundary") boundary?: string,
  ) {
    await this.queue.add(
      FACILITIES_IMPORT_JOB,
      { parkId, boundary: boundary ? parseBoundaryRef(boundary) : undefined },
      { attempts: 1 },
    );
    return {
      message: "Facility import queued",
      queue: FACILITIES_QUEUE,
      parkId,
    };
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

  private async queueState(): Promise<FacilityQueueState> {
    const [active, waiting, failed] = await Promise.all([
      this.queue.getJobs(["active"]),
      this.queue.getJobs(["waiting", "delayed"]),
      this.queue.getJobs(["failed"], 0, 0),
    ]);
    const state: FacilityQueueState = {
      bulk: null,
      parkIds: [],
      lastFailure: null,
    };
    for (const [jobs, name] of [
      [waiting, "waiting"],
      [active, "active"],
    ] as const) {
      for (const job of jobs) {
        if (!job) continue;
        if (job.data.parkId) state.parkIds.push(job.data.parkId);
        else {
          const progress: unknown = job.progress();
          state.bulk = {
            state: name,
            progress:
              progress && typeof progress === "object"
                ? (progress as FacilityBulkProgress)
                : null,
          };
        }
      }
    }
    const last = failed[0];
    if (!state.bulk && last && !last.data.parkId) {
      state.lastFailure = last.failedReason ?? null;
    }
    return state;
  }
}
