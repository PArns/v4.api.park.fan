import { Controller, Post, HttpCode, HttpStatus } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";

/**
 * Admin Controller
 *
 * Administrative endpoints for manual operations.
 * These endpoints should be protected in production.
 */
@ApiTags("admin")
@Controller("admin")
export class AdminController {
    constructor(
        @InjectQueue("holidays") private holidaysQueue: Queue,
        @InjectQueue("park-metadata") private parkMetadataQueue: Queue,
    ) { }

    /**
     * Manually trigger holiday sync
     *
     * Forces a complete resync of all holidays from Nager.Date API.
     * Useful after code changes to holiday storage logic.
     */
    @Post("sync-holidays")
    @HttpCode(HttpStatus.ACCEPTED)
    @ApiOperation({
        summary: "Trigger holiday sync",
        description:
            "Manually triggers a complete resync of all holidays from Nager.Date API",
    })
    @ApiResponse({
        status: 202,
        description: "Holiday sync job queued successfully",
    })
    async triggerHolidaySync(): Promise<{ message: string; jobId: string }> {
        const job = await this.holidaysQueue.add(
            "fetch-holidays",
            {},
            { priority: 10 },
        );
        return {
            message: "Holiday sync job queued",
            jobId: job.id.toString(),
        };
    }

    /**
     * Manually trigger schedule gap filling for all parks
     *
     * Updates holiday/bridge day metadata in schedule entries.
     */
    @Post("fill-schedule-gaps")
    @HttpCode(HttpStatus.ACCEPTED)
    @ApiOperation({
        summary: "Fill schedule gaps",
        description:
            "Triggers schedule gap filling to update holiday/bridge day metadata",
    })
    @ApiResponse({
        status: 202,
        description: "Schedule gap filling job queued successfully",
    })
    async triggerScheduleGapFilling(): Promise<{
        message: string;
        jobId: string;
    }> {
        const job = await this.parkMetadataQueue.add(
            "fill-all-gaps",
            {},
            { priority: 5 },
        );
        return {
            message: "Schedule gap filling job queued",
            jobId: job.id.toString(),
        };
    }
}
