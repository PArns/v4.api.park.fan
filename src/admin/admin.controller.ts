import { Controller, Post, HttpCode, HttpStatus, Inject } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";

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
        @Inject(REDIS_CLIENT) private readonly redis: Redis,
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

    /**
     * Flush park-related Redis cache
     *
     * Clears only park-related cached data (schedules, wait times, analytics, etc.)
     * while preserving Bull queue jobs and system caches.
     */
    @Post("flush-cache")
    @HttpCode(HttpStatus.OK)
    @ApiOperation({
        summary: "Flush park cache",
        description:
            "Clears park-related cached data (schedules, wait times, analytics) without affecting queue jobs",
    })
    @ApiResponse({
        status: 200,
        description: "Park cache flushed successfully",
    })
    async flushCache(): Promise<{ message: string; keysDeleted: number }> {
        // Define park-related cache key patterns
        const patterns = [
            "schedule:*",
            "park:*",
            "parks:*",
            "wait-times:*",
            "analytics:*",
            "occupancy:*",
            "predictions:*",
            "holiday:*",
            "attraction:*",
            "show:*",
            "restaurant:*",
            "weather:*",
            "search:*",
        ];

        let totalDeleted = 0;

        // Delete keys matching each pattern
        for (const pattern of patterns) {
            const keys = await this.redis.keys(pattern);
            if (keys.length > 0) {
                await this.redis.del(...keys);
                totalDeleted += keys.length;
            }
        }

        return {
            message: "Park cache flushed successfully",
            keysDeleted: totalDeleted,
        };
    }
}
