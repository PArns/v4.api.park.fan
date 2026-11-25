import {
  Body,
  Delete,
  Get,
  HttpCode,
  HttpException,
  HttpStatus,
  Post,
  Put,
} from "@nestjs/common";
import { ApiOperation } from "@nestjs/swagger";
import { z } from "zod";
import { AdminApi } from "../admin/auth/admin-api.decorator";
import { SelfAudited } from "../admin/auth/admin-audit.interceptor";
import { AdminAuditService } from "../admin/auth/admin-audit.service";
import {
  AdminMinRole,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { ApnsAdminService, type ApnsStatusView } from "./apns-admin.service";

/**
 * Only the shape: what the fields must be is said in German by
 * `ApnsAdminService.upload`, and a refusal here never repeats a value.
 */
const uploadSchema = z.object({
  content: z.string().min(1).max(20_000),
  filename: z.string().max(200).optional(),
  keyId: z.string().trim().max(40).optional(),
  teamId: z.string().trim().max(40).optional(),
  bundleId: z.string().trim().max(200).optional(),
});

const testSchema = z.object({ deviceId: z.uuid() });

function parse<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value);
  if (!result.success) {
    throw new HttpException(
      z.prettifyError(result.error),
      HttpStatus.BAD_REQUEST,
    );
  }
  return result.data;
}

/** What an audit row says of a key: which one, never the key. */
function keyFacts(status: ApnsStatusView): Record<string, unknown> {
  const { source, keyId, teamId, bundleId, fingerprint } = status;
  return { source, keyId, teamId, bundleId, fingerprint };
}

/**
 * Push to the iOS app in the admin: the APNs key file is uploaded here
 * instead of set as environment variables. Reading is for every account,
 * every action for owners. The key never comes back, and the upload and the
 * removal write their own audit rows, which name the key by its fingerprint.
 */
@AdminApi("admin/push/apns")
export class AdminPushController {
  constructor(
    private readonly apns: ApnsAdminService,
    private readonly audit: AdminAuditService,
  ) {}

  @Get()
  @ApiOperation({
    summary: "The APNs key in use, Apple's last answers, devices and errors",
  })
  status(): Promise<ApnsStatusView> {
    return this.apns.status();
  }

  @Put("key")
  @AdminMinRole("owner")
  @SelfAudited()
  @ApiOperation({
    summary: "Upload the APNs key file (.p8) and check it against Apple",
    description:
      "`content` is the file's text, `filename` its name: the key id is read " +
      "from `AuthKey_<KEYID>.p8` unless `keyId` is sent. `teamId` and " +
      "`bundleId` default to the app's own.",
  })
  async uploadKey(
    @Body() body: unknown,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<ApnsStatusView> {
    const before = keyFacts(await this.apns.status());
    const status = await this.apns.upload(
      parse(uploadSchema, body),
      admin.email,
    );
    await this.audit.record({
      actor: admin,
      action: "push.apns-key.upload",
      entityType: "system",
      entityLabel: "APNs-Schlüssel",
      before,
      after: keyFacts(status),
    });
    return status;
  }

  @Delete("key")
  @AdminMinRole("owner")
  @SelfAudited()
  @ApiOperation({
    summary:
      "Delete the uploaded APNs key; the APNS_* variables apply again, if set",
  })
  async removeKey(
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<ApnsStatusView> {
    const before = keyFacts(await this.apns.status());
    const status = await this.apns.remove();
    await this.audit.record({
      actor: admin,
      action: "push.apns-key.remove",
      entityType: "system",
      entityLabel: "APNs-Schlüssel",
      before,
      after: keyFacts(status),
    });
    return status;
  }

  @Post("check")
  @HttpCode(HttpStatus.OK)
  @AdminMinRole("owner")
  @ApiOperation({
    summary:
      "Ask Apple, in production and sandbox, whether it accepts the key; reaches no device",
  })
  async check(@CurrentAdmin() admin: AdminPrincipal): Promise<ApnsStatusView> {
    await this.apns.limit(admin.userId ?? admin.email);
    await this.apns.check();
    return this.apns.status();
  }

  @Get("devices")
  @ApiOperation({
    summary: "The app installs registered last; of a token only its end",
  })
  devices() {
    return this.apns.devices();
  }

  @Post("test")
  @HttpCode(HttpStatus.OK)
  @AdminMinRole("owner")
  @ApiOperation({
    summary: "Send a test notification to one registered device",
  })
  async test(@Body() body: unknown, @CurrentAdmin() admin: AdminPrincipal) {
    const { deviceId } = parse(testSchema, body);
    await this.apns.limit(admin.userId ?? admin.email);
    return this.apns.test(deviceId);
  }
}
