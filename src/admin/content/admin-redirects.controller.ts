import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  HttpCode,
  HttpStatus,
  Post,
  Query,
  Res,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import {
  ApiOperation,
  ApiQuery,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import type { Response } from "express";
import { AdminAuthGuard } from "../auth/admin-auth.guard";
import { AdminMinRole, CurrentAdmin } from "../auth/admin-auth.decorators";
import {
  AdminAuditInterceptor,
  SelfAudited,
} from "../auth/admin-audit.interceptor";
import type { AdminPrincipal } from "../auth/admin-principal";
import {
  AdminRedirectService,
  type RedirectView,
} from "./admin-redirect.service";
import { CreateRedirectDto } from "./dto/redirect.dto";

/**
 * Redirects from a dead park or ride URL, created from the SEO not-found list
 * (PAR-1233). docs/admin/curation.md#redirects-from-a-dead-path.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/redirects")
@UseGuards(AdminAuthGuard)
@UseInterceptors(AdminAuditInterceptor)
export class AdminRedirectsController {
  constructor(private readonly redirects: AdminRedirectService) {}

  @Post()
  @AdminMinRole("editor")
  @SelfAudited()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: "Redirect a dead park or ride path to a park or ride",
    description:
      "Writes a slug alias, so the park or ride lookup answers fromPath with a 301.",
  })
  @ApiResponse({ status: 201, description: "Created" })
  @ApiResponse({
    status: 200,
    description: "The same redirect already existed; nothing was written",
  })
  @ApiResponse({ status: 400, description: "Bad id, path shape or target" })
  @ApiResponse({ status: 404, description: "Target not found" })
  @ApiResponse({
    status: 409,
    description: "fromPath is a live page or redirects elsewhere",
  })
  async create(
    @Body() body: CreateRedirectDto,
    @CurrentAdmin() admin: AdminPrincipal,
    @Res({ passthrough: true }) res: Response,
  ): Promise<RedirectView> {
    const { created, redirect } = await this.redirects.create(
      body.fromPath,
      body.target,
      admin,
    );
    if (!created) res.status(HttpStatus.OK);
    return redirect;
  }

  @Delete()
  @AdminMinRole("editor")
  @SelfAudited()
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Remove a hand-made redirect" })
  @ApiQuery({ name: "fromPath", required: true })
  @ApiResponse({ status: 204, description: "Removed" })
  @ApiResponse({ status: 404, description: "No redirect at fromPath" })
  @ApiResponse({
    status: 409,
    description: "Recorded by a rename, merge or the sync; not removable here",
  })
  async remove(
    @Query("fromPath") fromPath: string | undefined,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<void> {
    if (!fromPath) throw new BadRequestException("fromPath is required");
    await this.redirects.remove(fromPath, admin);
  }
}
