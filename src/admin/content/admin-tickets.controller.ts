import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Patch,
  Post,
  Put,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import {
  ApiOperation,
  ApiParam,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import { AdminAuthGuard } from "../auth/admin-auth.guard";
import { AdminMinRole, CurrentAdmin } from "../auth/admin-auth.decorators";
import {
  AdminAuditInterceptor,
  SelfAudited,
} from "../auth/admin-audit.interceptor";
import type { AdminPrincipal } from "../auth/admin-principal";
import { AdminTicketService } from "./admin-ticket.service";
import {
  AdminTicketCatalogDto,
  AdminTicketProductDto,
  TicketArchiveDto,
  TicketOrderDto,
  TicketProductWriteDto,
  TicketValidationDto,
} from "./dto/ticket-product.dto";

/**
 * A trip region's ticket catalog in the admin: every product with its rules
 * and purchase links, checked and dry-run before a write, audited and
 * published after it. Reads for any admin, writes for editors. Contract:
 * docs/frontend/trip-regions.md#editing-the-catalog.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/content/regions/:region/tickets")
@UseGuards(AdminAuthGuard)
@UseInterceptors(AdminAuditInterceptor)
export class AdminTicketsController {
  constructor(private readonly tickets: AdminTicketService) {}

  @Get()
  @ApiOperation({
    summary:
      "A trip region's ticket products, archived ones included, in order",
  })
  @ApiParam({ name: "region", example: "orlando" })
  @ApiResponse({ status: 200, type: AdminTicketCatalogDto })
  list(@Param("region") region: string): Promise<AdminTicketCatalogDto> {
    return this.tickets.list(region);
  }

  @Post("validate")
  @AdminMinRole("editor")
  // Saves nothing, so there is nothing to audit.
  @SelfAudited()
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary:
      "Check a full product and plan a sample trip with it, without saving",
  })
  @ApiResponse({ status: 200, type: TicketValidationDto })
  validate(
    @Param("region") region: string,
    @Body() body: TicketProductWriteDto,
  ): Promise<TicketValidationDto> {
    return this.tickets.validate(region, { ...body });
  }

  @Put("order")
  @AdminMinRole("editor")
  @SelfAudited()
  @ApiOperation({
    summary: "Put every product of the region in a new order",
  })
  @ApiResponse({ status: 200, type: AdminTicketCatalogDto })
  @ApiResponse({
    status: 400,
    description: "A product missing, unknown or twice",
  })
  reorder(
    @Param("region") region: string,
    @Body() body: TicketOrderDto,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<AdminTicketCatalogDto> {
    return this.tickets.reorder(region, body.slugs, admin);
  }

  @Get(":slug")
  @ApiOperation({ summary: "One ticket product" })
  @ApiResponse({ status: 200, type: AdminTicketProductDto })
  @ApiResponse({ status: 404, description: "No such product in the region" })
  get(
    @Param("region") region: string,
    @Param("slug") slug: string,
  ): Promise<AdminTicketProductDto> {
    return this.tickets.get(region, slug);
  }

  @Post()
  @AdminMinRole("editor")
  @SelfAudited()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: "Create a ticket product",
    description:
      "Checked whole and dry-run on a sample trip; a 400 lists every problem by its field path.",
  })
  @ApiResponse({ status: 201, type: AdminTicketProductDto })
  @ApiResponse({
    status: 400,
    description: "{ statusCode, message, problems: [{ path, message }] }",
  })
  create(
    @Param("region") region: string,
    @Body() body: TicketProductWriteDto,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<AdminTicketProductDto> {
    return this.tickets.create(region, { ...body }, admin);
  }

  @Patch(":slug")
  @AdminMinRole("editor")
  @SelfAudited()
  @ApiOperation({
    summary: "Change a ticket product",
    description:
      "Any field but `slug`; the merged product is checked whole and dry-run like a create.",
  })
  @ApiResponse({ status: 200, type: AdminTicketProductDto })
  patch(
    @Param("region") region: string,
    @Param("slug") slug: string,
    @Body() body: TicketProductWriteDto,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<AdminTicketProductDto> {
    return this.tickets.update(region, slug, { ...body }, admin);
  }

  @Post(":slug/archive")
  @AdminMinRole("editor")
  @SelfAudited()
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Archive a ticket product",
    description:
      "Off the catalog route and never suggested; tickets= still takes it. A PATCH with active: true brings it back.",
  })
  @ApiResponse({ status: 200, type: AdminTicketProductDto })
  archive(
    @Param("region") region: string,
    @Param("slug") slug: string,
    @Body() body: TicketArchiveDto,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<AdminTicketProductDto> {
    return this.tickets.archive(region, slug, body.reason, admin);
  }
}
