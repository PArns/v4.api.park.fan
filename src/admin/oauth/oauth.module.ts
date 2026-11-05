import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RedisModule } from "../../common/redis/redis.module";
import { AdminOAuthClient } from "./entities/admin-oauth-client.entity";
import { OAuthClientService } from "./oauth-client.service";
import { OAuthAuthorizationService } from "./oauth-authorization.service";
import { OAuthController } from "./oauth.controller";
import { OAuthMetadataController } from "./oauth-metadata.controller";
import { AdminOAuthConsentController } from "./admin-oauth-consent.controller";

/**
 * The OAuth 2.1 authorization server for the admin MCP: discovery, client
 * registration, authorize, consent, token and revocation. The tokens it
 * issues are stored by `AdminApiTokenStore` in the global `AdminAuthModule`.
 */
@Module({
  imports: [TypeOrmModule.forFeature([AdminOAuthClient]), RedisModule],
  controllers: [
    OAuthMetadataController,
    OAuthController,
    AdminOAuthConsentController,
  ],
  providers: [OAuthClientService, OAuthAuthorizationService],
})
export class OAuthModule {}
