import { Controller, Get } from "@nestjs/common";
import { ApiOperation, ApiTags } from "@nestjs/swagger";
import { getApiPublicUrl, getMcpResourceUrl } from "../../config/oauth.config";
import { DEFAULT_ADMIN_TOKEN_SCOPE } from "../auth/admin-api-token.constants";

/**
 * Discovery documents, at the host root (excluded from the `/v1` prefix in
 * `swagger-config.ts`): an MCP client reads the protected-resource metadata
 * named in the 401, then the authorization server's metadata, and from there
 * knows every endpoint it needs.
 */
@ApiTags("oauth")
@Controller(".well-known")
export class OAuthMetadataController {
  @Get("oauth-protected-resource")
  @ApiOperation({ summary: "RFC 9728 protected-resource metadata" })
  protectedResource() {
    return protectedResourceMetadata();
  }

  /** The path-inserted form RFC 9728 §3.1 derives from the resource `/v1/mcp`. */
  @Get("oauth-protected-resource/v1/mcp")
  @ApiOperation({ summary: "RFC 9728 metadata for /v1/mcp" })
  protectedResourceForMcp() {
    return protectedResourceMetadata();
  }

  @Get("oauth-authorization-server")
  @ApiOperation({ summary: "RFC 8414 authorization-server metadata" })
  authorizationServer() {
    const issuer = getApiPublicUrl();
    return {
      issuer,
      authorization_endpoint: `${issuer}/v1/oauth/authorize`,
      token_endpoint: `${issuer}/v1/oauth/token`,
      registration_endpoint: `${issuer}/v1/oauth/register`,
      revocation_endpoint: `${issuer}/v1/oauth/revoke`,
      response_types_supported: ["code"],
      response_modes_supported: ["query"],
      grant_types_supported: ["authorization_code", "refresh_token"],
      code_challenge_methods_supported: ["S256"],
      token_endpoint_auth_methods_supported: ["none"],
      revocation_endpoint_auth_methods_supported: ["none"],
      scopes_supported: [DEFAULT_ADMIN_TOKEN_SCOPE],
      authorization_response_iss_parameter_supported: true,
      service_documentation:
        "https://github.com/PArns/v4.api.park.fan/blob/main/docs/admin/mcp.md",
    };
  }
}

function protectedResourceMetadata() {
  return {
    resource: getMcpResourceUrl(),
    authorization_servers: [getApiPublicUrl()],
    scopes_supported: [DEFAULT_ADMIN_TOKEN_SCOPE],
    bearer_methods_supported: ["header"],
    resource_name: "park.fan admin",
  };
}
