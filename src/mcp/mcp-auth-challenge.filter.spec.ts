import {
  ArgumentsHost,
  ForbiddenException,
  UnauthorizedException,
} from "@nestjs/common";
import { McpAuthChallengeFilter } from "./mcp-auth-challenge.filter";

function host(headers: Record<string, string>) {
  const response = {
    headersSent: false,
    headers: {} as Record<string, string>,
    statusCode: 0,
    body: undefined as unknown,
    setHeader(name: string, value: string) {
      this.headers[name] = value;
    },
    status(code: number) {
      this.statusCode = code;
      return this;
    },
    json(body: unknown) {
      this.body = body;
      return this;
    },
  };
  const argumentsHost = {
    switchToHttp: () => ({
      getRequest: () => ({ headers, query: {}, method: "POST" }),
      getResponse: () => response,
    }),
  } as unknown as ArgumentsHost;
  return { argumentsHost, response };
}

describe("McpAuthChallengeFilter", () => {
  beforeEach(() => {
    process.env.API_PUBLIC_URL = "https://api.park.fan";
  });
  afterAll(() => {
    delete process.env.API_PUBLIC_URL;
  });

  it("names the resource metadata when no token came", () => {
    const { argumentsHost, response } = host({});
    new McpAuthChallengeFilter().catch(
      new UnauthorizedException(),
      argumentsHost,
    );
    expect(response.statusCode).toBe(401);
    expect(response.headers["WWW-Authenticate"]).toBe(
      'Bearer resource_metadata="https://api.park.fan/.well-known/oauth-protected-resource/v1/mcp", scope="admin:read"',
    );
    expect(response.headers["Cache-Control"]).toBe("no-store");
  });

  it("says invalid_token for a token that did not resolve", () => {
    const { argumentsHost, response } = host({ authorization: "Bearer pfa_x" });
    new McpAuthChallengeFilter().catch(
      new UnauthorizedException(),
      argumentsHost,
    );
    expect(response.headers["WWW-Authenticate"]).toContain(
      'error="invalid_token"',
    );
    expect(response.body).toMatchObject({ error: "invalid_token" });
  });

  it("answers a refusal as insufficient_scope without breaking the header", () => {
    const { argumentsHost, response } = host({ authorization: "Bearer pfa_x" });
    new McpAuthChallengeFilter().catch(
      new ForbiddenException('This action needs the "editor" role'),
      argumentsHost,
    );
    expect(response.statusCode).toBe(403);
    const header = response.headers["WWW-Authenticate"];
    expect(header).toContain('error="insufficient_scope"');
    expect(header).toContain("needs the 'editor' role");
  });
});
