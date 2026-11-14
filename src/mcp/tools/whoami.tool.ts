import { defineMcpTool } from "../mcp-tool";

export const whoamiTool = defineMcpTool({
  name: "whoami",
  title: "Who am I",
  description:
    "The park.fan admin account this connection acts for: email, name, the role it may use here " +
    "(viewer with the admin:read scope; with admin:write the account's own role up to editor, " +
    "which writes; with admin:owner up to owner) and how it signed in (personal access token or OAuth grant, with the name the " +
    "OAuth client registered itself under, unverified, and the scope). Call it to check the " +
    "connection works.",
  input: {},
  async handler(_input, { admin }) {
    return {
      email: admin.email,
      displayName: admin.displayName,
      effectiveRole: admin.role,
      credential: admin.apiToken
        ? {
            kind: admin.apiToken.kind,
            client: admin.apiToken.clientName,
            scope: admin.apiToken.scope,
          }
        : null,
    };
  },
});
