import { createVerify, generateKeyPairSync } from "crypto";
import {
  GoogleAuthError,
  ServiceAccountTokens,
  signAssertion,
} from "./google-auth";
import { GOOGLE_TOKEN_URL, GSC_SCOPE } from "./search-console.constants";

const { publicKey, privateKey } = generateKeyPairSync("rsa", {
  modulusLength: 2048,
  publicKeyEncoding: { type: "spki", format: "pem" },
  privateKeyEncoding: { type: "pkcs8", format: "pem" },
});
const ACCOUNT = {
  clientEmail: "gsc@park-fan.iam.gserviceaccount.com",
  privateKey,
};

function decode(part: string) {
  return JSON.parse(Buffer.from(part, "base64url").toString("utf8"));
}

describe("signAssertion", () => {
  it("is an RS256 JWT for the account, the scope and the token endpoint, valid for an hour", () => {
    const jwt = signAssertion(ACCOUNT, GSC_SCOPE, 1_790_000_000);
    const [header, claims, signature] = jwt.split(".");
    expect(decode(header)).toEqual({ alg: "RS256", typ: "JWT" });
    expect(decode(claims)).toEqual({
      iss: ACCOUNT.clientEmail,
      scope: GSC_SCOPE,
      aud: GOOGLE_TOKEN_URL,
      iat: 1_790_000_000,
      exp: 1_790_003_600,
    });
    const verify = createVerify("RSA-SHA256").update(`${header}.${claims}`);
    expect(verify.verify(publicKey, Buffer.from(signature, "base64url"))).toBe(
      true,
    );
  });
});

describe("ServiceAccountTokens", () => {
  let fetchMock: jest.SpyInstance;
  let clock: number;
  const now = () => clock;

  const answer = (body: object, status = 200) =>
    Promise.resolve(
      new Response(JSON.stringify(body), {
        status,
        headers: { "Content-Type": "application/json" },
      }),
    );

  beforeEach(() => {
    clock = 1_790_000_000_000;
    fetchMock = jest.spyOn(globalThis, "fetch");
  });
  afterEach(() => jest.restoreAllMocks());

  it("trades the assertion for a token and keeps it until a minute before it expires", async () => {
    fetchMock.mockImplementation(() =>
      answer({ access_token: "ya29.token", expires_in: 3600 }),
    );
    const tokens = new ServiceAccountTokens(ACCOUNT, GSC_SCOPE, now);

    expect(await tokens.token()).toBe("ya29.token");
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(GOOGLE_TOKEN_URL);
    const body = new URLSearchParams(String(init.body));
    expect(body.get("grant_type")).toBe(
      "urn:ietf:params:oauth:grant-type:jwt-bearer",
    );
    expect(body.get("assertion")?.split(".")).toHaveLength(3);

    clock += 3_500_000;
    await tokens.token();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    clock += 100_000;
    await tokens.token();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("shares one request between callers that ask together", async () => {
    fetchMock.mockImplementation(() =>
      answer({ access_token: "ya29.token", expires_in: 3600 }),
    );
    const tokens = new ServiceAccountTokens(ACCOUNT, GSC_SCOPE, now);
    const all = await Promise.all([
      tokens.token(),
      tokens.token(),
      tokens.token(),
    ]);
    expect(new Set(all)).toEqual(new Set(["ya29.token"]));
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("fetches a new one after invalidate", async () => {
    fetchMock.mockImplementation(() =>
      answer({ access_token: "ya29.token", expires_in: 3600 }),
    );
    const tokens = new ServiceAccountTokens(ACCOUNT, GSC_SCOPE, now);
    await tokens.token();
    tokens.invalidate();
    await tokens.token();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("says what Google refused, without the assertion", async () => {
    fetchMock.mockImplementation(() =>
      answer(
        { error: "invalid_grant", error_description: "Invalid JWT Signature." },
        400,
      ),
    );
    const tokens = new ServiceAccountTokens(ACCOUNT, GSC_SCOPE, now);
    const failure = await tokens.token().catch((error: Error) => error);
    expect(failure).toBeInstanceOf(GoogleAuthError);
    expect((failure as Error).message).toContain("400 invalid_grant");
    expect((failure as Error).message).toContain("Invalid JWT Signature");
    expect((failure as Error).message).not.toMatch(/eyJ/);
  });

  it("says when Google cannot be reached, and tries again on the next call", async () => {
    fetchMock.mockRejectedValueOnce(new Error("ECONNRESET"));
    const tokens = new ServiceAccountTokens(ACCOUNT, GSC_SCOPE, now);
    await expect(tokens.token()).rejects.toThrow(
      /could not be reached: ECONNRESET/,
    );
    fetchMock.mockImplementation(() =>
      answer({ access_token: "ya29.again", expires_in: 3600 }),
    );
    expect(await tokens.token()).toBe("ya29.again");
  });

  it("says a key that cannot sign is not a whole key file", async () => {
    const tokens = new ServiceAccountTokens(
      {
        ...ACCOUNT,
        privateKey:
          "-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----",
      },
      GSC_SCOPE,
      now,
    );
    await expect(tokens.token()).rejects.toThrow(/could not sign/);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("knows whether it was made for an account", () => {
    const tokens = new ServiceAccountTokens(ACCOUNT, GSC_SCOPE, now);
    expect(tokens.isFor({ ...ACCOUNT })).toBe(true);
    expect(
      tokens.isFor({
        ...ACCOUNT,
        clientEmail: "other@x.iam.gserviceaccount.com",
      }),
    ).toBe(false);
  });
});
