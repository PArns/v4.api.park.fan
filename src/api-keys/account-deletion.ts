import { Injectable, type OnModuleInit } from "@nestjs/common";
import { AdminAuthService } from "../admin/auth/admin-auth.service";
import { ApiKeyRequestService } from "./api-key-request.service";
import { ApiKeyService } from "./api-key.service";

/**
 * Erases a deleted account's API keys and key requests. Registered with the
 * account service rather than called from it, because this module guards its
 * routes with the admin's and an import the other way would close the circle.
 */
@Injectable()
export class ApiKeyAccountDeletion implements OnModuleInit {
  constructor(
    private readonly accounts: AdminAuthService,
    private readonly keys: ApiKeyService,
    private readonly requests: ApiKeyRequestService,
  ) {}

  onModuleInit(): void {
    this.accounts.onAccountDeletion(async (userId) => ({
      apiKeys: await this.keys.deleteAllOwnedBy(userId),
      keyRequests: await this.requests.removeAllForUser(userId),
    }));
  }
}
