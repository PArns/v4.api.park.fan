import { Global, Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RedisModule } from "../../common/redis/redis.module";
import { AdminUser } from "./entities/admin-user.entity";
import { AdminAuditLog } from "./entities/admin-audit-log.entity";
import { AdminAuthService } from "./admin-auth.service";
import { AdminSessionStore } from "./admin-session.store";
import { AdminAuditService } from "./admin-audit.service";
import { AdminLoginRateLimitService } from "./admin-login-rate-limit.service";
import { AdminAuthGuard } from "./admin-auth.guard";
import { AdminAuthController } from "./admin-auth.controller";
import { AdminAuditInterceptor } from "./admin-audit.interceptor";

/**
 * Identity for the admin surface: accounts, sessions, roles, audit.
 *
 * Global so that any module gaining an administrative endpoint can put
 * `AdminAuthGuard` on it without importing this module and without a circular
 * import back into AdminModule — the guard is the one piece every admin
 * controller needs and nothing else should have to think about.
 */
@Global()
@Module({
  imports: [TypeOrmModule.forFeature([AdminUser, AdminAuditLog]), RedisModule],
  controllers: [AdminAuthController],
  providers: [
    AdminAuthService,
    AdminSessionStore,
    AdminAuditService,
    AdminLoginRateLimitService,
    AdminAuthGuard,
    AdminAuditInterceptor,
  ],
  exports: [
    AdminAuthService,
    AdminSessionStore,
    AdminAuditService,
    AdminLoginRateLimitService,
    AdminAuthGuard,
    AdminAuditInterceptor,
  ],
})
export class AdminAuthModule {}
