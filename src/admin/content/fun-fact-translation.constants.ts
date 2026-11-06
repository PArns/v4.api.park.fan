import type { AdminPrincipal } from "../auth/admin-principal";

/** The `manual-metadata` job that translates a park's fun facts after a save. */
export const TRANSLATE_FUN_FACTS_JOB = "translate-fun-facts";

/**
 * Who the machine translation writes as: its audit rows name this address, which is how history
 * tells them apart, and `curatePark` neither clears machine marks nor queues a translation for it.
 */
export const FUN_FACT_TRANSLATOR: AdminPrincipal = {
  userId: null,
  email: "translator@park.fan",
  displayName: "Automatic translation",
  role: "editor",
  sessionToken: null,
  legacy: false,
  ip: null,
  mustChangePassword: false,
  mustEnrolTotp: false,
};
