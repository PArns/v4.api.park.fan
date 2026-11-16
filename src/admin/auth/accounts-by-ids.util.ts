/** An account's address and name, as lists of keys and requests show who holds them. */
export interface AccountName {
  email: string;
  displayName: string;
}

/**
 * The address and name of each account id, in one query; ids without an account are missing from
 * the map. Takes anything that runs SQL (a repository or a manager), so a service outside the
 * admin's module needs no new dependency for it.
 */
export async function accountsByIds(
  db: { query: (sql: string, params: unknown[]) => Promise<unknown> },
  ids: readonly string[],
): Promise<Map<string, AccountName>> {
  if (ids.length === 0) return new Map();
  const rows = (await db.query(
    `SELECT id, email, display_name FROM admin_users WHERE id = ANY($1::uuid[])`,
    [[...new Set(ids)]],
  )) as { id: string; email: string; display_name: string }[];
  return new Map(
    rows.map((row) => [
      row.id,
      { email: row.email, displayName: row.display_name },
    ]),
  );
}
