import { ensureTableStorageOptions } from "./table-storage-options";

/**
 * Storage settings `synchronize` cannot express. What is pinned: written only
 * when missing, never interpolated from anything but a bare table name and
 * `name=number` options, and a failure never stops the start.
 */
describe("ensureTableStorageOptions", () => {
  const logger = { log: jest.fn(), warn: jest.fn() };
  const options = ["fillfactor=80", "autovacuum_vacuum_scale_factor=0.01"];

  it("writes the options a table lacks, and leaves one alone that has them", async () => {
    const query = jest.fn().mockResolvedValueOnce([{ reloptions: null }]);
    await ensureTableStorageOptions(
      { query },
      "site_event_days",
      options,
      logger,
    );
    expect(query).toHaveBeenLastCalledWith(
      "ALTER TABLE site_event_days SET (fillfactor=80, autovacuum_vacuum_scale_factor=0.01)",
    );

    query.mockReset().mockResolvedValueOnce([{ reloptions: [...options] }]);
    await ensureTableStorageOptions(
      { query },
      "site_event_days",
      options,
      logger,
    );
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("logs a failure instead of throwing it", async () => {
    const query = jest.fn().mockRejectedValue(new Error("no such relation"));
    await expect(
      ensureTableStorageOptions({ query }, "t", options, logger),
    ).resolves.toBeUndefined();
    expect(logger.warn).toHaveBeenCalled();
  });

  it("refuses a table name or an option that is not a bare word", async () => {
    const query = jest.fn();
    await expect(
      ensureTableStorageOptions({ query }, "t; DROP TABLE x", options, logger),
    ).rejects.toThrow();
    await expect(
      ensureTableStorageOptions({ query }, "t", ["fillfactor=80)"], logger),
    ).rejects.toThrow();
    expect(query).not.toHaveBeenCalled();
  });
});
