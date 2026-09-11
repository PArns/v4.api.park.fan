import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { LessThan, Repository } from "typeorm";
import { randomBytes } from "crypto";
import { Trip } from "./entities/trip.entity";
import { PushSubscription } from "../push/entities/push-subscription.entity";

/**
 * Reading and writing a stored plan.
 *
 * Everything about authorisation is in one sentence: **the id is the
 * credential**, so it is 96 bits of `randomBytes` and nothing else stands
 * between a caller and a trip. That is a deliberate trade — there is no account
 * system for visitors and none is being built — and the only way it stays
 * honest is if the UI says so where somebody copies the link.
 */
@Injectable()
export class TripsService {
  private readonly logger = new Logger(TripsService.name);

  /**
   * How long an untouched trip lives.
   *
   * Pushed forward on every write, so a plan somebody keeps editing never
   * expires. Long because a plan is a record of days already walked as well as
   * days ahead — the panel greys a finished day rather than sweeping it, and
   * this is the same decision one layer down.
   */
  private static readonly TTL_DAYS = 400;

  constructor(
    @InjectRepository(Trip)
    private readonly tripRepository: Repository<Trip>,
  ) {}

  async create(payload: Record<string, unknown>): Promise<Trip> {
    const trip = this.tripRepository.create({
      id: TripsService.newId(),
      payload,
      expiresAt: TripsService.expiry(),
    });
    return this.tripRepository.save(trip);
  }

  /**
   * A trip by id, or `null`.
   *
   * An expired row reads as absent rather than as an error: whether the sweep
   * has run yet is this service's business and not the caller's, and a trip that
   * answers 404 one day and 410 the next for the same reason is worse than one
   * that is simply gone.
   */
  async find(id: string): Promise<Trip | null> {
    const trip = await this.tripRepository.findOne({ where: { id } });
    return TripsService.live(trip, Date.now());
  }

  /**
   * The one place a row becomes a trip that exists.
   *
   * `find` and `remove` both need it and `remove` has to ask inside its own
   * transaction, so the rule is here rather than copied — two copies would be
   * free to disagree about what "expired" means, on the same id, between two
   * verbs of one route.
   */
  private static live(trip: Trip | null, nowMs: number): Trip | null {
    if (!trip) return null;
    if (trip.expiresAt.getTime() <= nowMs) return null;
    return trip;
  }

  /**
   * Replace a trip's payload, and push its expiry out.
   *
   * A full replace, never a merge. The browser holds the whole plan and is the
   * only writer that knows what was deleted — merging server-side would
   * resurrect an entry somebody removed, which is the one edit a sync must
   * never undo.
   *
   * `null` when there is nothing at that id, so the caller can answer 404
   * instead of quietly creating a trip at an id the caller chose. That would
   * hand an attacker the ability to pick their own ids, and with it the ability
   * to overwrite a trip by guessing one.
   */
  async update(
    id: string,
    payload: Record<string, unknown>,
  ): Promise<Trip | null> {
    const trip = await this.find(id);
    if (!trip) return null;
    trip.payload = payload;
    trip.expiresAt = TripsService.expiry();
    return this.tripRepository.save(trip);
  }

  /**
   * Delete a trip, and clear every pointer at it. `false` when there was
   * nothing live at that id.
   *
   * Deleting matters because the alternative is worse than keeping the plan:
   * the browser forgets the id when push is switched off, so the visitor can no
   * longer reach the row while anybody who kept the id — a log, a backup, an old
   * device — still can. Switching off would make a plan unreachable rather than
   * gone.
   *
   * **The pointer is cleared, the subscription is not.** `push_subscriptions`
   * carries a loose `tripId` — a plain column, no foreign key, because a trip
   * has its own lifecycle and TTL — and the same row also serves that browser's
   * ride alerts and followed shows, which hang off `subscriptionId`. So this
   * does exactly what `PushService.unsubscribe(endpoint, tripId)` does for one
   * endpoint, one level wider: `tripId: null, topics: []` for every subscriber
   * of this trip. Deleting the row would take a browser's ride alerts with a
   * plan it has nothing to do with.
   *
   * Both writes in one transaction: a cleared pointer without the delete leaves
   * a plan nobody is told about, and a delete without the clear leaves
   * subscriptions the five-minute job walks forever for a trip that is gone.
   *
   * An expired trip counts as absent and is left to `sweepExpired`, which
   * clears the same pointers — `live` is the only place that decides what
   * exists, for both verbs.
   *
   * The lookup happens INSIDE the transaction and the DELETE's own `affected`
   * is what answers, not the read: with the check outside, two deletes racing
   * on one id would both see a trip and both answer 204 against a route that
   * documents 404 for an id with nothing behind it.
   */
  async remove(id: string): Promise<boolean> {
    return this.tripRepository.manager.transaction(async (manager) => {
      const trip = TripsService.live(
        await manager.findOne(Trip, { where: { id } }),
        Date.now(),
      );
      if (!trip) return false;

      const result = await manager.delete(Trip, id);
      if (!result.affected) return false;

      await manager.update(
        PushSubscription,
        { tripId: id },
        { tripId: null, topics: [] },
      );
      return true;
    });
  }

  /**
   * Rows past their expiry. Returns how many went.
   *
   * Clears the pointers at them first, for the reason `remove` does: a
   * subscription left holding the id of a trip this just deleted is walked by
   * the five-minute notification job for the life of the browser, and its
   * `tripId IS NOT NULL` query cannot tell it from a live one.
   *
   * Both statements read the same instant and run in one transaction, so a row
   * cannot expire between them and be deleted with its pointer left standing.
   * Set-based rather than a list of ids: how many trips expire in a day is not
   * a number this bounds.
   */
  async sweepExpired(): Promise<number> {
    const now = new Date();
    const removed = await this.tripRepository.manager.transaction(
      async (manager) => {
        await manager
          .createQueryBuilder()
          .update(PushSubscription)
          .set({ tripId: null, topics: [] })
          .where(
            `"tripId" IN (SELECT id FROM trips WHERE "expiresAt" < :now)`,
            { now },
          )
          .execute();
        const result = await manager.delete(Trip, {
          expiresAt: LessThan(now),
        });
        return result.affected ?? 0;
      },
    );
    if (removed > 0) this.logger.log(`Swept ${removed} expired trip(s)`);
    return removed;
  }

  /**
   * A new id: 12 random bytes as base64url, so 16 characters and 96 bits.
   *
   * Random and nothing else. An id derived from the plan — a park slug and a
   * date, a hash of the payload — would be guessable in an afternoon, and since
   * the id is the whole of the authorisation, guessing one is reading somebody's
   * trip. `randomBytes` and not `Math.random`, for the same reason.
   *
   * base64url rather than hex so it fits in a message without wrapping, and
   * rather than a custom alphabet so nothing has to re-derive the mapping.
   */
  private static newId(): string {
    return randomBytes(12).toString("base64url");
  }

  private static expiry(): Date {
    return new Date(Date.now() + TripsService.TTL_DAYS * 86_400_000);
  }
}
