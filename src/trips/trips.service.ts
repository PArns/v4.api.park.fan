import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { LessThan, Repository } from "typeorm";
import { randomBytes } from "crypto";
import { Trip } from "./entities/trip.entity";

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
    if (!trip) return null;
    if (trip.expiresAt.getTime() <= Date.now()) return null;
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

  /** Rows past their expiry. Returns how many went. */
  async sweepExpired(): Promise<number> {
    const result = await this.tripRepository.delete({
      expiresAt: LessThan(new Date()),
    });
    const removed = result.affected ?? 0;
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
