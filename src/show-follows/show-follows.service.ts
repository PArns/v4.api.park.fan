import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { ShowFollow } from "./entities/show-follow.entity";
import { Show } from "../shows/entities/show.entity";
import { Park } from "../parks/entities/park.entity";

/** How many shows one browser may follow — a visitor follows a handful of shows. */
export const MAX_SHOW_FOLLOWS_PER_SUBSCRIPTION = 100;

export interface ShowForFollow {
  show: Show;
  park: Park;
}

@Injectable()
export class ShowFollowsService {
  /**
   * How long a passed performance's follow is kept before the sweep takes it.
   * See {@link ShowFollowsService.sweepExpired} for why it is not zero.
   */
  private static readonly EXPIRY_GRACE_HOURS = 24;

  constructor(
    @InjectRepository(ShowFollow)
    private readonly repository: Repository<ShowFollow>,
    @InjectRepository(Show)
    private readonly showRepository: Repository<Show>,
  ) {}

  async findShowForFollow(showId: string): Promise<ShowForFollow | null> {
    const show = await this.showRepository.findOne({
      where: { id: showId },
      relations: { park: true },
    });
    if (!show || !show.park) return null;
    return { show, park: show.park };
  }

  async countForSubscription(subscriptionId: string): Promise<number> {
    return this.repository.count({ where: { subscriptionId } });
  }

  async find(
    subscriptionId: string,
    showId: string,
  ): Promise<ShowFollow | null> {
    return this.repository.findOne({ where: { subscriptionId, showId } });
  }

  /**
   * Upsert on `(subscriptionId, showId)` — a real database upsert, not
   * read-then-write: two concurrent POSTs following the same show used to
   * both pass the `find` check and then have the second `save()`'s INSERT
   * hit the unique index and surface as a bare 500.
   *
   * `DO UPDATE`, not the `DO NOTHING` this used to do. That was right while
   * a follow carried nothing but its two keys; `startTime` is a second field
   * and a real choice, so re-following the same show to move the reminder
   * from the 17:30 performance to the 19:10 one has to overwrite. Ignoring
   * it would leave the visitor looking at a dialog that says 19:10 and a row
   * that still says 17:30.
   */
  async upsert(
    subscriptionId: string,
    showId: string,
    startTime: Date | null,
  ): Promise<ShowFollow> {
    await this.repository
      .createQueryBuilder()
      .insert()
      .into(ShowFollow)
      .values({ subscriptionId, showId, startTime })
      .orUpdate(["startTime"], ["subscriptionId", "showId"])
      .execute();
    return (await this.find(subscriptionId, showId))!;
  }

  /**
   * Delete follows whose chosen performance is over.
   *
   * A follow that named a showtime is a one-off: it identifies one
   * performance on one day, so the moment that instant passes the row can
   * never match another notification and is nothing but weight. It is not
   * merely inert either — it is visible, on `/alerts`, as a reminder for a
   * show that finished last Tuesday, which is worse than gone.
   *
   * An open-ended follow (`startTime IS NULL`) is deliberately untouched:
   * that one means "whichever performance is next" and has no end. So does a
   * ride alert, which is why `RideAlert` has no sweep of its own.
   *
   * The grace period is not decoration. The notification job runs on a
   * five-minute tick and its late window reaches to eight minutes before a
   * performance, so a row deleted the second its showtime passed would race a
   * tick that is legitimately still working on it. A day also leaves the row
   * on `/alerts` for the evening of the show itself, which is when somebody
   * is most likely to look at it.
   */
  async sweepExpired(now: Date = new Date()): Promise<number> {
    const cutoff = new Date(
      now.getTime() - ShowFollowsService.EXPIRY_GRACE_HOURS * 60 * 60 * 1000,
    );
    const result = await this.repository
      .createQueryBuilder()
      .delete()
      .from(ShowFollow)
      .where("startTime IS NOT NULL")
      .andWhere("startTime < :cutoff", { cutoff })
      .execute();
    return result.affected ?? 0;
  }

  /** Idempotent — unfollowing a show that is not followed is not an error. */
  async remove(subscriptionId: string, showId: string): Promise<void> {
    await this.repository.delete({ subscriptionId, showId });
  }

  /** Every followed show for one subscription, with the show+park to render it. */
  async listForSubscription(subscriptionId: string): Promise<ShowFollow[]> {
    return this.repository.find({
      where: { subscriptionId },
      relations: { show: { park: true } },
      order: { createdAt: "ASC" },
    });
  }

  /**
   * Every follow there is, for the job that walks them — same reasoning
   * `PushService.subscriptionsWithTrip()` rests on: this table is bounded by
   * browsers that opted in, not by traffic, and the notification job groups
   * these by `showId` in memory (`PushNotificationProcessor` already does the
   * equivalent grouping by `tripId`) rather than paying a second query.
   * Unfiltered rather than query-scoped like that sibling, because every row
   * here IS a follow worth walking — there is no trip-less equivalent to
   * exclude.
   */
  async allFollows(): Promise<ShowFollow[]> {
    return this.repository.find();
  }
}
