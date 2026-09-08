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

  /** Upsert on `(subscriptionId, showId)` — following an already-followed show is a no-op write. */
  async upsert(subscriptionId: string, showId: string): Promise<ShowFollow> {
    const existing = await this.find(subscriptionId, showId);
    return (
      existing ??
      this.repository.save(this.repository.create({ subscriptionId, showId }))
    );
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
   * Every follow there is, for the job that walks them — same shape and same
   * reasoning as `PushService.allSubscriptions()`: this table is bounded by
   * browsers that opted in, not by traffic, and the notification job groups
   * these by `showId` in memory (`PushNotificationProcessor` already does the
   * equivalent grouping by `tripId`) rather than paying a second query.
   */
  async allFollows(): Promise<ShowFollow[]> {
    return this.repository.find();
  }
}
