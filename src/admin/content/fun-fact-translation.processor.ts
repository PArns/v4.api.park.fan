import { Process, Processor } from "@nestjs/bull";
import { Job } from "bull";
import type { FunFactLocale } from "../../parks/utils/curated-park-facts.util";
import { TRANSLATE_FUN_FACTS_JOB } from "./fun-fact-translation.constants";
import { FunFactTranslationService } from "./fun-fact-translation.service";

/**
 * Runs the fun-fact translation `curatePark` queues, on the queue the curation already owns
 * (`CuratedDataProcessor` handles its other jobs). A throw is retried by the job's own
 * `attempts` and backoff.
 */
@Processor("manual-metadata")
export class FunFactTranslationProcessor {
  constructor(private readonly translation: FunFactTranslationService) {}

  @Process(TRANSLATE_FUN_FACTS_JOB)
  async handle(
    job: Job<{ parkId: string; source: FunFactLocale }>,
  ): Promise<void> {
    await this.translation.translate(job.data.parkId, job.data.source);
  }
}
