import { Module } from "@nestjs/common";
import { ThemeParksModule } from "../themeparks/themeparks.module";
import { QueueTimesModule } from "../queue-times/queue-times.module";
import { WartezeitenModule } from "../wartezeiten/wartezeiten.module";
import { MultiSourceOrchestrator } from "./multi-source-orchestrator.service";
import { EntityMatcherService } from "./entity-matcher.service";
import { ConflictResolverService } from "./conflict-resolver.service";
import { ThemeParksDataSource } from "../themeparks/themeparks-data-source";
import { QueueTimesDataSource } from "../queue-times/queue-times-data-source";
import { WartezeitenDataSource } from "../wartezeiten/wartezeiten-data-source";

@Module({
  imports: [ThemeParksModule, QueueTimesModule, WartezeitenModule],
  providers: [
    MultiSourceOrchestrator,
    EntityMatcherService,
    ConflictResolverService,
    ThemeParksDataSource,
    QueueTimesDataSource,
    WartezeitenDataSource,
  ],
  exports: [
    MultiSourceOrchestrator,
    EntityMatcherService,
    ConflictResolverService,
    ThemeParksDataSource,
    QueueTimesDataSource,
    WartezeitenDataSource,
  ],
})
export class DataSourcesModule {
  constructor(
    private readonly orchestrator: MultiSourceOrchestrator,
    private readonly wikiSource: ThemeParksDataSource,
    private readonly qtSource: QueueTimesDataSource,
    private readonly wartezeitenSource: WartezeitenDataSource,
  ) {
    // Register data sources on module initialization
    this.orchestrator.registerSource(this.wikiSource);
    this.orchestrator.registerSource(this.qtSource);
    this.orchestrator.registerSource(this.wartezeitenSource);
  }
}
