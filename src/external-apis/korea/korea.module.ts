import { Module } from "@nestjs/common";
import { KasiClient } from "./kasi.client";
import { NeisClient } from "./neis.client";

@Module({
  providers: [NeisClient, KasiClient],
  exports: [NeisClient, KasiClient],
})
export class KoreaHolidaysModule {}
