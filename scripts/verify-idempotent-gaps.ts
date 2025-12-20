import { NestFactory } from "@nestjs/core";
import { AppModule } from "../src/app.module";
import { ParksService } from "../src/parks/parks.service";
import { HolidaysService } from "../src/holidays/holidays.service";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import {
  ScheduleEntry,
  ScheduleType,
} from "../src/parks/entities/schedule-entry.entity";
import { Holiday } from "../src/holidays/entities/holiday.entity";
import { Park } from "../src/parks/entities/park.entity";
import { getRepositoryToken } from "@nestjs/typeorm";

async function verifyIdempotentGapFilling() {
  const app = await NestFactory.createApplicationContext(AppModule);
  const parksService = app.get(ParksService);
  const holidaysService = app.get(HolidaysService);
  const scheduleRepo = app.get<Repository<ScheduleEntry>>(
    getRepositoryToken(ScheduleEntry),
  );
  const holidayRepo = app.get<Repository<Holiday>>(getRepositoryToken(Holiday));
  const parkRepo = app.get<Repository<Park>>(getRepositoryToken(Park));

  const parkSlug = "phantasialand";
  const park = await parkRepo.findOne({ where: { slug: parkSlug } });

  if (!park) {
    console.error("Park not found");
    await app.close();
    return;
  }

  const testDateStr = "2025-12-26";
  const testDate = new Date(testDateStr);

  console.log("--- Step 1: Cleaning up existing test data ---");
  await scheduleRepo.delete({ parkId: park.id, date: testDate });
  await holidayRepo.delete({ country: park.countryCode, date: testDate });

  console.log("--- Step 2: Creating schedule entry WITHOUT holiday info ---");
  await scheduleRepo.save({
    parkId: park.id,
    date: testDate,
    scheduleType: ScheduleType.OPERATING,
    isHoliday: false,
    holidayName: null,
    isBridgeDay: false,
    openingTime: new Date(`${testDateStr}T09:00:00Z`),
    closingTime: new Date(`${testDateStr}T18:00:00Z`),
  });

  console.log(
    "--- Step 3: Running fillScheduleGaps (should do nothing yet) ---",
  );
  await parksService.fillScheduleGaps(park.id);

  let entry = await scheduleRepo.findOne({
    where: { parkId: park.id, date: testDate },
  });
  console.log(
    "Entry after 1st gap fill:",
    entry
      ? { isHoliday: entry.isHoliday, holidayName: entry.holidayName }
      : "NULL",
  );

  console.log("--- Step 4: Adding Holiday to DB ---");
  await holidayRepo.save({
    country: park.countryCode,
    date: testDate,
    localName: "Zweiter Weihnachtstag",
    name: "St. Stephen's Day",
    isNationwide: true,
    holidayType: "public",
    externalId: `test-holiday-${park.countryCode}-${testDateStr}`,
  });

  console.log(
    "--- Step 5: Running fillScheduleGaps AGAIN (should update existing entry) ---",
  );
  await parksService.fillScheduleGaps(park.id);

  entry = await scheduleRepo.findOne({
    where: { parkId: park.id, date: testDate },
  });
  console.log(
    "Entry after 2nd gap fill:",
    entry
      ? { isHoliday: entry.isHoliday, holidayName: entry.holidayName }
      : "NULL",
  );

  if (
    entry &&
    entry.isHoliday &&
    entry.holidayName === "Zweiter Weihnachtstag"
  ) {
    console.log("✅ SUCCESS: Idempotent update worked!");
  } else {
    console.error("❌ FAILURE: Entry was not updated correctly");
  }

  // Cleanup
  await holidayRepo.delete({ country: park.countryCode, date: testDate });
  // We keep the schedule entry for now as it's "real" data (or we could delete it if we want to leave it as it was)

  await app.close();
}

verifyIdempotentGapFilling().catch(console.error);
