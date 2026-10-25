import { Injectable, Logger } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import axios, { AxiosInstance } from "axios";

/** One row of NEIS `SchoolSchedule` (only the fields we read). */
export interface NeisScheduleRow {
  AA_YMD: string; // "20260717"
  EVENT_NM: string; // "여름방학"
  SBTR_DD_SC_NM: string; // "휴업일" = no lessons that day
}

interface NeisSchoolRow {
  SD_SCHUL_CODE: string;
}

/**
 * NEIS open API (교육정보 개방 포털, https://open.neis.go.kr) — the school
 * calendar every Korean school enters into NEIS.
 *
 * Needs a free key (`NEIS_API_KEY`, issued on sign-up at open.neis.go.kr).
 * Without one the API answers with a 5-row sample and ignores paging
 * (measured 2026-10-24), which is useless for a calendar, so without a key
 * the client fetches nothing.
 */
@Injectable()
export class NeisClient {
  private readonly logger = new Logger(NeisClient.name);
  private readonly http: AxiosInstance;
  private readonly apiKey: string | undefined;

  constructor(config: ConfigService) {
    this.apiKey = config.get<string>("NEIS_API_KEY") || undefined;
    this.http = axios.create({
      baseURL: "https://open.neis.go.kr/hub",
      timeout: 20000,
    });
  }

  get enabled(): boolean {
    return !!this.apiKey;
  }

  /** Up to `limit` elementary schools of one office, spread over its list. */
  async getElementarySchools(office: string, limit: number): Promise<string[]> {
    const rows = await this.fetchAll<NeisSchoolRow>("schoolInfo", {
      ATPT_OFCDC_SC_CODE: office,
      SCHUL_KND_SC_NM: "초등학교",
    });
    return sampleEvenly(
      rows.map((r) => r.SD_SCHUL_CODE?.trim()).filter((c): c is string => !!c),
      limit,
    );
  }

  /** One school's calendar between two `YYYY-MM-DD` days. */
  async getSchedule(
    office: string,
    school: string,
    from: string,
    to: string,
  ): Promise<NeisScheduleRow[]> {
    return this.fetchAll<NeisScheduleRow>("SchoolSchedule", {
      ATPT_OFCDC_SC_CODE: office,
      SD_SCHUL_CODE: school,
      AA_FROM_YMD: from.replace(/-/g, ""),
      AA_TO_YMD: to.replace(/-/g, ""),
    });
  }

  private async fetchAll<T>(
    endpoint: string,
    params: Record<string, string>,
  ): Promise<T[]> {
    if (!this.apiKey) return [];
    const pSize = 1000;
    const all: T[] = [];
    for (let pIndex = 1; pIndex <= 20; pIndex++) {
      const { data } = await this.http.get<unknown>(`/${endpoint}`, {
        params: { KEY: this.apiKey, Type: "json", pIndex, pSize, ...params },
      });
      const { rows, total } = parseNeisResponse<T>(data, endpoint);
      all.push(...rows);
      if (rows.length < pSize || all.length >= total) break;
    }
    return all;
  }
}

/**
 * The rows of a NEIS response. A query with no data comes back as
 * `{ RESULT: { CODE: "INFO-200" } }` and reads as no rows; any other
 * non-INFO-000 code is an error (bad key, quota) and throws, so a failed
 * fetch is never taken for a school without holidays.
 */
export function parseNeisResponse<T>(
  data: unknown,
  endpoint: string,
): { rows: T[]; total: number } {
  const body = data as Record<string, unknown> | null;
  const block = body?.[endpoint] as
    | [{ head: [{ list_total_count: number }, unknown] }, { row: T[] }]
    | undefined;
  if (!block) {
    const code = (body?.RESULT as { CODE?: string } | undefined)?.CODE;
    if (code === "INFO-200") return { rows: [], total: 0 };
    throw new Error(`NEIS ${endpoint}: ${JSON.stringify(body?.RESULT)}`);
  }
  return {
    rows: block[1]?.row ?? [],
    total: block[0]?.head?.[0]?.list_total_count ?? 0,
  };
}

/** A vacation day: a no-lessons day whose event is a 방학 (not the 방학식 ceremony). */
export function isVacationRow(r: NeisScheduleRow): boolean {
  return r.SBTR_DD_SC_NM === "휴업일" && /방학$/.test(r.EVENT_NM.trim());
}

/**
 * A school files one event per day, so a public holiday or a Saturday inside
 * its break is entered as 제헌절 / 토요휴업일 and not as 방학 (recorded:
 * 경기초 2026-07-17..19). A gap of at most this many days between two
 * vacation days is part of the break; terms are never that short.
 */
const MAX_VACATION_GAP_DAYS = 3;

/** One school's vacation days, `YYYY-MM-DD` → event name, gaps bridged. */
function schoolVacationDays(rows: NeisScheduleRow[]): Map<string, string> {
  const days = new Map<string, string>();
  for (const r of rows) {
    if (!isVacationRow(r)) continue;
    const d = r.AA_YMD;
    days.set(
      `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`,
      r.EVENT_NM.trim(),
    );
  }
  const sorted = [...days.keys()].sort();
  for (let i = 1; i < sorted.length; i++) {
    const gap =
      (Date.parse(sorted[i]) - Date.parse(sorted[i - 1])) / 86_400_000 - 1;
    if (gap < 1 || gap > MAX_VACATION_GAP_DAYS) continue;
    const name = days.get(sorted[i - 1])!;
    for (let k = 1; k <= gap; k++) {
      const day = new Date(Date.parse(sorted[i - 1]) + k * 86_400_000);
      days.set(day.toISOString().slice(0, 10), name);
    }
  }
  return days;
}

/**
 * Days on which more than half of the sampled schools are on vacation.
 * Each school decides its own break, so one school is not the region; a
 * school that returned no calendar at all does not vote.
 */
export function vacationDaysByMajority(
  schedules: NeisScheduleRow[][],
): Map<string, string> {
  const voting = schedules.filter((s) => s.length > 0);
  const votes = new Map<string, { n: number; name: string }>();
  for (const rows of voting) {
    for (const [day, name] of schoolVacationDays(rows)) {
      const v = votes.get(day) ?? { n: 0, name };
      v.n++;
      votes.set(day, v);
    }
  }
  const result = new Map<string, string>();
  for (const [day, v] of [...votes].sort(([a], [b]) => a.localeCompare(b))) {
    if (v.n * 2 > voting.length) result.set(day, v.name);
  }
  return result;
}

/** `limit` items spread evenly over `items`, in a stable order. */
export function sampleEvenly<T>(items: T[], limit: number): T[] {
  if (items.length <= limit) return items;
  const step = items.length / limit;
  return Array.from({ length: limit }, (_, i) => items[Math.floor(i * step)]);
}
