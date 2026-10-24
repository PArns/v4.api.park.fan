import { Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import axios, { AxiosInstance } from "axios";

export interface KasiHoliday {
  date: string; // YYYY-MM-DD
  name: string; // 추석, 대체공휴일, …
}

interface KasiItem {
  locdate: number | string;
  dateName: string;
  isHoliday: string;
}

/**
 * KASI (한국천문연구원) 특일 정보, operation `getRestDeInfo` — Korea's public
 * holidays as the astronomy institute that computes the lunar ones publishes
 * them, including substitute holidays (대체공휴일) and holidays declared at
 * short notice.
 *
 * data.go.kr dataset 15012690: free, auto-approved key, "이용허락범위 제한
 * 없음" (no restriction on use), 10,000 calls/day on a development key.
 * The key goes in `KASI_SERVICE_KEY` (the data.go.kr "일반 인증키", decoded
 * form); without it the client fetches nothing.
 */
@Injectable()
export class KasiClient {
  private readonly http: AxiosInstance;
  private readonly serviceKey: string | undefined;

  constructor(config: ConfigService) {
    this.serviceKey = config.get<string>("KASI_SERVICE_KEY") || undefined;
    this.http = axios.create({
      baseURL:
        "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService",
      timeout: 20000,
    });
  }

  get enabled(): boolean {
    return !!this.serviceKey;
  }

  /** Every public holiday of one year (solMonth omitted = the whole year). */
  async getPublicHolidays(year: number): Promise<KasiHoliday[]> {
    if (!this.serviceKey) return [];
    const { data } = await this.http.get<unknown>("/getRestDeInfo", {
      params: {
        ServiceKey: this.serviceKey,
        solYear: year,
        numOfRows: 100,
        _type: "json",
      },
    });
    return parseKasiResponse(data);
  }
}

/**
 * The holidays of a `getRestDeInfo` JSON response. The JSON form of the
 * provider's XML: `items` is an empty string when there is none and `item` a
 * single object when there is one. Only `isHoliday = "Y"` days count; a
 * resultCode other than "00" throws.
 */
export function parseKasiResponse(data: unknown): KasiHoliday[] {
  const response = (data as { response?: Record<string, unknown> } | null)
    ?.response;
  const header = response?.header as { resultCode?: string } | undefined;
  if (!response || header?.resultCode !== "00") {
    throw new Error(`KASI getRestDeInfo: ${JSON.stringify(data)}`);
  }
  const items = (response.body as { items?: unknown } | undefined)?.items;
  const item =
    items && typeof items === "object"
      ? (items as { item?: KasiItem | KasiItem[] }).item
      : undefined;
  const list = item === undefined ? [] : Array.isArray(item) ? item : [item];
  return list
    .filter((i) => i.isHoliday === "Y")
    .map((i) => {
      const d = String(i.locdate);
      return {
        date: `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`,
        name: i.dateName.trim(),
      };
    });
}
