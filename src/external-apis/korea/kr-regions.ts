/**
 * South Korea's 17 first-level regions: the ISO 3166-2:KR code and the NEIS
 * education-office code (ATPT_OFCDC_SC_CODE) that runs its schools.
 *
 * Office codes were read off the NEIS `schoolInfo` endpoint on 2026-10-24
 * (each code returns its office's name); ISO codes from ISO 3166-2:KR.
 * `names` are the romanised forms a geocoder may store instead of the ISO
 * number (Google returns names such as "Gyeonggi-do"), compared after
 * {@link krRegionKey} strips case, punctuation and the administrative suffix.
 */
export interface KrRegion {
  iso: string;
  office: string;
  names: string[];
}

export const KR_REGIONS: KrRegion[] = [
  { iso: "11", office: "B10", names: ["seoul"] },
  { iso: "26", office: "C10", names: ["busan"] },
  { iso: "27", office: "D10", names: ["daegu"] },
  { iso: "28", office: "E10", names: ["incheon"] },
  { iso: "29", office: "F10", names: ["gwangju"] },
  { iso: "30", office: "G10", names: ["daejeon"] },
  { iso: "31", office: "H10", names: ["ulsan"] },
  { iso: "50", office: "I10", names: ["sejong"] },
  { iso: "41", office: "J10", names: ["gyeonggi"] },
  { iso: "42", office: "K10", names: ["gangwon"] },
  {
    iso: "43",
    office: "M10",
    names: ["chungbuk", "chungcheongbuk", "northchungcheong"],
  },
  {
    iso: "44",
    office: "N10",
    names: ["chungnam", "chungcheongnam", "southchungcheong"],
  },
  { iso: "45", office: "P10", names: ["jeonbuk", "jeollabuk", "northjeolla"] },
  {
    iso: "46",
    office: "Q10",
    names: ["jeonnam", "jeollanam", "southjeolla"],
  },
  {
    iso: "47",
    office: "R10",
    names: ["gyeongbuk", "gyeongsangbuk", "northgyeongsang"],
  },
  {
    iso: "48",
    office: "S10",
    names: ["gyeongnam", "gyeongsangnam", "southgyeongsang"],
  },
  { iso: "49", office: "T10", names: ["jeju"] },
];

const SUFFIXES = [
  "specialselfgoverningprovince",
  "specialselfgoverningcity",
  "metropolitancity",
  "specialcity",
  "teukbyeoljachido",
  "teukbyeoljachisi",
  "teukbyeolsi",
  "gwangyeoksi",
  "province",
  "state",
  "city",
  "do",
  "si",
];

/** "Gyeonggi-do" → "gyeonggi", "Seoul Special City" → "seoul". */
export function krRegionKey(name: string): string {
  let key = name.toLowerCase().replace(/[^a-z0-9]/g, "");
  for (let changed = true; changed;) {
    changed = false;
    for (const s of SUFFIXES) {
      if (key.length > s.length && key.endsWith(s)) {
        key = key.slice(0, -s.length);
        changed = true;
      }
    }
  }
  return key;
}

/**
 * The region a stored region code names — ISO number ("41", "KR-41") or
 * romanised name — or null when it names none of the seventeen.
 */
export function resolveKrRegion(code: string): KrRegion | null {
  const short = code.replace(/^KR-/i, "");
  const key = krRegionKey(short);
  return (
    KR_REGIONS.find((r) => r.iso === short || r.names.includes(key)) ?? null
  );
}
