export interface OpenHolidaysName {
  language: string;
  text: string;
}

export interface OpenHolidaysSubdivision {
  code: string; // e.g. "DE-BW"
  shortName: string; // e.g. "BW"
}

export interface OpenHolidaysEntry {
  id: string;
  startDate: string; // "YYYY-MM-DD"
  endDate: string; // "YYYY-MM-DD"
  type: "Public" | "Bank" | "School" | "Observance";
  name: OpenHolidaysName[];
  regionalScope: "National" | "Regional";
  temporalScope: "FullDay" | "HalfDay";
  nationwide: boolean;
  subdivisions?: OpenHolidaysSubdivision[];
  groups?: OpenHolidaysSubdivision[]; // Some countries (e.g. BE) use groups instead
  comment?: OpenHolidaysName[];
}

export type OpenHolidaysResponse = OpenHolidaysEntry[];
