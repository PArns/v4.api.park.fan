import { ScheduleEntry } from "../../parks/entities/schedule-entry.entity";

/**
 * Interface for ride status data used in fallback logic
 */
export interface RideStatusData {
  status: string;
  waitTime: number | null;
  lastUpdated: Date;
}

/**
 * SINGLE SOURCE OF TRUTH für Park-Status
 *
 * Hybrid-Strategie:
 * 1. Primär: Schedule-basiert (wenn Schedule vorhanden)
 * 2. Fallback: Ride-basiert (nur für Parks OHNE Schedule)
 *
 * Diese Funktion wird von ALLEN Services verwendet, um konsistente
 * Status-Berechnungen sicherzustellen und die Inkonsistenz zwischen
 * verschiedenen API-Endpoints zu vermeiden.
 *
 * @param scheduleEntries - Schedule-Einträge des Parks
 * @param rideStatusData - Aktuelle Ride-Statusdaten (optional, für Fallback)
 * @returns true wenn Park offen, false wenn geschlossen
 *
 * @example
 * // Mit Schedule (Primär)
 * const isOpen = isParkOpen(scheduleEntries);
 *
 * @example
 * // Ohne Schedule (Fallback)
 * const isOpen = isParkOpen([], rideStatusData);
 */
export function isParkOpen(
  scheduleEntries: ScheduleEntry[],
  rideStatusData?: RideStatusData[],
): boolean {
  const now = new Date();

  // Strategie 1: Schedule-basiert (Primär)
  // Prüfe ob es ein OPERATING schedule gibt, das aktuell aktiv ist
  const operatingSchedule = scheduleEntries.find(
    (s) =>
      s.scheduleType === "OPERATING" &&
      s.openingTime &&
      s.closingTime &&
      now >= s.openingTime &&
      now < s.closingTime,
  );

  // Wenn Schedule vorhanden und Park ist offen: OPERATING
  if (operatingSchedule) {
    return true;
  }

  // Wenn Schedule vorhanden aber Park ist geschlossen: CLOSED
  // (Ignoriere Ride-Daten wenn Schedule existiert)
  const hasSchedule =
    scheduleEntries.length > 0 &&
    scheduleEntries.some((s) => s.scheduleType === "OPERATING");
  if (hasSchedule) {
    return false;
  }

  // Strategie 2: Ride-basierter Fallback (nur wenn KEIN Schedule)
  // Wird nur für Parks ohne Schedule-Integration verwendet
  if (!rideStatusData || rideStatusData.length === 0) {
    return false; // Keine Daten → Safe Default: CLOSED
  }

  // Filtere nur aktuelle Ride-Daten (letzte 30 Minuten)
  const recentRides = rideStatusData.filter((r) =>
    isDataRecent(r.lastUpdated, 30),
  );

  if (recentRides.length === 0) {
    return false; // Keine aktuellen Daten → CLOSED
  }

  // Prüfe ob mindestens eine Ride OPERATING ist UND Wartezeit > 0 hat
  const operatingRides = recentRides.filter(
    (r) => r.status === "OPERATING" && r.waitTime !== null && r.waitTime > 0,
  );

  return operatingRides.length > 0;
}

/**
 * Hilfsfunktion: Prüft ob Daten aktuell sind
 *
 * Verwendet um alte/stale Daten zu filtern und nur aktuelle
 * Informationen für Status-Berechnungen zu verwenden.
 *
 * @param lastUpdated - Zeitstempel des letzten Updates
 * @param maxAgeMinutes - Maximales Alter in Minuten (default: 30)
 * @returns true wenn Daten aktuell, false wenn zu alt
 *
 * @example
 * const isFresh = isDataRecent(new Date(), 30); // true
 * const isStale = isDataRecent(new Date('2024-01-01'), 30); // false
 */
export function isDataRecent(
  lastUpdated: Date,
  maxAgeMinutes: number = 30,
): boolean {
  const now = new Date();
  const ageInMinutes = (now.getTime() - lastUpdated.getTime()) / 1000 / 60;
  return ageInMinutes <= maxAgeMinutes;
}
