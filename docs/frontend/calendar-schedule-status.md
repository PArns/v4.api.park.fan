# Calendar: status (ParkStatus) – UNKNOWN vs CLOSED

Kurze Anleitung fürs Frontend: Wie ihr **Öffnungszeiten** im Kalender sauber darstellt und **UNKNOWN** von **CLOSED** unterscheidet.

## Feld: `status` (ParkStatus)

Jeder Kalendertag hat **ein** Feld:

- **`status`**: `ParkStatus` = `"OPERATING"` | `"CLOSED"` | `"UNKNOWN"`

Daran erkennt ihr sowohl „Park offen/zu“ als auch „haben wir überhaupt Schedule-Daten?“.

---

## Bedeutungen

| status      | Bedeutung | Anzeige-Empfehlung |
|------------|-----------|--------------------|
| **OPERATING** | Park hat Öffnungszeiten (von der Quelle). | Öffnungs- und Schließzeiten anzeigen (z.B. aus `hours.openingTime` / `hours.closingTime`). |
| **CLOSED**    | Park ist an diesem Tag **bestätigt geschlossen** (Quelle liefert „Closed“). | Z.B. „Geschlossen“ oder „Closed“ – kein Zeitbereich. |
| **UNKNOWN**   | **Noch keine Öffnungszeiten von der Quelle** (Monat noch nicht veröffentlicht oder Placeholder). | Z.B. „Öffnungszeiten noch nicht verfügbar“ oder „Noch nicht veröffentlicht“ – **nicht** „Geschlossen“. |

---

## Wichtig

- **UNKNOWN ≠ geschlossen.** UNKNOWN heißt: Wir haben für diesen Tag noch keine echten Schedule-Daten (z.B. Mai 2026, bis der Park den Monat veröffentlicht).
- **CLOSED** nur nutzen, wenn der Park für den Tag explizit als geschlossen gemeldet ist.
- Wenn `status === "UNKNOWN"`: Keine Öffnungszeiten anzeigen und klar kommunizieren, dass die Infos noch fehlen (nicht dass der Park zu ist).

---

## Beispiel (TypeScript)

```ts
function getScheduleLabel(day: CalendarDay): string {
  switch (day.status) {
    case "OPERATING":
      return day.hours
        ? `${formatTime(day.hours.openingTime)} – ${formatTime(day.hours.closingTime)}`
        : "Geöffnet";
    case "CLOSED":
      return "Geschlossen";
    case "UNKNOWN":
    default:
      return "Öffnungszeiten noch nicht verfügbar";
  }
}
```

---

## API-Referenz

- **Endpoint:** `GET /v1/parks/:continent/:country/:city/:parkSlug/calendar?from=&to=`
- **Response:** `days[]` mit je `date`, **`status`** (ParkStatus: OPERATING | CLOSED | UNKNOWN), `hours?`, …
- Backend-Details: [Schedule Sync & Calendar](../architecture/schedule-sync-and-calendar.md)
