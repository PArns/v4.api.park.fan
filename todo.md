# TODO

## Retire the attractions that no longer exist (2026-08-15)

The mechanism exists (`retired_at`, admin endpoints, job exclusions). What is
left is establishing, per attraction, whether it is actually gone.

- [ ] **73 individual retirement candidates** — attractions that had real wait
      times, stopped reporting more than 30 days ago on a date **not shared**
      with others in their park, and are still receiving reconciliation rows.

      Regenerate the list with this — it is the whole definition, and the
      `s.n <= 2` clause is the load-bearing part:

      ```sql
      WITH act AS (
        SELECT "attractionId" AS aid,
               max(timestamp) FILTER (WHERE status='OPERATING') AS last_op,
               max(timestamp) AS last_row, max("waitTime") AS max_wait
          FROM queue_data WHERE timestamp > now() - interval '400 days' GROUP BY 1
      ), cand AS (
        SELECT a.id, a.name, a."parkId", p.name AS park,
               act.last_op::date AS d, act.max_wait
          FROM act JOIN attractions a ON a.id = act.aid
          JOIN parks p ON p.id = a."parkId"
         WHERE act.last_op < now() - interval '30 days'
           AND act.last_row > now() - interval '2 days'
           AND act.max_wait > 0            -- it really had a queue
           AND a.retired_at IS NULL
      ), same_day AS (SELECT "parkId", d, count(*) AS n FROM cand GROUP BY 1,2)
      SELECT c.park, c.name, c.d AS went_silent, c.id
        FROM cand c JOIN same_day s ON s."parkId" = c."parkId" AND s.d = c.d
       WHERE s.n <= 2                      -- individual, not a block
       ORDER BY c.park, c.name;
      ```

      **Research shape that works** (six subagents grouped by operator, since
      causes cluster there): each attraction gets one of PERMANENTLY_CLOSED /
      REFURBISHMENT / SEASONAL / STILL_OPERATING / UNKNOWN, and
      PERMANENTLY_CLOSED requires a **date and a source** or it does not count.
      Tell the agents explicitly that STILL_OPERATING is a normal answer —
      *Pooh's Hunny Hunt*, *Dumbo*, *Enchanted Storybook Castle*, *Marvel Cave*
      and *Shock Wave* are all in the list and are all landmarks of their parks.
      If those come back "retired", the research is wrong, not the parks.

      Traps found while preparing the batch: "Sea World" here is the **Gold
      Coast, Australia** park, not Orlando; `Skyride (Egypt Station)` and
      `Walibi Express Station 2` are **stations** of one ride; Universal
      Beijing's "The Wizarding World of Harry Potter" is a whole **land**; and
      `Coastersaurus - Currently Closed for Maintenance` carries a hint in its
      name that still needs verifying.
      Spread over ~20 parks, at most five each. The scattered dates are what
      separates them from the 67 whose whole block fell silent on one day —
      those are seasonal closures (Wet'n'Wild's 13+9 on 2026-06-29 is the
      Southern-Hemisphere winter, Bellewaerde's 5 on 2026-02-11 the Belgian
      one) and must NOT be retired.
      Each needs a **date and a source**, not just a verdict: `retired_at` wants
      the actual closure date where one is stated, and anything that turns out
      to still exist stays untouched.
- [x] Four already established and retired as the proof batch: Animal Kingdom's
      *Affection Section* (2026-02-22), *Dino-Sue* (2026-02-15) and *The
      Animation Experience* (2026-02-23), Ocean Park's *North Pole Encounter*
      (2026-03-03).
- [ ] **`Expedition Everest - Legend of the Forbidden Mountain Single Rider` is
      not a retirement candidate — it is a data-model error.** It is a queue
      variant of a live roller coaster, not an attraction, so it should be
      merged into its parent or excluded at mapping time. Worth checking whether
      other parks have the same shape.


## Feed-dropped attractions still get marked seasonal (2026-08-15)

A ride that no upstream source reports any more now reads `UNKNOWN` instead of
CLOSED on all three surfaces. What is **not** fixed is what the detector does
with the same rows.

- [ ] `detect-seasonal` reads `current_status = 'CLOSED'` and the OPERATING
      history, both of which still describe the frozen feed. So the ~140
      feed-dropped attractions keep being marked seasonal, and the months it
      derives are the observation-window artefact **at scale**: all 44
      Europa-Park rides carry the identical list `[1,2,3,4,5,6,12]`, which is
      simply "every month before the feed went silent". In August that reads as
      44 attractions out of season at a park in peak season.
      **(b) is built** — months need `MIN_OBSERVED_DAYS` (330) of watching, on
      attractions and shows alike. **(a) still to do**: the stored artefact
      months must be cleared once the guard is deployed, not before — Step 3 has
      no `is_seasonal = false` filter, so every still-closed candidate is
      re-evaluated on the next 2:30am run and a reset done first would simply be
      rewritten.
- [ ] Consider teaching the detector the same distinction the read path just
      learned: a row written by `system-reconciliation` is not evidence of a
      season. That is probably the cleaner fix than (b) alone.

## Upstream: ThemeParks.wiki dropped whole clusters of attractions

- [ ] Ten parks lost a block of attractions from the wiki's **live** feed on a
      single day each — Europa-Park and Rulantica on 2026-06-07, Universal
      Studios Singapore on 2026-04-25, both Wet'n'Wild records on 2026-06-29,
      Busch Gardens Tampa 2026-06-13, Ocean Park 2026-06-30. The entities may
      still exist; only the live rows stopped. Worth establishing whether they
      were recategorised upstream (and can be re-matched), or genuinely dropped.
      Every affected ride lacks a `queue_times_entity_id` — the ones with a
      Queue-Times mapping kept working, which is both the tell and a hint at the
      mitigation: broaden Queue-Times matching for these parks.
- [ ] **"Wet'n'Wild" and "Wet 'n' Wild Gold Coast"** both show 13 silenced
      attractions with the same date. That looks like a duplicate park pair for
      the existing duplicate-records work.


## Free-flow attractions & seasonality (2026-08-15)

**Context:** `open_with_park` was only ever curated for Phantasialand. A sweep
found 76 name-matched candidates; 47 already report OPERATING (the feed handles
them), 4 sit in Hansa-Park where the flag deliberately cannot fire, and 25 were
researched one by one against the operators' own pages. 14 were flagged.

**Deliberately not flagged — genuinely seasonal, at parks open year-round:**
- [x] **The season gate exists now** — `isFreeFlowOpen` takes `seasonMonths` +
      the park timezone, and the detector no longer owns the months on a
      free-flow row. What is still missing is the months themselves.
- [ ] Curate `season_months` for Europa-Park — *Lítill Island* (summer) and
      *Water Playground* (summer + Halloween); Everland — *Snow playground*
      (winter); Bellewaerde — *Snowmen Playground* (Christmas event only), then
      set `open_with_park`. **Needs season-date research first**: "summer" is
      not a month list, Europa-Park's summer season opens in late March (is 3 in
      or out?), and Everland's snow-park closing date was never confirmed by a
      fetched source. These are curation calls against the operator's calendar,
      not util logic.
- [ ] **Season unknown, water-based, park open year-round** — Peppa Pig
      *Muddy Puddles Splash Pad*, Walibi Rhône-Alpes *Exotic Island 3-6* and
      *7-12*. Confirmed free-flow, but no source states an operating window, and
      a water play area plausibly closes in cold months. Held rather than
      guessed. (The splash pads at *seasonal parks* — Water Country USA,
      Hurricane Harbor Arlington — were flagged: the park-status gate does the
      seasonal work there.)

**`season_months` can encode the observation window, not a season:**
- [ ] Phantasialand's Avoras, Berliner Eislaufen and Ice skate hire all have
      their first queue_data row on **2025-12-24** and all derived
      `season_months = [1, 12]`. For the two ice-rink attractions that is
      correct; for Avoras — advertised by the park as open *"ganzjährig"* — it
      was pure artefact, and it read as out of season all summer.
      **The data alone cannot separate the two cases.** A guard ("only derive
      months once we have observed ≥ ~330 days") would drop the artefact but
      also drop the two correct labels, so it was not shipped. Revisit once
      history spans a full year for these attractions, or gate on observed span
      per attraction rather than globally.

**Attractions marked seasonal with no months at all:**
- [ ] Movie Park Germany's 9 Halloween Horror Fest mazes are correctly seasonal
      but have `season_months = NULL`: their first feed row is 2026-04-17, i.e.
      *after* the last Halloween, so there is no observed operating month to
      derive from. The detector honestly writes NULL. Consequence:
      `isCurrentlyInSeason` is `null`, the frontend cannot distinguish "closed
      today" from "not in season", and they drag the park's operating count to
      22/39 in August. Do **not** hand-fill October — whether the feed even
      reports the mazes OPERATING during HHF is unproven. After HHF 2026 the
      detector will have real evidence.
- [ ] Related presentation gap: the API already knows `isSeasonal: true`. What a
      client cannot do is tell "out of season" from "no data", because
      `isCurrentlyInSeason` is `null` in both cases.

**Unresolved identities — researched, not concluded:**
- [ ] Heide Park **"PLAYGROUND"** — no source ties this record to a specific
      physical area; the park's own attraction overview names no standalone
      playground. Possibly a feed artefact.
- [ ] Plopsaland De Panne **"The Pirates' Playground"** — absent from the
      official sitemap in all four languages, old URLs 404. Evidence points to
      removal; needs a decision (delete vs keep).
- [ ] LEGOLAND Korea **"Cole's Rock Climbing"** — the official page's entire
      body text does not say whether it is a walk-up wall or a staffed harnessed
      one. Sister parks point free-flow, but that is cross-park inference.
- [ ] Toverland **"Kletterparcours"** is **gone** — a harnessed high-ropes course
      (140 cm minimum, 120 kg max), permanently closed after 2 Nov 2025, and its
      Toverland page 404s. It is still in our DB. Note it was never free-flow:
      flagging it by name pattern would have advertised a demolished attraction
      as always open.
- [ ] Movie Park **"Teenage Mutant Ninja Turtles: License to Drive"** has never
      once reported OPERATING since its first row on 2025-12-24. Unclear whether
      it is unopened, removed, or mis-fed.

**The wider backlog (not yet touched):**
- [ ] The name-based net has a known hole: *Mopti's Monkey Depot* contains no
      playground vocabulary and would never have matched. The behavioural net —
      attractions that never report OPERATING in a park whose feed demonstrably
      works — returns **453** rows. A sample shows it is dominated by Halloween
      event attractions, winter operations (*Curlingbaan*, *Schaatsbaan*,
      *Tubingbaan*), off-season water areas and genuinely defunct rides, so it
      needs a cheaper triage than per-attraction research before it is useful.


## Ride profiles: the safety nets that went with the seed (2026-08-15)

**Context:** [#163](https://github.com/PArns/v4.api.park.fan/pull/163) removed
`RIDE_PROFILE_SEED`, its spec, the mirrored term-id allowlist and the
`apply-ride-profiles` job. `attraction_ride_profiles` is now the source of truth
and is edited directly. Two things the job did for free are now nobody's job,
and both fail **silently** — that is what makes them worth tracking.

- [x] **Nothing validates term ids** — built: `GET /v1/admin/ride-profile-term-audit`
      diffs every stored id against `park.fan/api/glossary/term-ids` (which
      publishes the ids that actually resolve to a page) and names both the
      broken ids and the rides they shorten.
- [x] **Nothing evicts caches after a curation write** — built:
      `POST /v1/admin/publish-ride-profiles` evicts every park whose profiles
      carry a recent `seeded_at`, revalidates, and queues the post-CDN sweep.
- [x] **Der Audit laeuft jetzt von selbst** — taeglich 06:30 auf der
      manual-metadata-Queue. Er scheitert bewusst NICHT bei einer kaputten ID:
      die IDs sind in Ordnung, bis das Frontend eine Umbenennung deployt, und
      ein jede Nacht roter Job erzieht dazu, ihn zu ignorieren. Eine Warnung,
      die die IDs und die betroffenen Rides nennt, ist das Signal; der Endpoint
      bleibt fuer die Details. Ein unerreichbares Frontend wird als solches
      geloggt und nicht als Kurations-Problem.
- [ ] **`publish-ride-profiles` bleibt ein manueller Aufruf** — und das ist
      richtig so: er gehoert ans Ende einer Kuratier-Sitzung, und die ist
      ohnehin ein menschlicher Moment.

**Curation left deliberately open:**
- [ ] **Der Audit-Cron ist registriert, aber noch nie gelaufen.** `delayed: 1` auf
      der Queue belegt die Registrierung, der Fehlerpfad ist getestet — dass der
      Handler in Produktion durchlaeuft, zeigt erst der erste Lauf um 06:30.
      Danach einmal im API-Log nach `Ride-profile term audit clean` schauen.
- [ ] **Die Bull-Queue heisst weiter `manual-metadata`**, obwohl der Seed dieses
      Namens geloescht ist. Bewusst so gelassen: Bull schluesselt Repeatable-Jobs
      in Redis am Queue-Namen, ein Umbenennen wuerde
      `ride-profile-term-audit-cron` unter dem alten Namen stranden — registriert
      und von niemandem konsumiert. Sauber ginge es nur per Expand/Contract: neue
      Queue registrieren, alten Repeatable entfernen, dann die alte fallenlassen.
      Der Grund steht am Dekorator, die Klasse heisst inzwischen
      `CuratedDataProcessor`.
- [x] **Universal Studios Florida ist zusammengefuehrt.** Der Queue-Times-Datensatz
      (`universal-studios-at-universal-orlando`) ist im Wiki-Datensatz aufgegangen:
      32 Attraktionen und 313 Schedule-Eintraege migriert, der Park steht jetzt bei
      36 Attraktionen ohne Duplikate, alle 11 Ride-Profile erhalten. RCDB 3866 ist
      damit nicht mehr doppelt vergeben.
- [x] **`wait_time_predictions`: die 51.000 doppelten Primaerschluessel sind weg,
      und Hurricane Harbor ist zusammengefuehrt.** Timescale erzwingt den PK auf
      komprimierten Chunks nicht, also kollidierten dort Zeilen beim
      Dekomprimieren — womit JEDE Loeschung scheiterte, die einen betroffenen
      Chunk beruehrte. Der PK haengt an der Hypertable, nicht am Chunk, ein
      chunkweises Reparieren war also nicht moeglich. Entschieden wurde, die
      sieben betroffenen Chunks (2026-05-21 bis 07-09) zu droppen: 13,5 Mio.
      Zeilen Vorhersage-Historie, ~37 % der Tabelle. `drop_chunks` braucht keine
      Dekomprimierung und lief in Sekunden. Die Tabelle steht jetzt bei 23,0 Mio.
      Zeilen, aeltester Chunk 2026-07-09, alle verbliebenen Chunks duplikatfrei.
      Danach ging der Park-Merge ohne Weiteres durch (13 Attraktionen, 173
      Schedule-Eintraege).
- [ ] **Die Ursache der Duplikate ist NICHT gefunden.** Das Fenster war
      abgegrenzt (05-21 bis 07-09) und alles danach ist sauber, das Problem hat
      also von selbst aufgehoert — aber niemand weiss, warum es anfing oder
      warum es endete. Wenn es wiederkommt, faellt es erst auf, wenn wieder eine
      Loeschung scheitert. Ein billiger Waechter waere eine periodische Zaehlung
      doppelter Schluessel pro Chunk; die Query steht im Verlauf dieser Session.
- [ ] **prediction_accuracy fuer 2026-05-21 bis 07-09 hat keine Grundlage mehr.**
      Die Vorhersagen dieses Zeitraums sind geloescht; ausgewertete Kennzahlen,
      die noch darauf verweisen, stehen ohne Beleg da. Pruefen, ob dort etwas
      nachgezogen oder als luecke markiert werden muss.
- [ ] **Traumatica und Europa-Park bleiben getrennt** (bewusste Entscheidung:
      Traumatica ist ein eigenstaendiges Event mit eigenem Ticket, kein
      Duplikat). Folge davon: `matterhorn-blitz` und `pegasus` existieren als
      Attraktion unter beiden Parks und teilen sich je eine RCDB-ID (971, 3403).
      Das ist die einzige zulaessige Ausnahme von der Regel "eine ID, eine
      Attraktion" — dieselbe physische Bahn wird in zwei Veranstaltungen
      gelistet. Beim Duplikat-Check also nicht als Fehler werten.
- [ ] **`curated_may_get_wet` beweist sich erst beim naechsten Detail-Sync.**
      Aktuell stimmt sie ueberall mit `may_get_wet` ueberein, weil der geloeschte
      Seed seine Werte dort schon hineingeschrieben hatte. Die Divergenz — und
      damit der Beweis, dass die Korrekturschicht greift — entsteht erst, wenn
      der Sync Gentings Shot Tower wieder auf `true` setzt. Danach einmal
      pruefen, dass die Ride-Seite weiterhin `false` ausliefert.
- [x] **Runde 5 erledigt — der Cluster-Sweep ueber benannte Inversionen ist
      sauber.** Alle Listen, die eine benannte Inversion enthalten, werden jetzt
      nur noch von Rides geteilt, die auch dieselbe Inversionszahl melden. Was
      die Query noch ausgibt, sind die generischen duennen Listen
      (`lifthill → first-drop → helix → brake-run` und Verwandte, 54 Rides),
      Minenzuege und Holzachterbahnen — die fuehren gar keine benannte
      Inversion, dort kann die Liste nur unvollstaendig sein, nicht falsch.
- [x] **Runde 6 erledigt — die Cluster-Query ist ausgereizt.** Es gibt keine
      geteilte Elementliste mit einer benannten Inversion mehr, deren Rides
      unterschiedliche Inversionszahlen melden. Die verbliebenen Gruppen sind
      echte Klonfamilien (Boomerang 6, SLC 6, Double Loop Corkscrew 5,
      Sky Rocket II 4, Batman 2 …) und ihre Uebereinstimmung ist jetzt ein
      Konsistenz-Signal statt eines Alarms. Als naechstes Instrument braeuchte
      es etwas anderes als diese Query — z. B. Elementlisten, die zur
      veroeffentlichten Inversionszahl nicht aufgehen, ueber den ganzen Bestand
      statt nur ueber Cluster.
- [x] **Die doppelte RCDB-ID 3 ist bereinigt** — nur Gurnee traegt sie noch,
      Santa Clara steht auf NULL. (Nebenbei aufgefallen: La Ronde hat einen
      dritten Demon, ganz ohne ID.)
- [x] **Runde 3 erledigt** — Voltron, Banshee, blue fire, Fury (Bobbejaanland)
      und die zehn Marquee-Namen mit 0 Inversionen geprueft. Vier Fehler
      gefunden und korrigiert; Millennium Force, Taron, Shambhala, Silver Star,
      Kondaa und drei der vier Goliaths waren exakt richtig.
- [ ] **Untamed ist der letzte ungeklaerte RMC.** Steel Vengeance und Iron Gwazi
      trugen beide einen erfundenen `twisted-horseshoe-roll`; Untamed traegt
      ihn auch, und die Arithmetik geht mit ihm auf (5). Aber keine Quelle
      benennt die Figuren ausser der ersten — Wikipedia und RMC nennen dort
      einen „270° Double Inverting Corner Stall", fuer den es keinen Begriff
      gibt. Ohne Beleg nichts entfernt: Verdacht ist kein Nachweis. Braucht
      eine Quelle, die alle fuenf benennt (RMC-Projektseite oder POV-Zaehlung).
- [ ] **Helix' hintere Haelfte ist ungeklaert.** Wikipedia nennt einen Pretzel
      Knot, eine zweite Quelle stattdessen Inside Top Hat + Inline Twist. Beide
      koennen nicht stimmen; die Liste fuehrt deshalb nur die uebereinstimmenden
      Figuren und kommt auf 5 statt der veroeffentlichten 7.
- [ ] **`the-flying-dinosaur-2` ist wieder ein `-2`-Duplikat** — diesmal ein
      B&M Flying Coaster mit 90 min p90, also ein echter Headliner auf einem
      Duplikat-Datensatz.
- [ ] **`sky-loop` fehlt im Glossar.** Abismo (Madrid, RCDB 3185) ist der einzige
      je gebaute Maurer SkyLoop XT 450, und seine Signaturfigur — die
      herzförmige Schleife, in der der Zug sekundenlang kopfüber hängt — hat
      keinen Begriff. Die Elementliste führt jetzt Korkenzieher + Immelmann
      (so benennt die spanische Wikipedia die beiden Überschläge, und nur so
      geht die veröffentlichte 2 auf), aber die Figur selbst fehlt. Gleiche
      Klasse wie `cobra-loop` vor der Ergänzung.
- [ ] **Red Force: Modell umstritten.** Die DB sagt `Accelerator Coaster`,
      Wikipedia `LSM Launch Coaster`. Je eine Quelle, also nichts geändert —
      die Regel „bei Widerspruch nichts schreiben" gilt auch fürs Modell.
      Klärt eine dritte Quelle (Intamin-Projektseite, Ferrari Land), ob der
      Abschuss hydraulisch oder per LSM erfolgt.
- [ ] **Furius Baco: `overbank` ist unbelegt.** Die gefundene Quelle nennt vier
      Helices und keinen Overbank. Eine Quelle reicht mir nicht zum Ändern,
      aber das Element steht auf der Kippe.
- [ ] **26 Attraktions-Duplikate warten, vom Tooling bereits als sicher
      eingestuft** — `POST /v1/admin/merge-duplicate-attractions` findet sie im
      Trockenlauf: **Energylandia 23**, Heide Park 2, Six Flags New England 1.
      Es ist derselbe Cross-Source-Fall wie bei Magic Kingdom: eine Wiki-Zeile
      und eine Queue-Times-Zeile derselben Bahn. Sechs weitere stellt das
      Tooling bewusst zur Handpruefung zurueck, weil die Namen auseinandergehen
      (`Riptide Racer` vs `Riptide`, `Main Train` vs `Choco Chip Creek`) — die
      brauchen ein Urteil, keinen Batch-Lauf.

- [ ] **Hyperia** (Thorpe Park, RCDB 20652) — sources state 2, 3 *and* 4
      inversions; Wikipedia contradicts itself within one article. The element
      list also calls its Immelmann non-inverting. Needs the park's own spec
      sheet or a POV count before the entry moves.
- [ ] **Zadra** (Energylandia, RCDB 16184) — publishes 3 inversions but the
      curated element list names 4 inverting figures. No source states the
      element order, so neither side can be corrected without inventing data.
- [ ] **Coasters with no RCDB id are unexamined.** Every ride that *has* an id
      now has a profile (0 remaining). The coverage question is the other
      direction: 6,508 attractions have no profile, and while most are flat rides
      and shows, an unknown number are coasters that never got an id from the
      Wikidata match. Find them by `types`/name heuristics before deciding
      whether it is worth a pass.

**park.fan follow-ups from the same work:**

- [ ] **`scripts/export-glossary-term-ids.mjs` has no consumer.** Its whole
      purpose was writing the API allowlist that #163 deleted. It still runs and
      still prints a useful id list — decide whether to delete it or document it
      as a diffing tool, but do not leave it looking like a required step.
- [ ] **18 named figures have no 3-D player** (42 of 77 `coaster-elements` have
      one). Concepts and brakes legitimately have none; these are real shapes:
      `bowtie, butterfly, cobra-loop, cutback, dive-drop, flying-snake-dive,
      high-five, inline-twist, jojo-roll, norwegian-loop, predrop, pretzel-knot,
      splashdown, stall, stengel-dive, swing-launch, treble-clef, turntable`.
      `inline-twist` and `stall` are the highest-value — they appear most often
      in curated layouts. Build against `lib/three/coaster/elements.ts` and
      verify with `scripts/render-coaster-elements.mjs`, per the three.js
      convention.

**Observed while auditing, not acted on:**

- [x] **Animal Kingdom haelt keine Magic-Kingdom-Rides mehr** — 42 umgehaengt,
      35 Duplikate gemerged, der Park steht bei 24 statt 66 Attraktionen.


## Schedule times: 12-hour-clock rows need a curated override (2026-07-27)

**Context:** `normalizeClosingTime` (PR "repair closing times whose date contradicts
the opening") re-anchors a misdated closing time to the opening's park-local date.
That fixes 178 rows, but 7 of them stay wrong in a way no generic rule can repair —
the source reports `opens 15:00 / closes 12:00`, which is almost certainly a
12-hour-clock error where 12:00 means **midnight**:

| Park | Days | Reported | After re-anchoring |
| --- | --- | --- | --- |
| Six Flags Qiddiya City | 2026-04-17/24, 05-01/08/15 | 15:00 → 12:00 | 21 h day |
| Kings Dominion | 2026-09-18, 09-25 (Haunt evenings) | 18:00 → 12:00 | 18 h day |

Those windows are **right during the actual event hours and wrong overnight** — a
strict improvement over the previous state (closing before opening ⇒ the park read
CLOSED all evening), but still not the truth. Kings Dominion's dates are upcoming
Halloween nights, so this has a real audience.

**Why a generic fix is wrong here:** "reinterpret 12:00 as 00:00" would silently
rewrite every legitimate noon closing (water parks and Christmas markets do close at
noon). The distinguishing signal is `closing < opening`, which the normalizer has
already consumed. Guessing further means inventing data.

**How:**
- Curate the affected park/date pairs the way ride heights are curated
  (`src/attractions/data/manual-attraction-metadata.ts` is the pattern): an explicit
  park + date + corrected closing time, applied in `saveScheduleData` after
  `normalizeClosingTime`.
- Or narrower: a per-park "source uses a 12-hour clock" flag, applied only when the
  raw closing is `< opening` **and** lands at exactly 12:00.
- Detect new cases with the audit query in `docs/troubleshooting/db-health-runbook.md`
  style:

  ```sql
  SELECT p.name, s.date,
         (s."openingTime" AT TIME ZONE p.timezone)::time AS opens,
         (s."closingTime" AT TIME ZONE p.timezone)::time AS closes
  FROM schedule_entries s JOIN parks p ON p.id = s."parkId"
  WHERE s."closingTime" - s."openingTime" > interval '16 hours';
  ```

- Effort: ~half a day including the curated list.

**Also still open from the same sweep:**

- [ ] One `schedule_entries` row with an equal opening and closing (Universal Volcano
      Bay, dated `1970-01-01`). Left untouched by design — rolling it forward would
      invent a 24 h operating day. Decide whether to delete the sentinel row.
- [ ] ~127 duplicate `(parkId, date, scheduleType, attractionId)` groups, unrelated to
      the date repair (which deliberately scoped its dedupe to the days it touched).
      There is **no unique index** on that tuple; adding one would need the duplicates
      resolved first.
- [ ] After the first schedule sync on the new code (daily 15:00 UTC), confirm no
      **new** impossible windows appear — the repair proved the old rows are fixed,
      not that the write path holds:

  ```sql
  SELECT count(*) FROM schedule_entries
  WHERE "openingTime" IS NOT NULL AND "closingTime" IS NOT NULL
    AND ("closingTime" < "openingTime"
         OR "closingTime" > "openingTime" + interval '24 hours');
  ```

## PCN/Shape — deferred model work (gated on a clean board, PR #79 review)

Deliberately NOT shipped in PR #79 — code-side that PR is complete; these are
model/quality experiments that must wait until the shadow boards have matured
1–2 weeks on the fixed scorer (above), because each is judged on that board and
"nothing flips production without a busy/headliner win on clean evidence".
Rough order by ROI; each its own PR. Full rationale in
[docs/ml/pcn-intraday-review.md](docs/ml/pcn-intraday-review.md) (§5–6, §8).

- [x] **Receptive-field bake-off** — done and flipped: `PCN_GWN_LAYERS` defaults
      to **8** in `pcn-service/config.py`, so the served GraphWaveNet sees the
      whole 192-slot context instead of ~1 h. Won on the busy segment and the
      champion swap now serves PCN intraday. *(Note: review §7 table row 6 still
      reads "Default bleibt 2" — that line is stale, the code is the truth.)*
- [ ] **Lead-curve scoring from the stored fan** (review §3 / §7.7) — *partly done:*
      the `pcn_blend` shadow model in `pcn-service/score.py` keeps the persistence
      blend under live A/B (`pcn_forecasts` stays raw on purpose). What remains: the scorer
      currently joins only the freshest origin (≈15-min leads), so the quality of
      the actually-served longer leads (3–12h, rest-of-day) is unmeasured. Join the
      stored 48-slot fan at lead 1h/3h/6h vs actual + persistence baseline. A
      CatBoost head-to-head at long lead additionally needs the design-doc §12.3
      CatBoost co-snapshot (not implemented) — optional.
- [ ] **Feature channels** (review §5b) — DOW shipped; `is_holiday` was measured
      and **rejected** (clean A/B came out flat, so it is not in the channel set —
      don't re-add it without new evidence). Still open, each through the bake-off
      on busy-MAE/bias: `is_school_break`, schedule-relative time (minutes since
      open / to close), weather (the worst-MAE list — Cheetah Hunt / Wolfpack Raft
      Slide / Manta — is a water/outdoor cluster).
- [ ] **KPIs must follow the served model** (review §6a): "Live MAE 8.70" + the drift
      warning (24.58/20) still measure CatBoost-stored, but PCN serves intraday. Point
      the `prediction_accuracy` pipeline at the serving view (incl. PCN override) or
      add a second "served" panel; split the drift monitor by horizon (CatBoost drift
      is now a far-daily concern where it stays the sole level provider).
- [ ] **Shape offline-vs-live reconcile** (review §6b): offline claimed −7.4% busy,
      live board shows Shape losing everywhere (busy −3.9, bias −20). After the scorer
      fix, re-read the board; then check whether the *level* Shape renders onto
      under-shoots busy days (bias −20 smells like a level, not a curve, error). No
      producer swap to `learned.py` before this is understood.
- [ ] **Cheap experiments from the design doc** (§11.5): Chronos-Bolt zero-shot as a
      foundation baseline (no training, instant comparison number) and TouringPlans
      pretraining seed against the ~6–7-month history gap. Weekend-sized, clear signal.
- [ ] **Per-park training hygiene** (review §5c): no validation split / early-stop
      (fixed 500 steps for a 10-ride park and a 100-ride park alike); one robust
      `_scale` per park (headliner + walk-on share a scale). Hold out the last day,
      early-stop, log per-park final loss so degenerate park models are visible before
      they serve.

## Days past midnight: verify after deploy, then let the frontend drop its hedge (2026-09-05)

The `/plan/day` wrap fix is in this repo (`unfoldedCloseHour`, changelog
"a park that closes after midnight had no plan at all"). Two things can only be
checked once it is live:

- [ ] **Confirm against the parks that wrap.** Six Flags Qiddiya City is the
      sharpest case — its rollup for the night of 2026-09-02 holds 16:00 through
      midnight with 13 rides still measured at hour 24, and the endpoint answered
      `rides: []` for it:

      ```
      curl -s 'https://api.park.fan/v1/parks/asia/saudi-arabia/al-moqbel-palaces/six-flags-qiddiya-city/plan/day?date=2026-09-02' \
        | jq '{ctx: .context | {openHour, closeHour}, rides: (.rides|length), lastHour: (.rides[0].hours|last)}'
      ```

      Expect `openHour: 16`, `closeHour: 0` and hours running past 23. Check a
      forecast day too (Parque Warner Madrid or Cedar Point on 31 October), and
      La Ronde, whose rollup is thin enough that it may still answer with few
      rides for reasons that have nothing to do with midnight.

- [ ] **Tell the frontend the contract is settled.** `estimateFor`
      (`lib/planner/estimate.ts` in the park.fan repo) looks a ride's curve up
      twice — once at the axis hour, once at the wall-clock hour — because
      nothing established which one `hours[].hour` carried on a wrap day. It
      carries the **unfolded** one (24 = midnight), so the second lookup can go,
      and `docs/…/parks-past-midnight.md` there wants the note it asks for.

## ML hourly_agg cache — post-deploy verification & follow-up

**Context:** `fetch_recent_wait_times` (`ml-service/predict.py`, the `WITH hourly_agg ...` query)
is the #1 steady-state DB load. Root cause: `base_time=datetime.now()` (microseconds) flowed
into the cache key → ~0% hit rate. Fix shipped: bucket `end_time` to the cache-TTL window +
raise TTL 2→15 min + evict expired entries on write.

**Baseline (pre-fix, measured 2026-06-03, ~133 min window):**

| fingerprint | calls/min | ms/min (DB time) | mean_ms |
|-------------|-----------|------------------|---------|
| 48c290bd    | 26.1      | 7196             | 275.2   |
| 0f0d8d65    | 3.6       | 1355             | 373.4   |
| **combined**| **~29.7** | **~8551** (≈14% of one core) | — |

### Verification protocol (run AFTER deploy)

> **Stale as written (noted 2026-08-15):** the fix itself is long deployed —
> `predict.py` buckets the cache key and runs a 900 s TTL with eviction. Nobody
> ran the before/after comparison, and the 2026-06-03 baseline is now two months
> and several query changes old. Either re-baseline and measure, or close this
> out on the current slow-query log instead of resurrecting the old numbers.

- [ ] Confirm new ml-service container is live (Coolify redeploy done — module-global cache
      only resets on a fresh process, so the fix is NOT active until redeploy).
- [ ] `SELECT pg_stat_statements_reset();` on celestrial Postgres.
- [ ] Let it run ~30–60 min (cover ≥2 of the 15-min prediction crons + on-demand traffic).
- [ ] Re-run the baseline query (calls/min + ms/min for `query LIKE 'WITH hourly_agg%'`) and
      compare against the table above. Expect the on-demand/repeat-park calls to collapse.

Baseline SQL: `pg_stat_statements` joined with `pg_stat_statements_info`, normalize
`calls` and `total_exec_time` by `EXTRACT(EPOCH FROM now()-stats_reset)/60`.

### Decision gate — per-attraction caching (only if still hot)
If `hourly_agg` is still a top load after the fix, the remaining cost is the **single-attraction
path** `getAttractionPredictions` (`src/ml/ml.service.ts`, `attractionIds: [attractionId]`,
attraction-detail pages) which uses a per-single-attraction key and does NOT reuse the
park-level fetch (`predictForPark` → `activeAttractionIds`).
- [ ] If hot: cache the query result **split by attractionId** (safe — window functions are
      `PARTITION BY "attractionId"`, so each attraction's rolling values are independent).
      On read, assemble from per-attraction cache; query only the missing IDs. Then single-
      attraction and park-level paths share entries.
- [ ] If not hot: close this out, no further work.

---

## Remaining refactorings from the 2026-06 codebase review

The low-risk findings were fixed in PR #68 (N+1 batching, parallel weather sync,
shared live-data/pagination/cache-key helpers, dead TTL constants, luxon removal,
ML consistency cleanup, `safeJsonParse`, broken merge/repair cache invalidation).
What follows is the deliberately deferred rest — each item carries behavioral risk
and should be its own PR.

### 1. Tests for ShowsService / RestaurantsService (do this FIRST)

**Why first:** both services have **zero** spec files, and they are the precondition
for item 2 — refactoring untested sync code is how regressions ship.

**How:**
- Mirror the existing patterns in `src/attractions/services/attraction-integration.service.spec.ts`
  and `src/parks/parks.service.spec.ts` (repository mocks via `getRepositoryToken`,
  Redis mock as plain object).
- Priority coverage, in order:
  1. `shouldSaveShowLiveData` / `shouldSaveDiningAvailability` (delta-save contract:
     status change, showtimes/waitTime change, operating-hours change, day rollover
     via `hasDateChangedInTimezone`),
  2. `findBatchCurrentStatusByShows` (stale-showtime skip: OPERATING + lastUpdated > 48h → null;
     `projectShowtimesToToday` projection),
  3. `findTodayOperatingDataByPark` (timezone filter — feed rows across a midnight boundary),
  4. `syncShows` / `syncRestaurants` (upsert behaviour, slug uniqueness, wiki-only park filter).
- Effort: ~1 day. No production code changes needed.

### 2. Generic entity sync (attractions/shows/restaurants)

**Current state:** `syncAttractions`, `syncShows`, `syncRestaurants` share the
walk-parks → fetch-children → filter-type → prefetch-existing → upsert skeleton
(~80 duplicated lines), but differ on purpose:
- attractions: also syncs from Queue-Times (`qt-`) and Wartezeiten (`wz-`) sources,
- shows: batches updates (`toUpdate[]` + `Promise.all`) and inserts separately,
- restaurants: optional `deep` mode (per-entity `getEntity()` with fallback),
  prefetch via `In(apiExternalIds)`.

**How:** template-method base class, NOT full unification:
```ts
abstract class ThemeParksEntitySync<TEntity, TChild> {
  // template: park loop + isThemeParksWikiId() skip + children fetch + prefetch maps
  protected abstract filterChildren(children: EntityChild[]): TChild[];
  protected abstract mapChild(child: TChild, parkId: string): Partial<TEntity>;
  protected abstract persist(toInsert: ..., toUpdate: ...): Promise<void>; // strategies stay per-entity
}
```
Attractions keep their qt-/wz- branches OUTSIDE the template (only the wiki branch
moves in). Don't force `deep` into the template — keep it a restaurants-only hook.
**Prereq:** item 1. Effort: ~1–2 days incl. test updates.

### 3. Queue processor batch-loop helper (NOT a base class)

**Current state:** ~21 processors in `src/queues/processors/` repeat
logger + batch loop (`BATCH_SIZE = 5`) + success/failure counters + duration log.
Redis done-markers and error semantics vary too much for inheritance.

**How:** extract only the uniform part into `src/queues/utils/batch-runner.util.ts`:
```ts
export async function runInBatches<T>(items: T[], batchSize: number,
  worker: (item: T) => Promise<void>,
): Promise<{ succeeded: number; failed: number }>
```
Adopt it opportunistically when touching a processor; don't do a big-bang rewrite.
Effort: helper ~1h, adoption incremental.

### 4. `any`-sweep in analytics.service.ts

**Current state:** ~12 `any`s in `src/analytics/analytics.service.ts`, mostly
untyped raw-SQL rows (`getRawMany()` results sorted/mapped with `(a: any, b: any)`).
Smaller offenders: `park-merge.service.ts`, `conflict-resolver.service.ts`,
`open-meteo.client.ts`, `file-logger.util.ts`.

**How:** for each raw query, declare a row type next to it (same pattern as the
`ExistingScheduleRow` type added to `parks.service.ts` in PR #68) and type the
`getRawMany<Row>()` call. Verify each field against the actual SELECT — numeric
columns come back as **strings** from pg (`parseFloat` sites are the tell).
Effort: ~0.5–1 day; mechanical but needs care with pg string-typing.

### 5. Search index bounding (only when the warning fires)

**Current state:** the 4 `loadXxxIndexFromDb` methods in `search.service.ts` are
unbounded full-table reads. There is no active/deleted flag to filter on, so a
naive LIMIT would silently drop entities from search. PR #68 added a size log +
a warning at >16 MB serialized.

**How (when the warning appears in logs):**
- preferred: add an `isSearchable`/popularity-derived flag and filter on it,
- or: split the Redis index into per-continent keys and lazy-load,
- or: switch serialization to msgpack/gzip (last resort, complexity for ~2–3x).

### 6. Python `features.py` vectorization (nice-to-have)

**Current state:** per-park `groupby` loops with `df.loc[idx, ...]` assignments for
timezone-local features (`ml-service/features.py` ~lines 51–114). Affects nightly
training wall-time only, not request latency.

**How:** group rows by timezone (not park), convert once per unique tz via
`df["timestamp"].dt.tz_convert(tz)`, assign back via `.loc[mask]`. Validate by
comparing feature output on a fixed dataset before/after (`verify_features.py`
exists for exactly this).

### 7. Decide: PoC scripts in nf-service

`nf-service/poc_eval.py` and `poc_eval_hourly.py` are standalone eval tools in the
same spirit as `backtest_*.py` / ml-service's `verify_*.py`. Kept for now.
Either document them in a README line each, or delete them. Owner call.
