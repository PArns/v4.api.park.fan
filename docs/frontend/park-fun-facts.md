# Park fun facts

`funFacts` on the park detail payload (`GET /v1/parks/:continent/:country/:city/:parkSlug`): short,
hand-written facts about the park for Parky to tell. No feed carries them and nothing seeds them.

```json
"funFacts": {
  "en": ["First fact.", "Second fact."],
  "de": ["Erster Fakt."]
}
```

- **Keyed by locale** (`en`, `de`, `nl`, `fr`, `es`, `it`), each a list of plain-text sentences in
  that language, in the order the editor wrote them.
- **A locale nobody wrote is absent**, never filled from another language. Show the list for the
  page's locale and nothing when it is missing; an English fact on a German page is the
  half-translated screen the six-locale rule forbids.
- **The whole key is absent** when no locale has a fact (null is stripped, see the
  [conventions](README.md#conventions-every-page-assumes)).
- **All six in one payload, no `?locale=`**: the endpoint is cached per URL (5 minutes, Redis and
  CDN) and every locale of the park page reads the same one. A locale parameter would split that
  into six entries for a few hundred bytes of text.
- Only the detail payload carries it, never the listings.
- **Parky's chat** gets the same facts from `get_park_live`: the chat language's list, or the
  English one when that language has none (the model retells it). The admin MCP's `get_park`
  carries the whole map.

## Where they come from

Six `longtext` fields in the park editor (`/admin`, group „Fun Facts“): `curated_fun_facts_<locale>`,
one fact per line; blank lines and surrounding spaces are dropped. Written through
`PATCH /v1/admin/content/parks/:id` like every curated park column
([curation](../admin/curation.md)), so a save evicts the cache and revalidates the frontend.

## Machine translation

A save of any locale queues `translate-fun-facts` on the `manual-metadata` queue (3 attempts,
exponential backoff from a minute), so the save never waits. `FunFactTranslationService` translates
the saved locale (English when several were saved) into every other locale that is **empty or was
machine-translated** from another locale or another version of this text, in one call to Parky's
model (`ASSISTANT_MODEL`) with his stored key, counted against his daily token budget.

- **A locale typed by hand is never overwritten.** `parks.fun_facts_machine` (jsonb) records per
  machine locale `{ from, sourceHash }`; an editor's save of a locale removes its entry, so the field
  is theirs from then on. The editor's hint for a machine locale says so ("Maschinell übersetzt aus
  …").
- **Checked before saving:** the answer is JSON, one array per locale with exactly as many facts as
  the source has lines, each a single line of at most 300 characters, at most 2,000 per locale. A
  locale that fails is logged and dropped; the others are saved.
- **Written like an editor's save**, through `AdminCurationService.curatePark` with the actor
  `translator@park.fan` and the reason „Automatic translation from en (model)“: an audit row, undo,
  cache eviction and revalidation, so the payload and the chat see it. The park is read again
  first, so a locale an editor typed or a source changed meanwhile is left alone.
- **An emptied source** takes the machine text made from it along; hand-typed text stays.
- No key stored: logged, nothing translated. Budget spent: the job throws and the queue retries.
- A park merge that inherits a fun-fact locale takes its machine mark along
  (`inheritedMachineMarks`). An undo of a fun-fact change neither re-translates nor clears marks.
