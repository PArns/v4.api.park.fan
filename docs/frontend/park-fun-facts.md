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
([curation](../admin/curation.md)), so a save evicts the cache and revalidates the frontend. There is
no automatic translation: each language is typed into its own field.
