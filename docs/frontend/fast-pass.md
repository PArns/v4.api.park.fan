# The queue-jump product on a ride (`fastPass`)

**What it answers:** can this ride be skipped for money, what does the park call
the thing, and what does it cost.

Present on the attraction detail payload and on every ride in the park payload,
so a ride list can badge without a second request.

```jsonc
"fastPass": {
  "name": "QuickPass",      // never empty — see below
  "price": 12,              // 0 means FREE. null means unknown.
  "currency": "EUR",        // ISO-4217, null when there is no price to denominate
  "termId": "quick-pass"    // glossary term id, for the link out
}
```

## Rendering

```
name only            → "QuickPass"
name + price         → "QuickPass: 12 €"
name + price === 0   → "QuickPass (kostenlos)"
```

Compose it in the frontend, with `Intl.NumberFormat(locale, { style: 'currency',
currency })`. The API deliberately sends the parts: "12 €" and "€12" are the same
price in two locales, and the API does not know which of the six the reader is
in.

**`price` is a number that can be `0`, and `0` is a real answer.** Europa-Park's
Virtual Line is a queue-jump product included with admission. `if (price)` drops
it and renders a paid product as a free-looking one; test `price !== null`.

`termId` is a glossary term id — the same kind the ride profile stores. Link the
name to the glossary entry when the id resolves, and render plain text when it
does not, exactly as `RideProfileSection` treats a manufacturer.

## The absence

**A missing `fastPass` is not "this ride has no fast pass."** It covers two
states that the API deliberately does not distinguish:

- nobody has checked this ride yet, and
- somebody checked and the park sells no such product.

Both are recorded in the admin — the second one so the next editor does not
search again — and both reach the client as nothing. Never render "kein
Fastpass", "no express pass available", or a struck-through badge from an absent
object: most of the ~7000 attractions have never been looked at, and that
sentence would be our own bookkeeping presented as the park's statement.

Render the badge when the object is there. Render nothing when it is not.

## Where the values come from

Nothing we ingest publishes these products; they live in park apps and on ticket
pages. Everything here is hand-curated, and it is spread over two rows:

| Part                       | Lives on   | Why                                        |
| -------------------------- | ---------- | ------------------------------------------ |
| name, currency, glossary id | the park   | It is a brand, and it is the same 40 times |
| flag, price, name override | the ride   | These differ ride by ride                  |

`name` falls back through the ride's override, then the park's brand, then the
neutral **"Fast Pass"** — so a flagged ride always has something to label. The
park's own name and currency are also on the park payload's `info` block
(`info.fastPassName`, `info.currency`, `info.fastPassTermId`), for a park page
that wants to name the product without walking the ride list.

The backend rules are in [admin/curation.md](../admin/curation.md#fast-passes-across-two-rows).
