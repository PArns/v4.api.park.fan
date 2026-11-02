### Changed — Parky knows the page you are on

Parky used to get only the slug of the page a visitor reads. He now gets the
park's name, city and country, the ride and its park, the post's title or the
glossary term, and the kind of page for everything else, cached in Redis for
six hours. A question that names nothing else ("Why?", "Saturday or
Sunday?") is about that page, so Parky looks it up instead of asking which park
is meant. The prompt (version 13) also gives him more voice and lets him tell
well-known park lore, while numbers still come from tools only. Details in
[the assistant page](frontend/assistant.md#the-page-the-visitor-is-on).
