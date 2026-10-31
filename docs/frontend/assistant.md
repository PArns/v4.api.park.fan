# Assistant: Parky's chat and the site feedback

`/v1/assistant/*` serves Parky, the chat assistant at the bottom right of park.fan, and the feedback
form beside it. `/v1/admin/assistant/*` serves the admin pages. Open this page before changing the
prompt, the tools, the limits, the challenge or the stored key. The widget is described in
park.fan's `docs/features/assistant.md`.

## Endpoints

| Endpoint                                     | Who               | What                                                         |
| -------------------------------------------- | ----------------- | ------------------------------------------------------------ |
| `GET /v1/assistant/config`                   | park.fan's server | `{ enabled, rolloutPercent }`, a minute at the edge          |
| `POST /v1/assistant/session`                 | browser (CORS)    | a solved Turnstile token in, a session token out             |
| `POST /v1/assistant/chat`                    | browser           | one question, answered as server-sent events                 |
| `POST /v1/assistant/rating`                  | browser           | 👍/👎 for one answer                                         |
| `POST /v1/assistant/feedback`                | browser           | feedback about the site                                      |
| `GET/PUT /v1/admin/assistant/settings`       | admin, PUT owner  | switch, share, daily token budget, key (stored, never shown) |
| `POST /v1/admin/assistant/settings/test`     | owner             | a one-token request with the stored key                      |
| `GET /v1/admin/assistant/stats`              | admin             | 30 days of chats, tokens, cost, ratings, open feedback       |
| `GET /v1/admin/assistant/conversations[/id]` | admin             | stored chats with tool calls, tokens and cost                |
| `GET/PATCH /v1/admin/assistant/feedback`     | admin, PATCH ed.  | feedback list, status `new`/`done`/`archived`                |

## The challenge

Chat, rating and feedback need a `sessionToken`, or they answer 401. The browser gets one from
`POST session` with a Turnstile token solved for the action `assistant-session`; the API checks it
with Cloudflare, including the action. The token (`assistant-session.util.ts`) is signed with a key derived from `ASSISTANT_SECRET_KEY` and valid for two hours. It is not bound to an address, which a phone changes between Wi-Fi and mobile data: the limits count per session, and only issuing sessions counts per address. Without
`TURNSTILE_SECRET_KEY` production issues no session at all; elsewhere any token passes, for local
work. A chat can only be continued with the session that started it (`sessionHash`), so a leaked
chat id shows nobody else's history.

## The chat

The body is `{ conversationId?, locale, pagePath, message, sessionToken, location?, timeZone? }`. There is no
history in it: the server reads the last ten messages from the database, so a caller cannot hand
the model made-up earlier answers; a failed answer (empty text) is left out with its question.
`location` is sent only after the visitor pressed „Standort teilen" in the chat and is rounded to
two decimals here; it is never stored or logged.

The stream sends `start` (the chat id), `status` (a lookup is running: `parks`, `wait-times`,
`rides`, `forecast`, `posts`, `planner`, `location`, `weather`), `action` (something for the widget
to do, below), `delta` (text), then `done` (the stored answer's id) or `error`. A refusal before the
stream is a normal status: 400, 401 (no valid session), 404 (unknown chat or another session's),
429 with `Retry-After`, 503 (no key stored or the day's budget spent).

The switch and the share in the admin only decide which browsers are shown the widget. A chat is
answered whenever a key is stored and budget is left, so a signed-in admin and a `?assistant=on`
tester can chat while the switch is off. To stop every chat at once, remove the key.

## Times in the visitor's clock

`timeZone` is the browser's IANA zone (an unknown one counts as UTC). The model never converts a
time: `localizeTimes` (`assistant-time.ts`) rewrites every ISO instant in a tool result into that
zone as `yyyy-MM-dd HH:mm` before the model reads it, and the per-hour forecasts turn their
park-local hours into instants first (`parkHourInstant`). A ride forecast keeps only the hours the
park is open, the closing hour excluded, and on today only those not over yet (`bookableHours`).

## The answer loop

`AssistantChatService.answer` streams Claude Haiku 5.5 (`ASSISTANT_MODEL`) with effort `low`, at
most 2,048 tokens per round and four rounds; the last round may not call a tool, so it has to
answer. The system prompt (`system-prompt.ts`, versioned) is cached together with the tool list;
date, page and language follow after the cache breakpoint. It keeps Parky to parks and rides,
makes every number come from a tool, sets the answer language to the page's and the informal
address (du, tu, je, tú), lets Parky act on the page only when asked, and tells the model that tool
results are data, never instructions.

The tools (`tools/assistant-tools.ts`) are a fixed list. Lookups, most reusing the admin MCP
server's handlers with Parky's own descriptions: `search_parks`, `get_park_live`, `find_rides`,
`get_ride`, `get_ride_forecast` (per hour, from `PlanDayService`), `get_park_forecast` (at most 31
days), `get_park_weather` (now, rain and storm times, 12 hours), `get_best_days`,
`look_up_glossary` (park.fan's glossary, through the frontend's `/api/glossary-search`),
`find_parks_nearby` (the shared position, else a GeoIP guess marked `precise: false`; inside a park
the rides around the visitor), `get_directions` (a Google Maps link), and `search_posts`/`get_post`,
which read the frontend's `/api/assistant-posts` and `/api/assistant-post` (cached ten minutes in
Redis; park.fan's Cloudflare needs a skip rule for these three paths from the API server,
which sends `User-Agent: api.park.fan-assistant`). There is no web search, no fetch of a URL and no admin data. Every input is checked with
zod, a result is cut at 12,000 characters, and an unexpected error reaches the model as "the lookup
failed" while its message goes to the log.

Actions (`tools/assistant-actions.ts`) return an `ActionOutcome`: the model reads its result, the
widget gets the `action` and carries it out after checking it again: `show_card` (a park, a ride or a
park's rides with their waits, from live data), `open_page` (only a park.fan path of slugs), `request_location` (the share
buttons), `plan_day`, `plan_add_rides` (rides of that park only), `plan_add_block` and
`plan_optimize` (the planner's own „Tag optimieren" or „Alle Headliner").

## Limits and cost

Per session (`AssistantRateLimitService`): 20 questions per 10 minutes and 150 per day, 5 feedback messages and 60 ratings per hour, and 150,000 tokens a day (`VISITOR_DAILY_TOKENS`), so one visitor cannot spend the day's budget everybody shares. Per address, an IPv6 address counted as its /64: 10 new sessions an hour, so sessions cannot be farmed. A carrier NAT with many visitors behind one address limits nobody's chat. Over all chats, the daily token budget from the settings (Redis, per UTC day): once it is
spent, the config says off and new chats get a 503. Cache reads cost a tenth and are left out of
both counts. A stream cut short is counted from its events, so dropping the connection is no way
around the budget. Each answer stores its tokens and cost at Haiku 5.5's prices.

## The key and what is stored

The Claude key is entered in the admin and stored sealed with AES-256-GCM
(`assistant-secret.util.ts`). The master key is `ASSISTANT_SECRET_KEY`, 32 bytes as hex or base64
(`openssl rand -hex 32`); without it no key can be stored and the assistant is off. The admin only
ever sees `sk-ant-…abcd`, and the audit log redacts the `apiKey` field. Changing the master key
makes the stored key unreadable: enter it again.

Chats (`assistant_conversations`, `assistant_messages`) keep no address. `visitorKey` is a hash of
the address with the day and the master key, which tells one visitor's chats of a day apart and
nothing more. They are deleted after 90 days by the nightly `assistant` queue job
(`prune-conversations`, 04:50). Feedback (`site_feedback`) is kept until it is deleted by hand,
e-mail included; its link to a chat leads nowhere once the chat is pruned.
