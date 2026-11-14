### Added — Parky reads and changes the visitor's plans, offers replies to tap and interviews

Every question now carries the trip planner's state per region (`trips`: the hub's own query, the
one on screen and the last one the browser remembers) and every planned day of the day planner,
eight in full with their minute past midnight and show slug, the rest by park and date. For the
trip on screen the chat plans it before the answer, within 1.5 s and from the plan route's Redis
entry, and lists it a line a day, so Parky gives one hint worth knowing unasked: a party night, a
ride in maintenance, a quieter day nearby, a ticket that misses a day, the storm season.

New tools: `get_trip_plan` reads the visitor's trip as `plan_vacation` retells a plan;
`change_trip` changes it through the planner's own query (dates, parks, a pin, a swap, rest days,
tickets, hotel, pace, `moveBuffer` in a region with bases) and says which days moved and the big
rides' wait before and after; `trip_undo` takes Parky's last change back; `review_plan_day` prices
a planned day with the day optimizer ("how long do I wait in total"); `suggest_replies` puts two to
four tappable replies under an answer and ends it without another round; `interview` leads a trip
or a park day from simple to detailed, a plan after the first few answers and the rest offered as replies, each answer applied at once, and asks in Japan how a hotel
change should go, with the luggage courier. `plan_move` moves an entry to a time, `plan_trip`
files the party's smallest rider with the days. Prompt version 53; 15 eval cases
([assistant](frontend/assistant.md#the-visitors-plans)).
