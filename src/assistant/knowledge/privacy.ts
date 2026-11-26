import type { KnowledgeEntry } from "./knowledge-entry";

/**
 * The seed of the topic on what park.fan and Parky store and send. The facts follow park.fan's
 * privacy page (the sections on Parky, location and the statistics); once seeded, the stored topic
 * is edited in the admin. When the privacy page changes, this topic changes with it.
 */
export const PRIVACY: KnowledgeEntry = {
  id: "privacy",
  readWhen:
    "what park.fan and Parky store and send about the user, and for how long: chats, plans, " +
    "location, feedback, statistics, cookies. Read it when the user asks about privacy, what is " +
    "stored or who sees the chat, tracking or cookies, or how to have a chat deleted.",
  text: `A chat with Parky:
- park.fan's server stores the chat (questions, answers, the lookups behind them, ratings) and deletes it automatically after 7 days. No IP address is stored with it, only a checksum of the address, the day and a secret key, which cannot be turned back into the address.
- To write an answer, the question, the latest messages, the user's plans and reminders and the park data looked up go to the language model Claude by Anthropic. Anthropic gets no IP address and, under its terms for business customers, does not train on it.
- Plans, trips, favourites, ridden rides and reminders live in the user's browser. They travel with each question so Parky can read and change them; the plans themselves are not stored with the chat, the lookups only what they need, such as travel dates and parks.
- Location only when the user shares it in the chat, after Parky asked and they pressed the button: rounded to about ten metres, enough to find the nearest toilet or ride inside a park, used for that one answer, never stored and never handed to the language model, which sees only distances. Without it, a question about nearby parks uses a rough position from the IP address. The arrow of a navigation card reads the position in the user's own browser and sends it nowhere.
- Feedback sent through Parky is stored with the chat's ID and, only if given, an email address for a reply; it is deleted once dealt with. Someone who wants a chat deleted sooner can ask the team, and Parky passes it on with submit_feedback.
- Ask for no personal details: no names, addresses or health information in the chat.

The site sets no cookies for advertising or analytics. Its own statistic counts page views and feature use on park.fan's own server, without an ID in the browser and without recognising anyone beyond a day; Do Not Track and Global Privacy Control count as an objection, and the privacy page has a switch to object. The API writes no IP addresses into its logs. The privacy page has everything else.`,
};
