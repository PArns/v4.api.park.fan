import type { KnowledgeEntry } from "./knowledge-entry";

/**
 * The seed of the topic on park.fan's app. It moved out of the prompt so the answer can change
 * on the day the app reaches a store, in the admin and without a deploy; once seeded, the stored
 * topic is edited there.
 */
export const APP: KnowledgeEntry = {
  id: "app",
  readWhen:
    "whether park.fan has an app for iPhone or Android, when it comes and how to use park.fan " +
    "like an app until then. Read it when the user asks for the app, the App Store, Google Play " +
    "or how to put park.fan on the home screen.",
  text: `park.fan's app for iPhone is on its way to the App Store and comes soon; there is no date yet. For Android there is no app yet.
Until then park.fan runs in the browser and goes on the home screen like an app: on an iPhone, Share and then Add to Home Screen; on Android, the browser menu and then Install app or Add to Home screen.
Say it gladly in one or two sentences. Never promise a date, a feature or an Android app; once the app is in the store, this topic says so with its link.`,
};
