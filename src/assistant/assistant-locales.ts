/**
 * The languages Parky answers in and switches the site to: the site's six. In a module of their
 * own, since the request schemas, the tools and the prompt all read them, and the prompt reads
 * the tools' plan lines in turn.
 */
export const ASSISTANT_LOCALES = ["en", "de", "nl", "fr", "es", "it"] as const;
export type AssistantLocale = (typeof ASSISTANT_LOCALES)[number];
