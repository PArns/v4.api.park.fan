import { getFrontendUrl } from "../../config/oauth.config";
import {
  frontendAttractionPath,
  frontendParkPath,
} from "../../common/utils/frontend-url.util";
import type { GeocodedEntity } from "../../common/utils/url.util";

/** The park's page on park.fan, so an answer can link where a person looks. */
export function parkPageUrl(park: GeocodedEntity): string | null {
  const path = frontendParkPath(park);
  return path ? `${getFrontendUrl()}${path}` : null;
}

/** One of Parky's chats in the admin, where a person reads it with the feedback's email. */
export function adminConversationUrl(id: string): string {
  return `${getFrontendUrl()}/admin/assistant/conversations/${id}`;
}

/** The admin's feedback list. */
export function adminFeedbackUrl(): string {
  return `${getFrontendUrl()}/admin/assistant/feedback`;
}

/** The admin's page of Parky's knowledge base, or one topic on it. */
export function adminKnowledgeUrl(topic?: string): string {
  const base = `${getFrontendUrl()}/admin/assistant/knowledge`;
  return topic ? `${base}/${topic}` : base;
}

/** The ride's page on park.fan. */
export function attractionPageUrl(
  park: GeocodedEntity,
  attraction: { slug: string },
): string | null {
  const path = frontendAttractionPath(park, attraction);
  return path ? `${getFrontendUrl()}${path}` : null;
}
