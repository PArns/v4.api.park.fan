import { defineMcpTool } from "../mcp-tool";
import { AssistantRecordsService } from "../../assistant/assistant-records.service";
import { adminConversationUrl } from "./links";
import { utcDaysInput, utcDaysWindow } from "./utc-window";

/** As the admin's token statistic (`GET admin/assistant/stats/tokens`). */
const LIMITS = { maxDays: 90, defaultDays: 30 };

export const assistantTokensReportTool = defineMcpTool({
  name: "assistant_tokens_report",
  title: "Parky's tokens and cost",
  description:
    "Who spends Parky's tokens, over a window of UTC days: per day and source (web for the " +
    "site's widget, app for park.fan's app through its data key) the chats, messages, input " +
    "tokens (fresh input and cache writes), output tokens, cache-read tokens, all tokens and " +
    "the cost in USD, with totals per source, and the data keys by id and name with their sums " +
    "over the whole window. Then, from the chats still kept (keptDays), the top consumers: " +
    "visitors (a visitor is one address on one UTC day; no key or hash is returned, only its " +
    "chats, tokens and cost) and the most expensive single chats with their adminUrl. Chats from before the source was stored count " +
    "as web; for days pruned before then only the total tokens are known, not the kinds. " +
    "Numbers and ids only, no message text.",
  input: utcDaysInput(LIMITS),
  async handler(input, ctx) {
    const report = await ctx
      .get(AssistantRecordsService)
      .tokenStats(utcDaysWindow(input, LIMITS));
    return {
      ...report,
      // The day's visitor hash identifies a browser for a day: the admin shows it, a model gets the rank.
      topVisitors: report.topVisitors.map(
        ({ visitorKey: _visitorKey, ...visitor }) => visitor,
      ),
      topConversations: report.topConversations.map(
        ({ visitorKey: _visitorKey, ...chat }) => ({
          ...chat,
          adminUrl: adminConversationUrl(chat.id),
        }),
      ),
    };
  },
});
