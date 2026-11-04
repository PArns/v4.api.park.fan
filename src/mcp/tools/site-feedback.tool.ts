import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { AssistantRecordsService } from "../../assistant/assistant-records.service";
import { FEEDBACK_STATUSES } from "../../assistant/entities/site-feedback.entity";
import { feedbackEntry } from "./assistant-records";
import { adminFeedbackUrl } from "./links";
import {
  CHAT_DATA_DESCRIPTION,
  CHAT_DATA_NOTICE,
  withVisitorData,
} from "./visitor-data";

export const siteFeedbackTool = defineMcpTool({
  name: "site_feedback",
  title: "Visitor feedback",
  description:
    "Feedback visitors sent from the feedback tab of Parky's chat window, newest first: the text, " +
    "when, the site language, the page it was sent from, its status (new until an admin marks it " +
    "done or archived), whether the visitor left an email address for a reply (the address itself " +
    "only in the admin, adminUrl) and the chat it came from, which assistant_conversation reads. " +
    "Feedback is kept until an admin deletes it. " +
    CHAT_DATA_DESCRIPTION,
  input: {
    status: z
      .enum(FEEDBACK_STATUSES)
      .optional()
      .describe('Only this status; "new" for the open ones'),
    page: z.number().int().min(1).max(1000).default(1).describe("Page, from 1"),
    pageSize: z
      .number()
      .int()
      .min(1)
      .max(50)
      .default(20)
      .describe("Entries per page"),
  },
  async handler({ status, page, pageSize }, ctx) {
    const list = await ctx
      .get(AssistantRecordsService)
      .listFeedback({ status, page, pageSize });
    return withVisitorData(
      {
        total: list.total,
        page: list.page,
        pageSize: list.pageSize,
        adminUrl: adminFeedbackUrl(),
      },
      { feedback: list.items.map(feedbackEntry) },
      CHAT_DATA_NOTICE,
    );
  },
});
