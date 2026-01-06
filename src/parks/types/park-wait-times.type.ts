/**
 * Park Wait Times Types
 *
 * Used for park wait times responses
 */

import { ParkStatus } from "../../common/types/status.type";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";

export interface ParkInfo {
  id: string;
  name: string;
  slug: string;
  timezone: string;
  status: ParkStatus;
}

export interface AttractionWithQueues {
  attraction: {
    id: string;
    name: string;
    slug: string;
  };
  queues: QueueDataItemDto[];
}

export interface ParkWaitTimesResponse {
  park: ParkInfo;
  attractions: AttractionWithQueues[];
}
