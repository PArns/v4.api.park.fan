import { QueueData } from "../../src/queue-data/entities/queue-data.entity";
import {
  QueueType,
  LiveStatus,
} from "../../src/external-apis/themeparks/themeparks.types";

export const createTestQueueData = (
  attractionId: string,
  overrides?: Partial<QueueData>,
): QueueData => {
  const queueData = new QueueData();
  queueData.attractionId = attractionId;
  queueData.queueType = QueueType.STANDBY;
  queueData.status = LiveStatus.OPERATING;
  queueData.waitTime = 30;
  queueData.timestamp = new Date();
  queueData.lastUpdated = new Date();

  return Object.assign(queueData, overrides);
};

/**
 * Creates a set of test queue data entries for an attraction
 * Returns historical wait times over the past 24 hours
 */
export const createTestQueueDataHistory = (
  attractionId: string,
): QueueData[] => {
  const now = new Date();
  const queueDataEntries: QueueData[] = [];

  // Generate 24 hourly entries for the past 24 hours
  for (let i = 0; i < 24; i++) {
    const timestamp = new Date(now.getTime() - i * 60 * 60 * 1000);
    const waitTime = Math.floor(Math.random() * 60) + 10; // Random wait time 10-70 minutes

    queueDataEntries.push(
      createTestQueueData(attractionId, {
        timestamp,
        waitTime,
        lastUpdated: timestamp,
      }),
    );
  }

  return queueDataEntries;
};

/**
 * Creates queue data with different queue types
 */
export const createTestQueueDataVarieties = (
  attractionId: string,
): QueueData[] => [
  // STANDBY queue
  createTestQueueData(attractionId, {
    queueType: QueueType.STANDBY,
    waitTime: 45,
    status: LiveStatus.OPERATING,
  }),
  // SINGLE_RIDER queue
  createTestQueueData(attractionId, {
    queueType: QueueType.SINGLE_RIDER,
    waitTime: 20,
    status: LiveStatus.OPERATING,
  }),
  // RETURN_TIME (Virtual Queue)
  createTestQueueData(attractionId, {
    queueType: QueueType.RETURN_TIME,
    state: "AVAILABLE",
    returnStart: new Date(Date.now() + 60 * 60 * 1000), // 1 hour from now
    returnEnd: new Date(Date.now() + 90 * 60 * 1000), // 1.5 hours from now
    status: LiveStatus.OPERATING,
    waitTime: null,
  }),
  // PAID_RETURN_TIME (Lightning Lane)
  createTestQueueData(attractionId, {
    queueType: QueueType.PAID_RETURN_TIME,
    waitTime: 10,
    price: {
      amount: 15,
      currency: "USD",
      formatted: "$15.00",
    },
    status: LiveStatus.OPERATING,
  }),
  // BOARDING_GROUP
  createTestQueueData(attractionId, {
    queueType: QueueType.BOARDING_GROUP,
    allocationStatus: "AVAILABLE",
    currentGroupStart: 120,
    currentGroupEnd: 130,
    estimatedWait: 180,
    status: LiveStatus.OPERATING,
    waitTime: null,
  }),
];

/**
 * Creates queue data with different statuses
 */
export const createTestQueueDataStatuses = (
  attractionId: string,
): QueueData[] => [
  createTestQueueData(attractionId, {
    status: LiveStatus.OPERATING,
    waitTime: 30,
  }),
  createTestQueueData(attractionId, {
    status: LiveStatus.DOWN,
    waitTime: null,
  }),
  createTestQueueData(attractionId, {
    status: LiveStatus.CLOSED,
    waitTime: null,
  }),
  createTestQueueData(attractionId, {
    status: LiveStatus.REFURBISHMENT,
    waitTime: null,
  }),
];
