/**
 * Feature Context Types
 *
 * Used for ML predictions to provide real-time context
 */

export interface QueueDataInfo {
  queueType: string;
  status: string;
}

export interface FeatureContext {
  parkOccupancy?: Record<string, number>;
  parkOpeningTimes?: Record<string, string>;
  downtimeCache?: Record<string, number>;
  queueData?: Record<string, QueueDataInfo>;
  isBridgeDay?: Record<string, boolean>;
  parkHasSchedule?: Record<string, boolean>;
  isSchoolHoliday?: Record<string, boolean>;
}
