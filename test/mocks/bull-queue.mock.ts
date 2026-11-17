import { Queue, Job } from "bull";

/**
 * Mock Bull Queue for testing
 * Prevents actual Redis connections and job processing during tests
 */
export const createMockQueue = (): Partial<Queue> => ({
  add: jest.fn().mockResolvedValue({ id: "mock-job-id", data: {} } as Job),
  process: jest.fn(),
  on: jest.fn(),
  getJob: jest.fn().mockResolvedValue(null),
  getJobs: jest.fn().mockResolvedValue([]),
  getActive: jest.fn().mockResolvedValue([]),
  getWaiting: jest.fn().mockResolvedValue([]),
  getCompleted: jest.fn().mockResolvedValue([]),
  getFailed: jest.fn().mockResolvedValue([]),
  getDelayed: jest.fn().mockResolvedValue([]),
  getRepeatableJobs: jest.fn().mockResolvedValue([]),
  removeRepeatableByKey: jest.fn().mockResolvedValue(undefined),
  clean: jest.fn().mockResolvedValue([]),
  empty: jest.fn().mockResolvedValue(undefined),
  pause: jest.fn().mockResolvedValue(undefined),
  resume: jest.fn().mockResolvedValue(undefined),
  close: jest.fn().mockResolvedValue(undefined),
  isReady: jest.fn().mockReturnValue(true),
  whenCurrentJobsFinished: jest.fn().mockResolvedValue(undefined),
  obliterate: jest.fn().mockResolvedValue(undefined),
});

/**
 * Mock job factory
 */
export const createMockJob = <T = any>(
  data: T,
  opts?: Partial<Job>,
): Partial<Job<T>> => ({
  id: "mock-job-id",
  data,
  opts: {
    attempts: 3,
    backoff: 1000,
    ...opts?.opts,
  },
  progress: jest.fn().mockResolvedValue(undefined),
  log: jest.fn().mockResolvedValue(undefined),
  moveToCompleted: jest.fn().mockResolvedValue(undefined),
  moveToFailed: jest.fn().mockResolvedValue(undefined),
  retry: jest.fn().mockResolvedValue(undefined),
  discard: jest.fn().mockResolvedValue(undefined),
  promote: jest.fn().mockResolvedValue(undefined),
  finished: jest.fn().mockResolvedValue(undefined),
  remove: jest.fn().mockResolvedValue(undefined),
  ...opts,
});
