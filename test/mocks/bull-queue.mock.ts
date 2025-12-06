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
  removeRepeatableByKey: jest.fn().mockResolvedValue(),
  clean: jest.fn().mockResolvedValue([]),
  empty: jest.fn().mockResolvedValue(),
  pause: jest.fn().mockResolvedValue(),
  resume: jest.fn().mockResolvedValue(),
  close: jest.fn().mockResolvedValue(),
  isReady: jest.fn().mockReturnValue(true),
  whenCurrentJobsFinished: jest.fn().mockResolvedValue(),
  obliterate: jest.fn().mockResolvedValue(),
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
  progress: jest.fn().mockResolvedValue(),
  log: jest.fn().mockResolvedValue(),
  moveToCompleted: jest.fn().mockResolvedValue(),
  moveToFailed: jest.fn().mockResolvedValue(),
  retry: jest.fn().mockResolvedValue(),
  discard: jest.fn().mockResolvedValue(),
  promote: jest.fn().mockResolvedValue(),
  finished: jest.fn().mockResolvedValue(),
  remove: jest.fn().mockResolvedValue(),
  ...opts,
});
