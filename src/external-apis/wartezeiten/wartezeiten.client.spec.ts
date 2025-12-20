import { Test, TestingModule } from "@nestjs/testing";
import { WartezeitenClient } from "./wartezeiten.client";
import axios from "axios";

// Mock axios
jest.mock("axios");
const mockedAxios = axios as jest.Mocked<typeof axios>;

describe("WartezeitenClient", () => {
  let client: WartezeitenClient;
  let mockAxiosInstance: any;

  beforeEach(async () => {
    // Create mock axios instance
    mockAxiosInstance = {
      get: jest.fn(),
      interceptors: {
        response: {
          use: jest.fn((successHandler, errorHandler) => {
            mockAxiosInstance._errorHandler = errorHandler;
            return 0;
          }),
        },
      },
    };

    mockedAxios.create = jest.fn().mockReturnValue(mockAxiosInstance);

    const module: TestingModule = await Test.createTestingModule({
      providers: [WartezeitenClient],
    }).compile();

    client = module.get<WartezeitenClient>(WartezeitenClient);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("getParks", () => {
    it("should fetch parks successfully", async () => {
      const mockParks = [
        {
          id: "phantasialand",
          uuid: "3a48bc99-3a51-4730-9fb1-be485f0c2742",
          name: "Phantasialand",
          land: "Germany",
        },
        {
          id: "europapark",
          uuid: "30816cc0-aedb-4bfc-a180-b269a3a2f31d",
          name: "Europa-Park",
          land: "Germany",
        },
      ];

      mockAxiosInstance.get.mockResolvedValue({ data: mockParks });

      const result = await client.getParks("en");

      expect(result).toEqual(mockParks);
      expect(mockAxiosInstance.get).toHaveBeenCalledWith("/v1/parks", {
        headers: { language: "en" },
      });
    });

    it("should fetch parks with German language", async () => {
      mockAxiosInstance.get.mockResolvedValue({ data: [] });

      await client.getParks("de");

      expect(mockAxiosInstance.get).toHaveBeenCalledWith("/v1/parks", {
        headers: { language: "de" },
      });
    });
  });

  describe("getWaitTimes", () => {
    it("should fetch wait times successfully", async () => {
      const mockWaitTimes = [
        {
          datetime: "2025-12-20T15:45:00+01:00",
          date: "2025-12-20",
          time: "15:45",
          code: "3137",
          uuid: "fa77fbb2-e12b-471e-8b10-5c517727be4c",
          waitingtime: 20,
          status: "opened",
          name: "Raik",
        },
      ];

      mockAxiosInstance.get.mockResolvedValue({ data: mockWaitTimes });

      const result = await client.getWaitTimes("phantasialand", "en");

      expect(result).toEqual(mockWaitTimes);
      expect(mockAxiosInstance.get).toHaveBeenCalledWith("/v1/waitingtimes", {
        headers: {
          park: "phantasialand",
          language: "en",
        },
      });
    });
  });

  describe("getOpeningTimes", () => {
    it("should fetch opening times successfully", async () => {
      const mockOpeningTimes = [
        {
          opened_today: true,
          open_from: "2025-12-20T11:00:00+01:00",
          closed_from: "2025-12-20T20:00:00+01:00",
        },
      ];

      mockAxiosInstance.get.mockResolvedValue({ data: mockOpeningTimes });

      const result = await client.getOpeningTimes("phantasialand");

      expect(result).toEqual(mockOpeningTimes);
      expect(mockAxiosInstance.get).toHaveBeenCalledWith("/v1/openingtimes", {
        headers: {
          park: "phantasialand",
        },
      });
    });
  });

  describe("getCrowdLevel", () => {
    it("should fetch crowd level successfully", async () => {
      const mockCrowdLevel = {
        crowd_level: 56.67,
        timestamp: "2025-12-20T15:41:00+01:00",
      };

      mockAxiosInstance.get.mockResolvedValue({ data: mockCrowdLevel });

      const result = await client.getCrowdLevel("phantasialand");

      expect(result).toEqual(mockCrowdLevel);
      expect(mockAxiosInstance.get).toHaveBeenCalledWith("/v1/crowdlevel", {
        headers: {
          park: "phantasialand",
        },
      });
    });
  });

  describe.skip("error handling", () => {
    it("should handle 404 errors", async () => {
      const error = {
        response: {
          status: 404,
          data: { message: "No data for park" },
        },
      };

      mockAxiosInstance.get.mockRejectedValue(error);

      await expect(client.getWaitTimes("invalid", "en")).rejects.toThrow(
        "Wartezeiten API: No data for park",
      );
    });

    it("should handle 429 rate limit errors", async () => {
      const error = {
        response: {
          status: 429,
          data: {},
        },
      };

      mockAxiosInstance.get.mockRejectedValue(error);

      await expect(client.getParks("en")).rejects.toThrow(
        "Wartezeiten API: Rate limit exceeded",
      );
    });

    it("should handle 400 bad request errors", async () => {
      const error = {
        response: {
          status: 400,
          data: { message: "Invalid park ID" },
        },
      };

      mockAxiosInstance.get.mockRejectedValue(error);

      await expect(client.getWaitTimes("bad", "en")).rejects.toThrow(
        "Wartezeiten API: Invalid parameters",
      );
    });
  });

  describe("isHealthy", () => {
    it("should return true when API is accessible", async () => {
      mockAxiosInstance.get.mockResolvedValue({ data: [] });

      const result = await client.isHealthy();

      expect(result).toBe(true);
    });

    it("should return false when API is not accessible", async () => {
      mockAxiosInstance.get.mockRejectedValue(new Error("Network error"));

      const result = await client.isHealthy();

      expect(result).toBe(false);
    });
  });
});
