import { BadGatewayException, Injectable } from "@nestjs/common";
import axios from "axios";
import type { OverpassResponse } from "./overpass.parser";

/** A name and a way to reach us, as the Overpass instance's policy asks. */
const USER_AGENT = "park.fan-facilities/1.0 (https://park.fan; info@park.fan)";

/**
 * One query against the public Overpass API. No retry and no fallback
 * instance: an import is a person pressing a button for one park, and a busy
 * server is answered by pressing it again later, not by a loop on a free
 * service. An import is two queries at most.
 */
@Injectable()
export class OverpassClient {
  async query(ql: string): Promise<OverpassResponse> {
    let data: OverpassResponse;
    try {
      ({ data } = await axios.post<OverpassResponse>(
        process.env.OVERPASS_URL ?? "https://overpass-api.de/api/interpreter",
        new URLSearchParams({ data: ql }).toString(),
        {
          timeout: 90_000,
          headers: {
            "User-Agent": USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded",
          },
        },
      ));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new BadGatewayException(`Overpass did not answer: ${message}`);
    }
    // A timeout or memory limit arrives as HTTP 200 with a `remark` and part
    // of the elements. Importing that would delete every point it left out.
    if (data?.remark || !Array.isArray(data?.elements)) {
      throw new BadGatewayException(
        `Overpass answered incompletely: ${data?.remark ?? "no elements"}`,
      );
    }
    return data;
  }
}
