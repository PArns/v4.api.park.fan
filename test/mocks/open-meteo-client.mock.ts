/**
 * Mock Open-Meteo API Client for testing
 * Returns predictable weather data for deterministic tests
 */
export class MockOpenMeteoClient {
  async getForecast(latitude: number, longitude: number) {
    const today = new Date();
    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);

    const dates: string[] = [];
    const maxTemps: number[] = [];
    const minTemps: number[] = [];
    const precipitation: number[] = [];
    const precipProb: number[] = [];
    const windSpeed: number[] = [];

    // Generate 7 days of forecast
    for (let i = 0; i < 7; i++) {
      const date = new Date(today);
      date.setDate(date.getDate() + i);
      dates.push(date.toISOString().split("T")[0]);

      // Predictable weather data
      maxTemps.push(22 + i); // 22-28°C
      minTemps.push(15 + i); // 15-21°C
      precipitation.push(i % 3 === 0 ? 5 : 0); // Rain every 3rd day
      precipProb.push(i % 3 === 0 ? 60 : 10); // 60% chance on rain days
      windSpeed.push(10 + i); // 10-16 km/h
    }

    return {
      latitude,
      longitude,
      timezone: "America/New_York",
      daily: {
        time: dates,
        temperature_2m_max: maxTemps,
        temperature_2m_min: minTemps,
        precipitation_sum: precipitation,
        precipitation_probability_max: precipProb,
        windspeed_10m_max: windSpeed,
      },
    };
  }

  async getHistorical(
    latitude: number,
    longitude: number,
    startDate: string,
    endDate: string,
  ) {
    const start = new Date(startDate);
    const end = new Date(endDate);
    const days = Math.ceil(
      (end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24),
    );

    const dates: string[] = [];
    const maxTemps: number[] = [];
    const minTemps: number[] = [];
    const precipitation: number[] = [];
    const windSpeed: number[] = [];

    for (let i = 0; i <= days; i++) {
      const date = new Date(start);
      date.setDate(date.getDate() + i);
      dates.push(date.toISOString().split("T")[0]);

      // Historical data (slightly different pattern)
      maxTemps.push(20 + (i % 7)); // 20-26°C
      minTemps.push(14 + (i % 5)); // 14-18°C
      precipitation.push(i % 4 === 0 ? 8 : 0); // Rain every 4th day
      windSpeed.push(12 + (i % 6)); // 12-17 km/h
    }

    return {
      latitude,
      longitude,
      timezone: "America/New_York",
      daily: {
        time: dates,
        temperature_2m_max: maxTemps,
        temperature_2m_min: minTemps,
        precipitation_sum: precipitation,
        windspeed_10m_max: windSpeed,
      },
    };
  }
}

/**
 * Factory function to create a mock Open-Meteo client
 */
export const createMockOpenMeteoClient = () => new MockOpenMeteoClient();
