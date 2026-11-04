import {
  ASSUMED_TJMAX_C,
  cpuTemperatureLimits,
  cpuTemperatureSensor,
  type HwmonSensor,
} from "./cpu-temperature";

const core = (n: number, tempC: number): HwmonSensor => ({
  chip: "coretemp",
  label: `Core ${n}`,
  tempC,
  critC: 105,
});

describe("cpuTemperatureSensor", () => {
  it("takes the package, not the hottest core", () => {
    const sensors: HwmonSensor[] = [
      { chip: "nvme", label: "Composite", tempC: 31, critC: 84.8 },
      core(8, 58),
      { chip: "coretemp", label: "Package id 0", tempC: 69, critC: 105 },
      core(24, 70),
    ];
    expect(cpuTemperatureSensor(sensors)).toEqual({
      chip: "coretemp",
      label: "Package id 0",
      tempC: 69,
      critC: 105,
    });
  });

  it("takes the hotter package on a host with two sockets", () => {
    const sensors: HwmonSensor[] = [
      { chip: "coretemp", label: "Package id 0", tempC: 61 },
      { chip: "coretemp", label: "Package id 1", tempC: 74 },
    ];
    expect(cpuTemperatureSensor(sensors)?.label).toBe("Package id 1");
  });

  it("reads AMD's Tctl as the package", () => {
    const sensors: HwmonSensor[] = [
      { chip: "k10temp", label: "Tccd1", tempC: 80 },
      { chip: "k10temp", label: "Tctl", tempC: 72 },
    ];
    expect(cpuTemperatureSensor(sensors)?.label).toBe("Tctl");
  });

  it("falls back to the hottest core without a package sensor", () => {
    expect(cpuTemperatureSensor([core(0, 52), core(24, 70)])?.label).toBe(
      "Core 24",
    );
  });

  it("is null without a CPU sensor", () => {
    expect(
      cpuTemperatureSensor([{ chip: "nvme", label: "Composite", tempC: 31 }]),
    ).toBeNull();
  });
});

describe("cpuTemperatureLimits", () => {
  it("counts down from the sensor's TjMax", () => {
    expect(cpuTemperatureLimits(105)).toEqual({
      warnC: 90,
      badC: 100,
      tjMaxC: 105,
      tjMaxSource: "sensor",
    });
  });

  it("assumes a TjMax without a crit from the sensor", () => {
    expect(cpuTemperatureLimits(undefined)).toEqual({
      warnC: ASSUMED_TJMAX_C - 15,
      badC: ASSUMED_TJMAX_C - 5,
      tjMaxC: ASSUMED_TJMAX_C,
      tjMaxSource: "assumed",
    });
  });

  it("ignores a crit no CPU throttles at", () => {
    expect(cpuTemperatureLimits(60).tjMaxSource).toBe("assumed");
    expect(cpuTemperatureLimits(255).tjMaxSource).toBe("assumed");
  });
});
