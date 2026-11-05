/**
 * The host's one CPU temperature and the limits it is judged by, for
 * `GET /v1/admin/system-health`. Contract and sources:
 * docs/frontend/cpu-temperature.md.
 */

/** One hwmon temperature input. `critC` is the driver's `temp*_crit` where it has one. */
export interface HwmonSensor {
  chip: string;
  label: string;
  tempC: number;
  critC?: number;
}

/** Where the CPU's colour changes, counted down from the chip's junction maximum. */
export interface CpuTemperatureLimits {
  warnC: number;
  badC: number;
  tjMaxC: number;
  /** `sensor`: TjMax is the sensor's own `temp*_crit`; `assumed`: it gave none. */
  tjMaxSource: "sensor" | "assumed";
}

const CPU_CHIP = /coretemp|k10temp|zenpower|cpu_thermal/i;
const PACKAGE_LABEL = /package|tctl|tdie/i;

/** The lowest TjMax Intel names for its current CPUs, and coretemp's own fallback. */
export const ASSUMED_TJMAX_C = 100;
const WARN_BELOW_TJMAX_C = 15;
const BAD_BELOW_TJMAX_C = 5;
// A crit outside this range is a driver default, not a junction maximum.
const PLAUSIBLE_TJMAX_C = { min: 85, max: 125 };

function hottest(sensors: readonly HwmonSensor[]): HwmonSensor {
  return sensors.reduce((hot, s) => (s.tempC > hot.tempC ? s : hot));
}

/**
 * The CPU sensor that stands for the whole chip: the package (Intel
 * `Package id N`, AMD `Tctl`/`Tdie`), the hottest of them on a multi-socket
 * host, else the hottest core. Never an average, which hides a hot die.
 */
export function cpuTemperatureSensor(
  sensors: readonly HwmonSensor[],
): HwmonSensor | null {
  const cpu = sensors.filter((s) => CPU_CHIP.test(s.chip));
  if (!cpu.length) return null;
  const packages = cpu.filter((s) => PACKAGE_LABEL.test(s.label));
  return hottest(packages.length ? packages : cpu);
}

/** Amber 15 °C and red 5 °C under TjMax, the point where the chip starts to throttle. */
export function cpuTemperatureLimits(
  critC: number | null | undefined,
): CpuTemperatureLimits {
  const fromSensor =
    critC != null &&
    critC >= PLAUSIBLE_TJMAX_C.min &&
    critC <= PLAUSIBLE_TJMAX_C.max;
  const tjMaxC = fromSensor ? critC : ASSUMED_TJMAX_C;
  return {
    warnC: tjMaxC - WARN_BELOW_TJMAX_C,
    badC: tjMaxC - BAD_BELOW_TJMAX_C,
    tjMaxC,
    tjMaxSource: fromSensor ? "sensor" : "assumed",
  };
}
