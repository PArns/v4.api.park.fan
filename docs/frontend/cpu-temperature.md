# The host's CPU temperature (`host.cpu.temperatureC`, `temperatureLimits`)

The contract for the one CPU temperature `GET /v1/admin/system-health` (and the MCP `system_health`
tool) reports, and the limits a client judges it by. Built in `src/admin/cpu-temperature.ts`
(PAR-1226).

```json
"cpu": {
  "cores": 24,
  "temperatureC": 69,
  "temperatureLimits": { "warnC": 90, "badC": 100, "tjMaxC": 105, "tjMaxSource": "sensor" }
}
```

## The figure

`temperatureC` is one sensor's reading, never an average of the cores, which hides a hot die:

1. the CPU package: Intel `coretemp` `Package id N`, AMD `k10temp`/`zenpower` `Tctl` or `Tdie`, the
   hottest of them on a host with several sockets;
2. without one, the hottest sensor of the CPU chip (`coretemp`, `k10temp`, `zenpower`,
   `cpu_thermal`);
3. without hwmon, a thermal zone whose type names the CPU (`x86_pkg_temp` …), else the first zone.

`null` when the host exposes none of these. On Intel the package reading follows the hottest core: on
2026-11-04 the production host read 69 °C for the package and 70 °C for its hottest core at 9 % load.
The package is the right figure because it stands for the whole die and the CPU's thermal control
acts on the same sensors, not because it is calmer than a core. What keeps one hot core from raising
an alarm is the limit below, not the choice of sensor.

## The limits

`temperatureLimits` is `null` exactly when `temperatureC` is. It counts down from the chip's junction
maximum (TjMax), the temperature at which the CPU starts to throttle:

| Field         | Value                                                                                             |
| ------------- | ------------------------------------------------------------------------------------------------- |
| `tjMaxC`      | the chosen sensor's `temp*_crit` (coretemp reads it from `MSR_IA32_TEMPERATURE_TARGET`), else 100 |
| `tjMaxSource` | `sensor`, or `assumed` when the sensor has no crit or one outside 85–125 °C                       |
| `warnC`       | `tjMaxC − 15`: hot enough to look at the cooling, the dust and the power limits                   |
| `badC`        | `tjMaxC − 5`: at or near the throttle point                                                       |

On the production host (Intel Core Ultra 9 275HX, TjMax 105 °C) that is amber from 90 °C and red from
100 °C. The assumed 100 °C is the low end of the 100–110 °C Intel gives for its CPUs and coretemp's own
fallback, so a chip that reports nothing is judged on the strict side.

The reading is taken once per request. There is no history, so a short spike to the red limit shows
as red until the next poll; nothing smooths it.

`host.sensors[]` carries the driver's `critC` beside a reading where the driver has one (coretemp
always does); a sensor without it has no key.

## What a client may not conclude

- A single core above `warnC` is not a finding. Only `temperatureC` is judged.
- `badC` is not damage. Intel's processors throttle at TjMax and shut down only if throttling cannot
  hold the temperature; red means the host is losing speed to heat.
- The GPU and the other sensors have no limits here; their colours are the client's own.

## Sources

- Linux kernel, [coretemp](https://docs.kernel.org/hwmon/coretemp.html): `temp*_crit` is the maximum
  junction temperature, `temp*_max` the recommended maximum (Tcontrol), the package is `temp1` and
  labelled `Package id N`.
- Intel, [Information about temperature for Intel processors](https://www.intel.com/content/www/us/en/support/articles/000005597/processors.html):
  TjMax "usually is between 100°C-110°C"; above the throttle temperature the core reduces power, and
  shuts down if that fails.
- Intel, [effects when the max temperature is approached](https://www.intel.com/content/www/us/en/support/articles/000058679.html):
  "Being at maximum temperature while running a workload isn't necessarily cause for concern."
- Intel, [Core Ultra 9 275HX specifications](https://www.intel.com/content/www/us/en/products/sku/242293/intel-core-ultra-9-processor-275hx-36m-cache-up-to-5-40-ghz/specifications.html):
  max operating temperature 105 °C, 24 cores (8 P, 16 E).
- [awesome-prometheus-alerts, host and hardware](https://samber.github.io/awesome-prometheus-alerts/rules/basic-resource-monitoring/host-and-hardware/):
  node_exporter's rules judge a sensor against its own limit, a warning for
  `node_hwmon_temp_celsius > node_hwmon_temp_max_celsius` held for 5 minutes and a critical one for
  the crit alarm, not against a fixed number.
- [Glances sensors](https://glances.readthedocs.io/en/latest/aoa/sensors.html) takes its limits from
  the system's thresholds by default; [zabbix-sensors](https://github.com/blind-oracle/zabbix-sensors)
  adds a hysteresis macro against flapping. Netdata's
  [hardware sensors](https://learn.netdata.cloud/docs/collecting-metrics/hardware-and-system/hardware-sensors)
  ships no default alert for them.
