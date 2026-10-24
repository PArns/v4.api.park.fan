"""Serving weather vs training weather (PAR-1031, PAR-1032).

Training reads one `weather_data` row per park-local day; serving has an hourly
forecast that ends after a few days, then nothing. These pin that every served
row reads its own month's climatology, that rows past the forecast are filled
from it, that the forecast is reduced to training's daily units, and that
`precipitation_last_3h` means the same thing on both sides.
"""

import pandas as pd
import pytest

from features import add_weather_features
from predict import apply_weather_features

PARK = "park-a"
TZ = "Europe/Berlin"

CLIM = pd.DataFrame(
    [
        # parkId, month, temp_max, temp_min, temp_avg, precip, wind, snow, code
        [PARK, 1, 4.0, -2.0, 1.0, 2.0, 20.0, 1.0, 71],
        [PARK, 2, 8.0, 0.0, 4.0, 3.0, 18.0, 0.5, 61],
        [PARK, 7, 28.0, 16.0, 22.0, 4.0, 12.0, 0.0, 3],
    ],
    columns=[
        "parkId",
        "month",
        "temp_max",
        "temp_min",
        "temp_avg",
        "precip_avg",
        "wind_avg",
        "snow_avg",
        "weather_code_mode",
    ],
)


def rows(local_times, rides=1):
    """Prediction rows at these park-local wall-clock times, `rides` per time."""
    local = pd.to_datetime(local_times)
    utc = local.tz_localize(TZ).tz_convert("UTC")
    frames = [
        pd.DataFrame(
            {
                "attractionId": f"ride-{r}",
                "parkId": PARK,
                "timestamp": utc,
                "local_timestamp": local,
                "hour": local.hour,
            }
        )
        for r in range(rides)
    ]
    return pd.concat(frames, ignore_index=True)


def hourly_forecast(local_day, precip_per_hour=1.0, hours=24):
    start = pd.Timestamp(local_day).tz_localize(TZ).tz_convert("UTC")
    times = pd.date_range(start, periods=hours, freq="h")
    return pd.DataFrame(
        {
            "time": times,
            "temperature": 10.0,
            "precipitation": precip_per_hour,
            "rain": precip_per_hour,
            "snowfall": 0.0,
            "weatherCode": [3] * (hours - 1) + [63],
            "windSpeed": list(range(hours)),
        }
    )


def test_each_row_reads_its_own_months_climatology():
    df = rows(["2026-01-31 14:00", "2026-02-01 14:00"])
    out = apply_weather_features(df, None, CLIM, {PARK: TZ})

    # 14:00 is the sinusoid's maximum
    assert out["temperature_avg"].tolist() == pytest.approx([4.0, 8.0])
    assert out["precipitation"].tolist() == pytest.approx([2.0, 3.0])
    assert out["weatherCode"].tolist() == [71, 61]
    # deviation against the row's own month: 4-1, 8-4
    assert out["temperature_deviation"].tolist() == pytest.approx([3.0, 4.0])


def test_rows_beyond_the_forecast_read_climatology_not_constants():
    df = rows(["2026-07-01 14:00", "2026-07-20 14:00"])
    out = apply_weather_features(df, hourly_forecast("2026-07-01"), CLIM, {PARK: TZ})

    covered, beyond = out.iloc[0], out.iloc[1]
    assert covered["temperature_avg"] == pytest.approx(10.0)  # the forecast hour
    assert beyond["temperature_avg"] == pytest.approx(28.0)  # not 20 °C
    assert beyond["precipitation"] == pytest.approx(4.0)  # not 0 mm
    assert beyond["weatherCode"] == 3  # not 0
    assert beyond["windSpeedMax"] == pytest.approx(12.0)


def test_forecast_is_reduced_to_training_daily_units():
    df = rows(["2026-07-01 10:00", "2026-07-01 18:00"])
    out = apply_weather_features(df, hourly_forecast("2026-07-01"), CLIM, {PARK: TZ})

    # 24 h x 1 mm = the day's total on every row of the day, like weather_data
    assert out["precipitation"].tolist() == pytest.approx([24.0, 24.0])
    assert out["windSpeedMax"].tolist() == pytest.approx([23.0, 23.0])
    assert out["weatherCode"].tolist() == [63, 63]
    assert out["precipitation_last_3h"].tolist() == pytest.approx([3.0, 3.0])


def test_short_forecast_day_falls_back_to_climatology():
    df = rows(["2026-07-01 10:00"])
    fc = hourly_forecast("2026-07-01", hours=4)
    out = apply_weather_features(df, fc, CLIM, {PARK: TZ})
    assert out["precipitation"].iloc[0] == pytest.approx(4.0)


def test_partial_forecast_day_is_scaled_to_24_hours():
    df = rows(["2026-07-01 20:00"])
    fc = hourly_forecast("2026-07-01", hours=12)
    out = apply_weather_features(df, fc, CLIM, {PARK: TZ})
    assert out["precipitation"].iloc[0] == pytest.approx(24.0)


def training_frame(rides):
    df = rows(
        ["2026-07-01 10:00", "2026-07-01 11:00", "2026-07-01 12:00"], rides=rides
    )
    df["month"] = 7
    df["temperatureMax"] = 28.0
    df["temperatureMin"] = 16.0
    df["precipitation"] = 8.0  # the day's total, as weather_data stores it
    df["windSpeedMax"] = 10.0
    df["snowfallSum"] = 0.0
    df["weatherCode"] = 61
    return df


def test_training_precipitation_last_3h_does_not_scale_with_ride_count():
    one = add_weather_features(training_frame(1))
    five = add_weather_features(training_frame(5))
    assert set(one["precipitation_last_3h"]) == {1.0}
    assert set(five["precipitation_last_3h"]) == {1.0}


def test_precipitation_last_3h_has_one_definition_on_both_sides():
    train = add_weather_features(training_frame(3))
    clim = CLIM.copy()
    clim.loc[clim["month"] == 7, "precip_avg"] = 8.0
    served = apply_weather_features(
        rows(["2026-07-01 12:00"]), None, clim, {PARK: TZ}
    )
    assert served["precipitation"].iloc[0] == train["precipitation"].iloc[0]
    assert (
        served["precipitation_last_3h"].iloc[0]
        == train["precipitation_last_3h"].iloc[0]
    )


def test_sinusoid_peaks_at_14_on_both_sides():
    """Training's curve had its MINIMUM at 14:00 (sign error), the opposite of
    the real hourly forecast served beside it."""
    train = training_frame(1).assign(hour=[2, 14, 8])
    train = add_weather_features(train)
    assert train["temperature_avg"].tolist() == pytest.approx([16.0, 28.0, 22.0])
    served = apply_weather_features(
        rows(["2026-07-01 02:00", "2026-07-01 14:00"]), None, CLIM, {PARK: TZ}
    )
    assert served["temperature_avg"].tolist() == pytest.approx([16.0, 28.0])


def test_no_climatology_and_no_forecast_keeps_neutral_defaults():
    out = apply_weather_features(rows(["2026-07-01 14:00"]), None, None, {PARK: TZ})
    assert out["temperature_avg"].iloc[0] == pytest.approx(25.0)
    assert out["precipitation"].iloc[0] == 0.0
    assert out["weatherCode"].iloc[0] == 0
