from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any
import math

import pandas as pd

try:
    from prophet import Prophet
except Exception:
    Prophet = None


@dataclass
class ForecastResult:
    daily_forecast: float
    next_7_days_units: float
    next_30_days_units: float
    trend: str


class ForecastService:
    @staticmethod
    def _fallback_forecast(history: List[Dict[str, Any]]) -> ForecastResult:
        if not history:
            return ForecastResult(
                daily_forecast=0.0,
                next_7_days_units=0.0,
                next_30_days_units=0.0,
                trend="flat",
            )

        values = [float(x["units"]) for x in history]
        if not values:
            return ForecastResult(0.0, 0.0, 0.0, "flat")

        last_7 = values[-7:] if len(values) >= 7 else values
        daily = sum(last_7) / max(len(last_7), 1)

        if len(values) >= 14:
            prev_7 = values[-14:-7]
            prev_avg = sum(prev_7) / max(len(prev_7), 1)
            if daily > prev_avg * 1.10:
                trend = "up"
            elif daily < prev_avg * 0.90:
                trend = "down"
            else:
                trend = "flat"
        else:
            trend = "flat"

        return ForecastResult(
            daily_forecast=round(daily, 4),
            next_7_days_units=round(daily * 7, 4),
            next_30_days_units=round(daily * 30, 4),
            trend=trend,
        )

    @staticmethod
    def forecast_daily_units(history: List[Dict[str, Any]]) -> ForecastResult:
        """
        history: [{"date": "2026-01-01", "units": 4}, ...]
        """
        if len(history) < 14 or Prophet is None:
            return ForecastService._fallback_forecast(history)

        df = pd.DataFrame(history)
        df["ds"] = pd.to_datetime(df["date"])
        df["y"] = pd.to_numeric(df["units"], errors="coerce").fillna(0.0)
        df = df[["ds", "y"]].sort_values("ds")

        if df["y"].sum() <= 0:
            return ForecastResult(0.0, 0.0, 0.0, "flat")

        try:
            model = Prophet(
                daily_seasonality=False,
                weekly_seasonality=True,
                yearly_seasonality=False,
                changepoint_prior_scale=0.1,
            )
            model.fit(df)

            future = model.make_future_dataframe(periods=30)
            forecast = model.predict(future)

            future_only = forecast.tail(30).copy()
            next_7 = future_only.head(7)
            next_30 = future_only.head(30)

            next_7_sum = max(float(next_7["yhat"].sum()), 0.0)
            next_30_sum = max(float(next_30["yhat"].sum()), 0.0)
            daily_forecast = next_30_sum / 30.0 if next_30_sum > 0 else 0.0

            recent_fitted = forecast.tail(14)["trend"].tolist()
            trend = "flat"
            if len(recent_fitted) >= 2:
                if recent_fitted[-1] > recent_fitted[0] * 1.02:
                    trend = "up"
                elif recent_fitted[-1] < recent_fitted[0] * 0.98:
                    trend = "down"

            return ForecastResult(
                daily_forecast=round(daily_forecast, 4),
                next_7_days_units=round(next_7_sum, 4),
                next_30_days_units=round(next_30_sum, 4),
                trend=trend,
            )
        except Exception:
            return ForecastService._fallback_forecast(history)