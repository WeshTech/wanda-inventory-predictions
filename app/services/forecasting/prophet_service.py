from __future__ import annotations

from typing import Any

import pandas as pd
from prophet import Prophet


class ProphetForecastService:
    def __init__(
        self,
        daily_seasonality: bool = True,
        weekly_seasonality: bool = True,
        yearly_seasonality: bool = True,
    ) -> None:
        self.daily_seasonality = daily_seasonality
        self.weekly_seasonality = weekly_seasonality
        self.yearly_seasonality = yearly_seasonality

    def build_model(self) -> Prophet:
        model = Prophet(
            daily_seasonality=self.daily_seasonality,
            weekly_seasonality=self.weekly_seasonality,
            yearly_seasonality=self.yearly_seasonality,
        )
        return model

    def train(self, df: pd.DataFrame) -> Prophet:
        """
        Train Prophet using a dataframe with columns:
        - ds
        - y
        """
        if df.empty:
            raise ValueError("Training dataframe is empty.")

        required_columns = {"ds", "y"}
        if not required_columns.issubset(df.columns):
            raise ValueError("Training dataframe must contain 'ds' and 'y' columns.")

        model = self.build_model()
        model.fit(df)
        return model

    def forecast(
        self,
        df: pd.DataFrame,
        periods: int = 30,
    ) -> list[dict[str, Any]]:
        """
        Train and forecast future values.
        Returns forecast rows as JSON-serializable dicts.
        """
        model = self.train(df)

        future = model.make_future_dataframe(periods=periods, freq="D")
        forecast_df = model.predict(future)

        result = forecast_df[
            [
                "ds",
                "yhat",
                "yhat_lower",
                "yhat_upper",
                "trend",
            ]
        ].copy()

        result["ds"] = result["ds"].astype(str)

        return result.to_dict(orient="records")

    def forecast_only_future(
        self,
        df: pd.DataFrame,
        periods: int = 30,
    ) -> list[dict[str, Any]]:
        """
        Return only future forecast rows, excluding historical dates.
        """
        model = self.train(df)

        future = model.make_future_dataframe(periods=periods, freq="D")
        forecast_df = model.predict(future)

        last_historical_date = pd.to_datetime(df["ds"]).max()

        future_forecast = forecast_df[forecast_df["ds"] > last_historical_date].copy()

        result = future_forecast[
            [
                "ds",
                "yhat",
                "yhat_lower",
                "yhat_upper",
                "trend",
            ]
        ].copy()

        result["ds"] = result["ds"].astype(str)

        return result.to_dict(orient="records")