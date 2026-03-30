from __future__ import annotations

from typing import Any

import pandas as pd


class ForecastDataPrepService:
    @staticmethod
    def prepare_timeseries(
        records: list[dict[str, Any]],
        date_column: str = "sale_date",
        value_column: str = "total_sales",
        fill_missing: bool = True,
        fill_value: float = 0.0,
    ) -> pd.DataFrame:
        """
        Convert repository records into a Prophet-ready dataframe.

        Output columns:
        - ds: datetime
        - y: numeric target value

        Example input:
        [
            {"sale_date": "2026-01-01", "total_sales": 1000},
            {"sale_date": "2026-01-02", "total_sales": 1500},
        ]
        """
        if not records:
            return pd.DataFrame(columns=["ds", "y"])

        df = pd.DataFrame(records).copy()

        if date_column not in df.columns:
            raise ValueError(f"Missing required date column: {date_column}")

        if value_column not in df.columns:
            raise ValueError(f"Missing required value column: {value_column}")

        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df[value_column] = pd.to_numeric(df[value_column], errors="coerce")

        df = df.dropna(subset=[date_column, value_column])

        df = df.rename(
            columns={
                date_column: "ds",
                value_column: "y",
            }
        )

        df = df[["ds", "y"]].sort_values("ds").reset_index(drop=True)

        if fill_missing and not df.empty:
            full_range = pd.date_range(
                start=df["ds"].min(),
                end=df["ds"].max(),
                freq="D",
            )

            df = (
                df.set_index("ds")
                .reindex(full_range, fill_value=fill_value)
                .rename_axis("ds")
                .reset_index()
            )

        df["y"] = df["y"].astype(float)

        return df

    @staticmethod
    def prepare_sales_forecast_data(
        records: list[dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Prepare daily store/business sales data for Prophet.
        Expects keys:
        - sale_date
        - total_sales
        """
        return ForecastDataPrepService.prepare_timeseries(
            records=records,
            date_column="sale_date",
            value_column="total_sales",
            fill_missing=True,
            fill_value=0.0,
        )

    @staticmethod
    def prepare_product_quantity_forecast_data(
        records: list[dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Prepare daily product quantity data for Prophet.
        Expects keys:
        - sale_date
        - total_quantity
        """
        return ForecastDataPrepService.prepare_timeseries(
            records=records,
            date_column="sale_date",
            value_column="total_quantity",
            fill_missing=True,
            fill_value=0.0,
        )

    @staticmethod
    def validate_minimum_history(
        df: pd.DataFrame,
        minimum_days: int = 30,
    ) -> None:
        """
        Ensure enough historical data exists before training.
        """
        if df.empty:
            raise ValueError("No historical data available for forecasting.")

        if len(df) < minimum_days:
            raise ValueError(
                f"Not enough historical data for forecasting. "
                f"At least {minimum_days} days are required, got {len(df)}."
            )