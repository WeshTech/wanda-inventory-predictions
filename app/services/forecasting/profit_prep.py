from __future__ import annotations

from typing import Any

import pandas as pd


class ProfitDataPrepService:
    @staticmethod
    def prepare_profit_timeseries(
        sales_records: list[dict[str, Any]],
        expense_records: list[dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Build a Prophet-ready profit dataframe with:
        - ds
        - y   (daily profit = sales - expenses)
        """

        sales_df = pd.DataFrame(sales_records).copy()
        expense_df = pd.DataFrame(expense_records).copy()

        if sales_df.empty:
            return pd.DataFrame(columns=["ds", "y"])

        if not sales_df.empty:
            sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"], errors="coerce")
            sales_df["total_sales"] = pd.to_numeric(sales_df["total_sales"], errors="coerce")
            sales_df = sales_df.dropna(subset=["sale_date", "total_sales"])
            sales_df = sales_df[["sale_date", "total_sales"]]
            sales_df = sales_df.rename(columns={"sale_date": "ds"})

        if not expense_df.empty:
            expense_df["expense_date"] = pd.to_datetime(expense_df["expense_date"], errors="coerce")
            expense_df["total_expenses"] = pd.to_numeric(expense_df["total_expenses"], errors="coerce")
            expense_df = expense_df.dropna(subset=["expense_date", "total_expenses"])
            expense_df = expense_df[["expense_date", "total_expenses"]]
            expense_df = expense_df.rename(columns={"expense_date": "ds"})
        else:
            expense_df = pd.DataFrame(columns=["ds", "total_expenses"])

        merged_df = pd.merge(
            sales_df,
            expense_df,
            on="ds",
            how="outer",
        )

        merged_df["total_sales"] = merged_df["total_sales"].fillna(0.0)
        merged_df["total_expenses"] = merged_df["total_expenses"].fillna(0.0)

        merged_df = merged_df.sort_values("ds").reset_index(drop=True)

        full_range = pd.date_range(
            start=merged_df["ds"].min(),
            end=merged_df["ds"].max(),
            freq="D",
        )

        merged_df = (
            merged_df.set_index("ds")
            .reindex(full_range)
            .rename_axis("ds")
            .reset_index()
        )

        merged_df["total_sales"] = merged_df["total_sales"].fillna(0.0)
        merged_df["total_expenses"] = merged_df["total_expenses"].fillna(0.0)

        merged_df["y"] = merged_df["total_sales"] - merged_df["total_expenses"]

        return merged_df[["ds", "y"]]

    @staticmethod
    def validate_minimum_history(
        df: pd.DataFrame,
        minimum_days: int = 30,
    ) -> None:
        if df.empty:
            raise ValueError("No historical profit data available for forecasting.")

        if len(df) < minimum_days:
            raise ValueError(
                f"Not enough historical profit data for forecasting. "
                f"At least {minimum_days} days are required, got {len(df)}."
            )