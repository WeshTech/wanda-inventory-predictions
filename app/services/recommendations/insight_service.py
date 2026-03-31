from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any


class RecommendationInsightService:
    MONTH_TO_SEASON = {
        12: "Festive/Holiday",
        1: "Back-to-school / New Year",
        2: "Back-to-school / New Year",
        3: "Long-rain season prep",
        4: "Long-rain season prep",
        5: "Long-rain season prep",
        6: "Mid-year",
        7: "Mid-year",
        8: "Mid-year",
        9: "Short-rain season prep",
        10: "Short-rain season prep",
        11: "Short-rain season prep",
    }

    @staticmethod
    def build_fast_moving_goods_response(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        enriched = []

        for index, item in enumerate(items, start=1):
            enriched.append(
                {
                    **item,
                    "rank": index,
                    "insight": "Fast-moving product based on recent quantity sold and revenue.",
                }
            )

        return enriched

    @staticmethod
    def build_weekend_hot_sales_response(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        enriched = []

        for index, item in enumerate(items, start=1):
            enriched.append(
                {
                    **item,
                    "rank": index,
                    "insight": "Strong weekend performance based on Saturday/Sunday sales activity.",
                }
            )

        return enriched

    @classmethod
    def build_seasonal_products_response(cls, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        enriched = []

        for item in items:
            sale_month = item.get("sale_month")
            season = cls.MONTH_TO_SEASON.get(sale_month, "General")

            enriched.append(
                {
                    **item,
                    "season": season,
                    "insight": f"Product shows notable movement in month {sale_month}, mapped to {season}.",
                }
            )

        return enriched

    @staticmethod
    def build_restock_response(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        enriched = []

        for index, item in enumerate(items, start=1):
            enriched.append(
                {
                    **item,
                    "rank": index,
                    "restock_signal": "HIGH",
                    "insight": "Recommended for stocking due to strong area demand, quantity sold, and transaction frequency.",
                }
            )

        return enriched