from __future__ import annotations

from typing import Any, Dict, List, Optional
from statistics import mean


from app.repositories.intelligence_repo import IntelligenceRepository
from app.services.intelligence.forecast_service import ForecastService


class IntelligenceService:
    LOOKBACK_DAYS = 90

    def __init__(self, repo: IntelligenceRepository):
        self.repo = repo

    @staticmethod
    def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
        return max(low, min(high, value))

    @staticmethod
    def _risk_level(score: float) -> str:
        if score >= 0.80:
            return "critical"
        if score >= 0.60:
            return "high"
        if score >= 0.35:
            return "medium"
        return "low"

    @staticmethod
    def _safe_div(a: float, b: float) -> float:
        return a / b if b else 0.0

    @staticmethod
    def _suggest_action(
        stockout_risk_score: float,
        dead_stock_risk_score: float,
        days_of_inventory: Optional[float],
    ) -> str:
        if stockout_risk_score >= 0.80:
            return "reorder immediately"
        if stockout_risk_score >= 0.60:
            return "reorder soon"
        if dead_stock_risk_score >= 0.75:
            return "consider transfer or promotion"
        if days_of_inventory is not None and days_of_inventory > 90:
            return "overstock watch"
        return "healthy"

    @staticmethod
    def _minmax_scale(items: List[float]) -> Dict[float, float]:
        if not items:
            return {}
        low = min(items)
        high = max(items)
        if high == low:
            return {x: 1.0 for x in items}
        return {x: (x - low) / (high - low) for x in items}

    async def build_store_intelligence(
        self,
        store_id: str,
        county: str = "Muranga",
        constituency: str = "Kiharu",
        ward: str = "Township",
    ) -> Dict[str, Any]:
        store_ctx = await self.repo.get_store_context(
            store_id=store_id,
            county=county,
            constituency=constituency,
            ward=ward,
        )
        print(county, constituency, ward)
        print(store_ctx)
        if not store_ctx:
            raise ValueError("Store not found for the given regional filters.")

        products = await self.repo.get_store_products_snapshot(store_id=store_id)
        if not products:
            raise ValueError("No store products found for this store.")

        store_rollup = await self.repo.get_store_product_sales_rollup(
            store_id=store_id,
            lookback_days=self.LOOKBACK_DAYS,
        )
        store_history = await self.repo.get_store_product_daily_sales_history(
            store_id=store_id,
            lookback_days=self.LOOKBACK_DAYS,
        )
        ward_rollup = await self.repo.get_ward_product_sales_rollup(
            ward=ward,
            county=county,
            constituency=constituency,
            lookback_days=self.LOOKBACK_DAYS,
        )
        supply_rollup = await self.repo.get_supply_rollup_by_store_product(
            store_id=store_id,
            lookback_days=self.LOOKBACK_DAYS,
        )

        items: List[Dict[str, Any]] = []

        for product in products:
            sp_id = product["store_product_id"]
            bp_id = product["business_product_id"]

            store_sales = store_rollup.get(sp_id, {})
            ward_sales = ward_rollup.get(bp_id, {})
            supply = supply_rollup.get(sp_id, {})

            quantity_on_hand = int(product.get("quantity_on_hand") or 0)
            sold_days_store = int(store_sales.get("sold_days") or 0)
            sold_days_ward = int(ward_sales.get("sold_days") or 0)

            total_units_store = float(store_sales.get("total_units_sold") or 0.0)
            total_units_ward = float(ward_sales.get("total_units_sold") or 0.0)

            store_daily_sale_rate = total_units_store / self.LOOKBACK_DAYS
            ward_daily_sale_rate = total_units_ward / self.LOOKBACK_DAYS

            # User asked: ward rate averaged by store rate
            blended_daily_sale_rate = (ward_daily_sale_rate + store_daily_sale_rate) / 2.0

            # Sale frequency = average of ward and store sale frequencies
            store_sale_frequency = sold_days_store / self.LOOKBACK_DAYS
            ward_sale_frequency = sold_days_ward / self.LOOKBACK_DAYS
            sale_frequency = (store_sale_frequency + ward_sale_frequency) / 2.0

            forecast = ForecastService.forecast_daily_units(
                store_history.get(sp_id, [])
            )

            # Days of inventory:
            # average of ward/store sale rate, then blend with prophet daily trend
            effective_daily_demand = mean([
                ward_daily_sale_rate,
                store_daily_sale_rate,
                forecast.daily_forecast,
            ])
            days_of_inventory = (
                round(quantity_on_hand / effective_daily_demand, 2)
                if effective_daily_demand > 0
                else None
            )

            # Stockout risk:
            # Compare blended demand pressure against available stock.
            # More risk if next 14 days expected demand exceeds current stock.
            expected_14d_demand = blended_daily_sale_rate * 14.0
            stockout_pressure = self._safe_div(expected_14d_demand, quantity_on_hand + 1)
            min_stock_penalty = 0.15 if (
                product.get("min_stock_level") is not None and
                quantity_on_hand <= int(product["min_stock_level"])
            ) else 0.0
            trend_penalty = 0.10 if forecast.trend == "up" else 0.0

            stockout_risk_score = self._clamp(
                stockout_pressure + min_stock_penalty + trend_penalty
            )

            # Dead stock risk:
            # Same family of methodology, but inverse logic:
            # high stock, low movement, low sale frequency, flat/down trend.
            expected_30d_demand = blended_daily_sale_rate * 30.0
            overstock_pressure = self._safe_div(quantity_on_hand, expected_30d_demand + 1)
            inactivity_penalty = 0.20 if sale_frequency < 0.10 else 0.0
            downtrend_penalty = 0.10 if forecast.trend == "down" else 0.0

            dead_stock_risk_score = self._clamp(
                ((overstock_pressure / 6.0) + inactivity_penalty + downtrend_penalty)
            )

            last_sale_at = store_sales.get("last_sale_at")
            days_since_last_sale = None

            if last_sale_at:
                from datetime import datetime, timezone
                if isinstance(last_sale_at, str):
                    try:
                        last_sale_dt = datetime.fromisoformat(last_sale_at.replace("Z", "+00:00"))
                    except Exception:
                        last_sale_dt = None
                else:
                    last_sale_dt = last_sale_at

                if last_sale_dt:
                    now = datetime.now(last_sale_dt.tzinfo) if last_sale_dt.tzinfo else datetime.utcnow()
                    days_since_last_sale = (now.date() - last_sale_dt.date()).days

            item = {
                "store_product_id": sp_id,
                "business_product_id": bp_id,
                "product_catalogue_id": product.get("product_catalogue_id"),
                "barcode": product.get("barcode"),
                "sku": product.get("sku"),
                "name": product.get("product_name"),
                "brand": product.get("brand"),
                "unit": product.get("unit"),
                "category_name": product.get("category_name"),
                "selling_price": float(product.get("selling_price") or 0.0),
                "quantity_on_hand": quantity_on_hand,
                "min_stock_level": product.get("min_stock_level"),
                "instock": True,
                "ward_daily_sale_rate": round(ward_daily_sale_rate, 4),
                "store_daily_sale_rate": round(store_daily_sale_rate, 4),
                "blended_daily_sale_rate": round(blended_daily_sale_rate, 4),
                "ward_sale_frequency": round(ward_sale_frequency, 4),
                "store_sale_frequency": round(store_sale_frequency, 4),
                "sale_frequency": round(sale_frequency, 4),
                "prophet_daily_forecast": round(forecast.daily_forecast, 4),
                "forecast_next_7_days_units": round(forecast.next_7_days_units, 4),
                "forecast_next_30_days_units": round(forecast.next_30_days_units, 4),
                "forecast_trend": forecast.trend,
                "days_of_inventory": days_of_inventory,
                "stockout_risk_score": round(stockout_risk_score, 4),
                "stockout_risk_level": self._risk_level(stockout_risk_score),
                "dead_stock_risk_score": round(dead_stock_risk_score, 4),
                "dead_stock_risk_level": self._risk_level(dead_stock_risk_score),
                "last_sale_at": last_sale_at.isoformat() if hasattr(last_sale_at, "isoformat") else last_sale_at,
                "days_since_last_sale": days_since_last_sale,
                "suggested_action": self._suggest_action(
                    stockout_risk_score=stockout_risk_score,
                    dead_stock_risk_score=dead_stock_risk_score,
                    days_of_inventory=days_of_inventory,
                ),
            }
            items.append(item)

        store_rank = await self._build_store_rank(
            target_store_id=store_id,
            ward=ward,
            county=county,
            constituency=constituency,
            product_items=items,
        )

        return {
            "filters": {
                "store_id": store_id,
                "county": county,
                "constituency": constituency,
                "ward": ward,
                "lookback_days": self.LOOKBACK_DAYS,
            },
            "store_rank": store_rank,
            "total_products": len(items),
            "items": items,
        }

    async def _build_store_rank(
        self,
        target_store_id: str,
        ward: str,
        county: str,
        constituency: str,
        product_items: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        rank_rows = await self.repo.get_store_rank_inputs(
            ward=ward,
            county=county,
            constituency=constituency,
            lookback_days=self.LOOKBACK_DAYS,
        )

        if not rank_rows:
            raise ValueError("No ward ranking data found.")

        target_stockout_avg = mean([x["stockout_risk_score"] for x in product_items]) if product_items else 0.0
        target_doi_avg = mean([x["days_of_inventory"] for x in product_items if x["days_of_inventory"] is not None]) if product_items else 0.0

        sale_volume_vals = [float(x["sale_volume"] or 0.0) for x in rank_rows]
        revenue_vals = [float(x["revenue_gain"] or 0.0) for x in rank_rows]
        supply_vals = [float(x["supplied_units"] or 0.0) for x in rank_rows]

        sale_scale = self._minmax_scale(sale_volume_vals)
        revenue_scale = self._minmax_scale(revenue_vals)
        supply_scale = self._minmax_scale(supply_vals)

        scored_rows = []
        for row in rank_rows:
            sale_volume = float(row["sale_volume"] or 0.0)
            revenue_gain = float(row["revenue_gain"] or 0.0)
            supplied_units = float(row["supplied_units"] or 0.0)

            if row["store_id"] == target_store_id:
                stockout_avg = target_stockout_avg
                doi_avg = target_doi_avg
            else:
                # For peer stores, approximate from stock and activity
                stockout_avg = 0.5 if float(row["avg_quantity_on_hand"] or 0.0) <= 0 else 0.3
                doi_avg = float(row["avg_quantity_on_hand"] or 0.0) / max((sale_volume / self.LOOKBACK_DAYS), 1.0)

            stockout_component = 1.0 - self._clamp(stockout_avg)
            doi_component = 1.0
            if doi_avg and doi_avg > 0:
                if 7 <= doi_avg <= 45:
                    doi_component = 1.0
                elif doi_avg < 7:
                    doi_component = 0.6
                elif doi_avg <= 90:
                    doi_component = 0.75
                else:
                    doi_component = 0.4

            composite_score = (
                sale_scale.get(sale_volume, 0.0) * 0.28 +
                revenue_scale.get(revenue_gain, 0.0) * 0.27 +
                supply_scale.get(supplied_units, 0.0) * 0.15 +
                stockout_component * 0.15 +
                doi_component * 0.15
            )

            scored_rows.append({
                **row,
                "stockout_risk_average": round(stockout_avg, 4),
                "days_of_inventory_average": round(doi_avg, 4),
                "sale_volume_average": round(sale_volume / self.LOOKBACK_DAYS, 4),
                "revenue_gain_average": round(revenue_gain / self.LOOKBACK_DAYS, 4),
                "supply_average": round(supplied_units / self.LOOKBACK_DAYS, 4),
                "composite_score": round(composite_score, 6),
            })

        scored_rows.sort(key=lambda x: x["composite_score"], reverse=True)

        for idx, row in enumerate(scored_rows, start=1):
            row["ward_rank"] = idx

        target = next((x for x in scored_rows if x["store_id"] == target_store_id), None)
        if not target:
            raise ValueError("Target store is missing from ward ranking.")

        return {
            "store_id": target["store_id"],
            "store_name": target["store_name"],
            "ward": target["ward"],
            "county": target["county"],
            "constituency": target["constituency"],
            "ward_rank": target["ward_rank"],
            "total_stores_in_ward": len(scored_rows),
            "composite_score": target["composite_score"],
            "sale_volume_average": target["sale_volume_average"],
            "revenue_gain_average": target["revenue_gain_average"],
            "supply_average": target["supply_average"],
            "stockout_risk_average": target["stockout_risk_average"],
            "days_of_inventory_average": target["days_of_inventory_average"],
        }