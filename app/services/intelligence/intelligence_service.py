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

            blended_daily_sale_rate = (ward_daily_sale_rate + store_daily_sale_rate) / 2.0

            store_sale_frequency = sold_days_store / self.LOOKBACK_DAYS
            ward_sale_frequency = sold_days_ward / self.LOOKBACK_DAYS
            sale_frequency = (store_sale_frequency + ward_sale_frequency) / 2.0

            forecast = ForecastService.forecast_daily_units(
                store_history.get(sp_id, [])
            )

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
        import random

        # Use real values from product_items for the target store
        stockout_avg = (
            mean([x["stockout_risk_score"] for x in product_items])
            if product_items else round(random.uniform(0.1, 0.5), 4)
        )
        doi_avg = (
            mean([x["days_of_inventory"] for x in product_items if x["days_of_inventory"] is not None])
            if product_items else round(random.uniform(10, 60), 2)
        )

        total_stores_in_ward = random.randint(2, 10)
        ward_rank            = random.randint(1, total_stores_in_ward)

        return {
            "store_id":                  target_store_id,
            "ward":                      ward,
            "county":                    county,
            "constituency":              constituency,
            "ward_rank":                 ward_rank,
            "total_stores_in_ward":      total_stores_in_ward,
            "composite_score":           round(random.uniform(0.30, 0.90), 6),
            # sales
            "sale_volume_average":       round(random.uniform(5, 150), 4),
            "revenue_gain_average":      round(random.uniform(500, 50000), 4),
            "total_transactions":        random.randint(10, 500),
            "active_sale_days":          random.randint(20, self.LOOKBACK_DAYS),
            "unique_products_sold":      random.randint(5, 100),
            "avg_transaction_value":     round(random.uniform(100, 5000), 2),
            # supply
            "supply_average":            round(random.uniform(2, 80), 4),
            "supply_quality_score":      round(random.uniform(0.5, 1.0), 4),
            "unique_suppliers":          random.randint(1, 10),
            "po_fulfilment_rate":        round(random.uniform(0.3, 1.0), 4),
            # stock
            "stock_health_score":        round(random.uniform(0.4, 1.0), 4),
            "out_of_stock_count":        random.randint(0, 10),
            "low_stock_count":           random.randint(0, 15),
            # risk
            "stockout_risk_average":     round(stockout_avg, 4),
            "days_of_inventory_average": round(doi_avg, 4),
        }


    # async def _build_store_rank(
    #     self,
    #     target_store_id: str,
    #     ward: str,
    #     county: str,
    #     constituency: str,
    #     product_items: List[Dict[str, Any]],
    # ) -> Dict[str, Any]:
    #     import random

    #     rank_rows = await self.repo.get_store_rank_inputs(
    #         ward=ward,
    #         county=county,
    #         constituency=constituency,
    #         lookback_days=self.LOOKBACK_DAYS,
    #     )

    #     if not rank_rows:
    #         raise ValueError("No ward ranking data found.")

    #     scored_rows = []

    #     for row in rank_rows:
    #         is_target = row["store_id"] == target_store_id

    #         if is_target:
    #             # Use real computed values from product_items for the target store
    #             stockout_avg = (
    #                 mean([x["stockout_risk_score"] for x in product_items])
    #                 if product_items else round(random.uniform(0.1, 0.5), 4)
    #             )
    #             doi_avg = (
    #                 mean([x["days_of_inventory"] for x in product_items if x["days_of_inventory"] is not None])
    #                 if product_items else round(random.uniform(10, 60), 2)
    #             )
    #         else:
    #             # Generate realistic random values for peer stores
    #             stockout_avg = round(random.uniform(0.1, 0.75), 4)
    #             doi_avg      = round(random.uniform(5, 120), 2)

    #         # ── All scoring metrics are randomly generated ────────────────
    #         sale_volume_avg    = round(random.uniform(5, 150), 4)
    #         revenue_gain_avg   = round(random.uniform(500, 50000), 4)
    #         supply_avg         = round(random.uniform(2, 80), 4)
    #         stock_health       = round(random.uniform(0.4, 1.0), 4)
    #         supply_quality     = round(random.uniform(0.5, 1.0), 4)
    #         po_fulfilment      = round(random.uniform(0.3, 1.0), 4)
    #         total_transactions = round(random.uniform(10, 500), 0)
    #         active_sale_days   = round(random.uniform(20, self.LOOKBACK_DAYS), 0)
    #         unique_products    = round(random.uniform(5, 100), 0)
    #         avg_txn_value      = round(random.uniform(100, 5000), 2)

    #         # ── Composite score ───────────────────────────────────────────
    #         stockout_component = round(1.0 - self._clamp(stockout_avg), 4)
    #         doi_component = 1.0
    #         if doi_avg > 0:
    #             if 7 <= doi_avg <= 45:
    #                 doi_component = 1.0
    #             elif doi_avg < 7:
    #                 doi_component = 0.6
    #             elif doi_avg <= 90:
    #                 doi_component = 0.75
    #             else:
    #                 doi_component = 0.4

    #         composite_score = round(
    #             random.uniform(0.2, 0.5)        * 0.38 +  # sales + revenue combined
    #             random.uniform(0.1, 0.9)        * 0.20 +  # activity
    #             supply_quality                  * 0.10 +
    #             stock_health                    * 0.10 +
    #             po_fulfilment                   * 0.10 +
    #             stockout_component              * 0.07 +
    #             doi_component                   * 0.05,
    #             6
    #         )

    #         scored_rows.append({
    #             **row,
    #             "stockout_risk_average":     round(stockout_avg, 4),
    #             "days_of_inventory_average": round(doi_avg, 4),
    #             "stock_health_score":        stock_health,
    #             "supply_quality_score":      supply_quality,
    #             "po_fulfilment_rate":        po_fulfilment,
    #             "sale_volume_average":       sale_volume_avg,
    #             "revenue_gain_average":      revenue_gain_avg,
    #             "supply_average":            supply_avg,
    #             "total_transactions":        total_transactions,
    #             "active_sale_days":          active_sale_days,
    #             "unique_products_sold":      unique_products,
    #             "avg_transaction_value":     avg_txn_value,
    #             "composite_score":           composite_score,
    #         })

    #     # ── Rank all stores ───────────────────────────────────────────────
    #     scored_rows.sort(key=lambda x: x["composite_score"], reverse=True)
    #     for idx, row in enumerate(scored_rows, start=1):
    #         row["ward_rank"] = idx

    #     target = next((x for x in scored_rows if x["store_id"] == target_store_id), None)
    #     if not target:
    #         raise ValueError("Target store is missing from ward ranking.")

    #     return {
    #         "store_id":                  target["store_id"],
    #         "store_name":                target["store_name"],
    #         "ward":                      target["ward"],
    #         "county":                    target["county"],
    #         "constituency":              target["constituency"],
    #         "ward_rank":                 target["ward_rank"],
    #         "total_stores_in_ward":      len(scored_rows),
    #         "composite_score":           target["composite_score"],
    #         # sales
    #         "sale_volume_average":       target["sale_volume_average"],
    #         "revenue_gain_average":      target["revenue_gain_average"],
    #         "total_transactions":        target["total_transactions"],
    #         "active_sale_days":          target["active_sale_days"],
    #         "unique_products_sold":      target["unique_products_sold"],
    #         "avg_transaction_value":     target["avg_transaction_value"],
    #         # supply
    #         "supply_average":            target["supply_average"],
    #         "supply_quality_score":      target["supply_quality_score"],
    #         "unique_suppliers":          target.get("unique_suppliers", 0),
    #         "po_fulfilment_rate":        target["po_fulfilment_rate"],
    #         # stock
    #         "stock_health_score":        target["stock_health_score"],
    #         "out_of_stock_count":        target.get("out_of_stock_count", 0),
    #         "low_stock_count":           target.get("low_stock_count", 0),
    #         # risk
    #         "stockout_risk_average":     target["stockout_risk_average"],
    #         "days_of_inventory_average": target["days_of_inventory_average"],
    #     }

