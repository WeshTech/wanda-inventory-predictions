from typing import Any, Dict, List, Optional

from app.db import get_db_pool


class IntelligenceRepository:
    def __init__(self):
        self.pool = get_db_pool()

    async def get_store_context(
        self,
        store_id: str,
        county: str,
        constituency: str,
        ward: str,
    ) -> Optional[Dict[str, Any]]:
        query = """
            SELECT
                s.id AS store_id,
                s.name AS store_name,
                s.county,
                s.constituency,
                s.ward,
                s."businessId" AS business_id,
                b.business::text AS business_type
            FROM "Store" s
            INNER JOIN "Business" b
                ON b.id = s."businessId"
            WHERE s.id = $1
              AND s.county = $2
              AND s.constituency = $3
              AND s.ward = $4
            LIMIT 1
        """
        row = await self.pool.fetchrow(query, store_id, county, constituency, ward)
        return dict(row) if row else None

    async def get_store_products_snapshot(self, store_id: str) -> List[Dict[str, Any]]:
        query = """
            SELECT
                sp.id AS store_product_id,
                sp."businessProductId" AS business_product_id,
                bp."productId" AS product_catalogue_id,
                COALESCE(bp.barcode, pc.barcode) AS barcode,
                pc.sku AS sku,
                COALESCE(bp.name, pc.name) AS product_name,
                COALESCE(bp.brand, pc.brand) AS brand,
                COALESCE(bp.unit, pc.unit) AS unit,
                sc.name AS category_name,
                sp."sellingPrice" AS selling_price,
                sp.quantity AS quantity_on_hand,
                sp."minStockLevel" AS min_stock_level
            FROM "StoreProduct" sp
            INNER JOIN "BusinessProduct" bp
                ON bp.id = sp."businessProductId"
            LEFT JOIN "ProductCatalogue" pc
                ON pc.id = bp."productId"
            INNER JOIN "StoreCategory" sc
                ON sc.id = sp."categoryId"
            WHERE sp."storeId" = $1
            ORDER BY COALESCE(bp.name, pc.name) ASC
        """
        rows = await self.pool.fetch(query, store_id)
        return [dict(row) for row in rows]

    async def get_store_product_sales_rollup(
        self,
        store_id: str,
        lookback_days: int = 90,
    ) -> Dict[str, Dict[str, Any]]:
        query = """
            WITH sales_base AS (
                SELECT
                    sl."storeProductId" AS store_product_id,
                    s."createdAt"::date AS sale_date,
                    MAX(s."createdAt") AS last_sale_at,
                    SUM(sl.quantity) AS total_units_sold,
                    SUM(sl.quantity * sl.price) AS total_revenue
                FROM "SaleLine" sl
                INNER JOIN "Sale" s
                    ON s.id = sl."saleId"
                WHERE s."storeId" = $1
                  AND s."createdAt" >= NOW() - ($2 || ' days')::interval
                GROUP BY sl."storeProductId", s."createdAt"::date
            )
            SELECT
                store_product_id,
                SUM(total_units_sold) AS total_units_sold,
                SUM(total_revenue) AS total_revenue,
                MAX(last_sale_at) AS last_sale_at,
                COUNT(*) AS sold_days
            FROM sales_base
            GROUP BY store_product_id
        """
        rows = await self.pool.fetch(query, store_id, str(lookback_days))
        return {row["store_product_id"]: dict(row) for row in rows}

    async def get_store_product_daily_sales_history(
        self,
        store_id: str,
        lookback_days: int = 90,
    ) -> Dict[str, List[Dict[str, Any]]]:
        query = """
            SELECT
                sl."storeProductId" AS store_product_id,
                s."createdAt"::date AS sale_date,
                SUM(sl.quantity) AS units
            FROM "SaleLine" sl
            INNER JOIN "Sale" s
                ON s.id = sl."saleId"
            WHERE s."storeId" = $1
              AND s."createdAt" >= NOW() - ($2 || ' days')::interval
            GROUP BY sl."storeProductId", s."createdAt"::date
            ORDER BY sl."storeProductId", s."createdAt"::date
        """
        rows = await self.pool.fetch(query, store_id, str(lookback_days))

        history: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            history.setdefault(row["store_product_id"], []).append({
                "date": str(row["sale_date"]),
                "units": float(row["units"] or 0),
            })
        return history

    async def get_ward_product_sales_rollup(
        self,
        ward: str,
        county: str,
        constituency: str,
        lookback_days: int = 90,
    ) -> Dict[str, Dict[str, Any]]:
        query = """
            WITH ward_sales AS (
                SELECT
                    sp."businessProductId" AS business_product_id,
                    s."createdAt"::date AS sale_date,
                    SUM(sl.quantity) AS units_sold
                FROM "SaleLine" sl
                INNER JOIN "Sale" s
                    ON s.id = sl."saleId"
                INNER JOIN "StoreProduct" sp
                    ON sp.id = sl."storeProductId"
                INNER JOIN "Store" st
                    ON st.id = s."storeId"
                WHERE st.ward = $1
                  AND st.county = $2
                  AND st.constituency = $3
                  AND s."createdAt" >= NOW() - ($4 || ' days')::interval
                GROUP BY sp."businessProductId", s."createdAt"::date
            )
            SELECT
                business_product_id,
                SUM(units_sold) AS total_units_sold,
                COUNT(*) AS sold_days
            FROM ward_sales
            GROUP BY business_product_id
        """
        rows = await self.pool.fetch(query, ward, county, constituency, str(lookback_days))
        return {row["business_product_id"]: dict(row) for row in rows}

    async def get_supply_rollup_by_store_product(
        self,
        store_id: str,
        lookback_days: int = 90,
    ) -> Dict[str, Dict[str, Any]]:
        query = """
            WITH receipt_supply AS (
                SELECT
                    sp.id AS store_product_id,
                    SUM(prl."acceptedQuantity") AS supplied_units
                FROM "PurchaseReceiptLine" prl
                INNER JOIN "PurchaseReceipt" pr
                    ON pr.id = prl."purchaseReceiptId"
                INNER JOIN "StoreProduct" sp
                    ON sp."businessProductId" = prl."businessProductId"
                   AND sp."storeId" = pr."storeId"
                WHERE pr."storeId" = $1
                  AND pr."createdAt" >= NOW() - ($2 || ' days')::interval
                GROUP BY sp.id
            ),
            transfer_in AS (
                SELECT
                    tl."storeProductId" AS store_product_id,
                    SUM(tl.quantity) AS transfer_in_units
                FROM "TransferLine" tl
                INNER JOIN "Transfer" t
                    ON t.id = tl."transferId"
                WHERE t."toStoreId" = $1
                  AND t."createdAt" >= NOW() - ($2 || ' days')::interval
                GROUP BY tl."storeProductId"
            )
            SELECT
                sp.id AS store_product_id,
                COALESCE(rs.supplied_units, 0) AS receipt_units,
                COALESCE(ti.transfer_in_units, 0) AS transfer_in_units,
                COALESCE(rs.supplied_units, 0) + COALESCE(ti.transfer_in_units, 0) AS total_supply_units
            FROM "StoreProduct" sp
            LEFT JOIN receipt_supply rs
                ON rs.store_product_id = sp.id
            LEFT JOIN transfer_in ti
                ON ti.store_product_id = sp.id
            WHERE sp."storeId" = $1
        """
        rows = await self.pool.fetch(query, store_id, str(lookback_days))
        return {row["store_product_id"]: dict(row) for row in rows}

    async def get_store_rank_inputs(
        self,
        ward: str,
        county: str,
        constituency: str,
        lookback_days: int = 90,
    ) -> List[Dict[str, Any]]:
        query = """
            WITH sales_agg AS (
                SELECT
                    s."storeId" AS store_id,
                    SUM(sl.quantity) AS sale_volume,
                    SUM(sl.quantity * sl.price) AS revenue_gain,
                    COUNT(DISTINCT s."createdAt"::date) AS active_sale_days
                FROM "Sale" s
                INNER JOIN "SaleLine" sl
                    ON sl."saleId" = s.id
                INNER JOIN "Store" st
                    ON st.id = s."storeId"
                WHERE st.ward = $1
                  AND st.county = $2
                  AND st.constituency = $3
                  AND s."createdAt" >= NOW() - ($4 || ' days')::interval
                GROUP BY s."storeId"
            ),
            supply_agg AS (
                SELECT
                    pr."storeId" AS store_id,
                    SUM(prl."acceptedQuantity") AS supplied_units
                FROM "PurchaseReceipt" pr
                INNER JOIN "PurchaseReceiptLine" prl
                    ON prl."purchaseReceiptId" = pr.id
                INNER JOIN "Store" st
                    ON st.id = pr."storeId"
                WHERE st.ward = $1
                  AND st.county = $2
                  AND st.constituency = $3
                  AND pr."createdAt" >= NOW() - ($4 || ' days')::interval
                GROUP BY pr."storeId"
            ),
            stock_agg AS (
                SELECT
                    sp."storeId" AS store_id,
                    AVG(sp.quantity) AS avg_quantity_on_hand
                FROM "StoreProduct" sp
                GROUP BY sp."storeId"
            )
            SELECT
                st.id AS store_id,
                st.name AS store_name,
                st.ward,
                st.county,
                st.constituency,
                COALESCE(sa.sale_volume, 0) AS sale_volume,
                COALESCE(sa.revenue_gain, 0) AS revenue_gain,
                COALESCE(sa.active_sale_days, 0) AS active_sale_days,
                COALESCE(su.supplied_units, 0) AS supplied_units,
                COALESCE(sk.avg_quantity_on_hand, 0) AS avg_quantity_on_hand
            FROM "Store" st
            LEFT JOIN sales_agg sa ON sa.store_id = st.id
            LEFT JOIN supply_agg su ON su.store_id = st.id
            LEFT JOIN stock_agg sk ON sk.store_id = st.id
            WHERE st.ward = $1
              AND st.county = $2
              AND st.constituency = $3
            ORDER BY st.name ASC
        """
        rows = await self.pool.fetch(query, ward, county, constituency, str(lookback_days))
        return [dict(row) for row in rows]