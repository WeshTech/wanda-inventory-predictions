from typing import Any

from app.db import get_db_pool


class SalesRepository:
    async def get_daily_sales_by_store(
        self,
        store_id: str,
        days: int = 180,
    ) -> list[dict[str, Any]]:
        """
        Returns daily aggregated sales for a given store.
        Used for store-level forecasting.
        """
        pool = get_db_pool()

        query = """
            SELECT
                DATE(s."createdAt") AS sale_date,
                COALESCE(SUM(s.subtotal), 0) AS total_sales,
                COUNT(s.id) AS total_transactions
            FROM "Sale" s
            WHERE s."storeId" = $1
              AND s."createdAt" >= NOW() - ($2 || ' days')::interval
            GROUP BY DATE(s."createdAt")
            ORDER BY sale_date ASC;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, store_id, days)

        return [dict(row) for row in rows]

    async def get_daily_sales_by_business(
        self,
        business_id: str,
        days: int = 180,
    ) -> list[dict[str, Any]]:
        """
        Returns daily aggregated sales for a business across all stores.
        Useful for profit forecasting and whole-business trends.
        """
        pool = get_db_pool()

        query = """
            SELECT
                DATE(s."createdAt") AS sale_date,
                COALESCE(SUM(s.subtotal), 0) AS total_sales,
                COUNT(s.id) AS total_transactions
            FROM "Sale" s
            WHERE s."businessId" = $1
              AND s."createdAt" >= NOW() - ($2 || ' days')::interval
            GROUP BY DATE(s."createdAt")
            ORDER BY sale_date ASC;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, business_id, days)

        return [dict(row) for row in rows]

    async def get_daily_product_sales_for_store(
        self,
        store_id: str,
        business_product_id: str,
        days: int = 180,
    ) -> list[dict[str, Any]]:
        """
        Returns daily sales for a specific product in a specific store.
        This is based on SaleLine joined to StoreProduct.
        """
        pool = get_db_pool()

        query = """
            SELECT
                DATE(s."createdAt") AS sale_date,
                COALESCE(SUM(sl.quantity), 0) AS total_quantity,
                COALESCE(SUM(sl.quantity * sl.price), 0) AS total_sales
            FROM "SaleLine" sl
            INNER JOIN "Sale" s
                ON s.id = sl."saleId"
            INNER JOIN "StoreProduct" sp
                ON sp.id = sl."storeProductId"
            WHERE s."storeId" = $1
              AND sp."businessProductId" = $2
              AND s."createdAt" >= NOW() - ($3 || ' days')::interval
            GROUP BY DATE(s."createdAt")
            ORDER BY sale_date ASC;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, store_id, business_product_id, days)

        return [dict(row) for row in rows]

    async def get_top_selling_products_by_store(
        self,
        store_id: str,
        days: int = 30,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Returns top-selling products in a store by quantity and revenue.
        Useful for fast-moving goods and recommendation engines.
        """
        pool = get_db_pool()

        query = """
            SELECT
                sp."businessProductId" AS business_product_id,
                COALESCE(bp.name, pc.name, 'Unknown Product') AS product_name,
                COALESCE(bp.brand, pc.brand, 'Unknown Brand') AS brand,
                COALESCE(SUM(sl.quantity), 0) AS total_quantity,
                COALESCE(SUM(sl.quantity * sl.price), 0) AS total_revenue
            FROM "SaleLine" sl
            INNER JOIN "Sale" s
                ON s.id = sl."saleId"
            INNER JOIN "StoreProduct" sp
                ON sp.id = sl."storeProductId"
            INNER JOIN "BusinessProduct" bp
                ON bp.id = sp."businessProductId"
            LEFT JOIN "ProductCatalogue" pc
                ON pc.id = bp."productId"
            WHERE s."storeId" = $1
              AND s."createdAt" >= NOW() - ($2 || ' days')::interval
            GROUP BY sp."businessProductId", bp.name, pc.name, bp.brand, pc.brand
            ORDER BY total_quantity DESC, total_revenue DESC
            LIMIT $3;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, store_id, days, limit)

        return [dict(row) for row in rows]

    async def get_weekend_hot_sales_by_store(
        self,
        store_id: str,
        days: int = 90,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Returns products that perform best on weekends in a specific store.
        PostgreSQL EXTRACT(DOW): Sunday=0, Saturday=6
        """
        pool = get_db_pool()

        query = """
            SELECT
                sp."businessProductId" AS business_product_id,
                COALESCE(bp.name, pc.name, 'Unknown Product') AS product_name,
                COALESCE(SUM(sl.quantity), 0) AS total_quantity,
                COALESCE(SUM(sl.quantity * sl.price), 0) AS total_revenue
            FROM "SaleLine" sl
            INNER JOIN "Sale" s
                ON s.id = sl."saleId"
            INNER JOIN "StoreProduct" sp
                ON sp.id = sl."storeProductId"
            INNER JOIN "BusinessProduct" bp
                ON bp.id = sp."businessProductId"
            LEFT JOIN "ProductCatalogue" pc
                ON pc.id = bp."productId"
            WHERE s."storeId" = $1
              AND s."createdAt" >= NOW() - ($2 || ' days')::interval
              AND EXTRACT(DOW FROM s."createdAt") IN (0, 6)
            GROUP BY sp."businessProductId", bp.name, pc.name
            ORDER BY total_quantity DESC, total_revenue DESC
            LIMIT $3;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, store_id, days, limit)

        return [dict(row) for row in rows]

    async def get_top_selling_products_by_region(
        self,
        *,
        county: str | None = None,
        constituency: str | None = None,
        ward: str | None = None,
        business_type: str | None = None,
        days: int = 30,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Region-based aggregation for recommendations.
        This uses Store regional fields and Business.business type.
        """
        pool = get_db_pool()

        query = """
            SELECT
                sp."businessProductId" AS business_product_id,
                COALESCE(bp.name, pc.name, 'Unknown Product') AS product_name,
                COALESCE(bp.brand, pc.brand, 'Unknown Brand') AS brand,
                st.county,
                st.constituency,
                st.ward,
                b.business::text AS business_type,
                COALESCE(SUM(sl.quantity), 0) AS total_quantity,
                COALESCE(SUM(sl.quantity * sl.price), 0) AS total_revenue
            FROM "SaleLine" sl
            INNER JOIN "Sale" s
                ON s.id = sl."saleId"
            INNER JOIN "Store" st
                ON st.id = s."storeId"
            INNER JOIN "Business" b
                ON b.id = s."businessId"
            INNER JOIN "StoreProduct" sp
                ON sp.id = sl."storeProductId"
            INNER JOIN "BusinessProduct" bp
                ON bp.id = sp."businessProductId"
            LEFT JOIN "ProductCatalogue" pc
                ON pc.id = bp."productId"
            WHERE s."createdAt" >= NOW() - ($1 || ' days')::interval
              AND ($2::text IS NULL OR st.county = $2)
              AND ($3::text IS NULL OR st.constituency = $3)
              AND ($4::text IS NULL OR st.ward = $4)
              AND ($5::text IS NULL OR b.business::text = $5)
            GROUP BY
                sp."businessProductId",
                bp.name,
                pc.name,
                bp.brand,
                pc.brand,
                st.county,
                st.constituency,
                st.ward,
                b.business
            ORDER BY total_quantity DESC, total_revenue DESC
            LIMIT $6;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                query,
                days,
                county,
                constituency,
                ward,
                business_type,
                limit,
            )

        return [dict(row) for row in rows]