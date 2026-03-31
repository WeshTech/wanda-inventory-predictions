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
            AND s."createdAt" >= NOW() - ($2::int * INTERVAL '1 day')
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
        pool = get_db_pool()

        query = """
            SELECT
                DATE(s."createdAt")             AS sale_date,
                COALESCE(SUM(s.subtotal), 0)    AS total_sales,
                COUNT(s.id)                     AS total_transactions
            FROM "Sale" s
            WHERE s."businessId" = $1
            AND s."createdAt" >= NOW() - ($2::int * INTERVAL '1 day')
            AND s."paymentStatus" NOT IN ('CANCELLED', 'REFUNDED')
            GROUP BY DATE(s."createdAt")
            ORDER BY sale_date ASC;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, business_id, days)

        return [dict(row) for row in rows]
    

    async def get_daily_product_sales_for_store(
        self,
        store_id: str,
        store_product_id: str,
        days: int = 180,
    ) -> list[dict[str, Any]]:
        """
        Returns daily sales for a specific product in a specific store.
        Uses StoreProduct.id directly since SaleLine references storeProductId.
        """
        pool = get_db_pool()

        query = """
            SELECT
                DATE(s."createdAt") AS sale_date,
                COALESCE(SUM(sl.quantity), 0)              AS total_quantity,
                COALESCE(SUM(sl.quantity * sl.price), 0)   AS total_sales
            FROM "SaleLine" sl
            INNER JOIN "Sale" s
                ON s.id = sl."saleId"
            INNER JOIN "StoreProduct" sp
                ON sp.id = sl."storeProductId"
            WHERE s."storeId" = $1
            AND sl."storeProductId" = $2
            AND s."createdAt" >= NOW() - ($3::int * INTERVAL '1 day')
            AND s."paymentStatus" NOT IN ('CANCELLED', 'REFUNDED')
            GROUP BY DATE(s."createdAt")
            ORDER BY sale_date ASC;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, store_id, store_product_id, days)

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

   
    async def get_product_sales_by_month_for_store(
        self,
        store_id: str,
        days: int = 365,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """
        Returns monthly product performance for a store.
        Useful for seasonal analysis.
        """
        pool = get_db_pool()

        query = """
            SELECT
                sp."businessProductId" AS business_product_id,
                COALESCE(bp.name, pc.name, 'Unknown Product') AS product_name,
                COALESCE(bp.brand, pc.brand, 'Unknown Brand') AS brand,
                EXTRACT(MONTH FROM s."createdAt")::int AS sale_month,
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
              AND s."createdAt" >= NOW() - ($2 * INTERVAL '1 day')
            GROUP BY
                sp."businessProductId",
                bp.name,
                pc.name,
                bp.brand,
                pc.brand,
                EXTRACT(MONTH FROM s."createdAt")
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
        days: int = 3650,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Aggregates sales across ALL businesses in a region to find
        the fastest-moving products. Supports county, constituency,
        ward, and business_type filters independently or combined.

        Use cases:
        - What products are moving fastest in Muranga county?
        - What's trending for PHARMACY businesses in Westlands?
        - What are the hot products in Parklands ward?
        """
        pool = get_db_pool()

        filters = []
        params: list = [days]  # $1 = days

        # Region filters go on Business (indexed), not Store
        # Business has county/constituency/ward with indexes
        if county:
            params.append(county)
            filters.append(f'LOWER(b.county) = LOWER(${len(params)})')

        if constituency:
            params.append(constituency)
            filters.append(f'LOWER(b.constituency) = LOWER(${len(params)})')

        if ward:
            params.append(ward)
            filters.append(f'LOWER(b.ward) = LOWER(${len(params)})')

        if business_type:
            params.append(business_type.upper())
            filters.append(f'b.business::text = ${len(params)}')

        params.append(limit)
        limit_placeholder = f'${len(params)}'

        where_clause = (
            "AND " + "\n              AND ".join(filters)
            if filters else ""
        )

        query = f"""
            SELECT
                sp."businessProductId"                              AS business_product_id,
                COALESCE(bp.name, pc.name, 'Unknown Product')       AS product_name,
                COALESCE(bp.brand, pc.brand, 'Unknown Brand')       AS brand,
                bp.unit                                             AS unit,
                b.county,
                b.constituency,
                b.ward,
                b.business::text                                    AS business_type,
                COUNT(DISTINCT s."businessId")                      AS business_count,
                COALESCE(SUM(sl.quantity), 0)                       AS total_quantity,
                COALESCE(SUM(sl.quantity * sl.price), 0)            AS total_revenue,
                ROUND(
                    COALESCE(SUM(sl.quantity), 0)::numeric / 
                    NULLIF($1::int, 0), 2
                )                                                   AS avg_daily_quantity
            FROM "SaleLine" sl
            INNER JOIN "Sale" s       ON s.id = sl."saleId"
            INNER JOIN "Business" b   ON b.id = s."businessId"
            INNER JOIN "StoreProduct" sp ON sp.id = sl."storeProductId"
            INNER JOIN "BusinessProduct" bp ON bp.id = sp."businessProductId"
            LEFT JOIN "ProductCatalogue" pc ON pc.id = bp."productId"
            WHERE s."createdAt" >= NOW() - ($1::int * INTERVAL '1 day')
            AND s."paymentStatus" NOT IN ('CANCELLED', 'REFUNDED')
            {where_clause}
            GROUP BY
                sp."businessProductId",
                bp.name, pc.name,
                bp.brand, pc.brand,
                bp.unit,
                b.county, b.constituency, b.ward,
                b.business
            ORDER BY total_quantity DESC, total_revenue DESC
            LIMIT {limit_placeholder};
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            print(f"[RegionalQuery] {len(rows)} rows | filters: county={county}, "
                f"constituency={constituency}, ward={ward}, "
                f"business_type={business_type}, days={days}")

        return [dict(row) for row in rows]
    

    async def get_top_selling_products_by_store(
        self,
        store_id: str,
        days: int = 30,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        pool = get_db_pool()

        query = """
            SELECT
                sp."businessProductId"                          AS business_product_id,
                COALESCE(bp.name, pc.name, 'Unknown Product')   AS product_name,
                COALESCE(bp.brand, pc.brand, 'Unknown Brand')   AS brand,
                COALESCE(SUM(sl.quantity), 0)                   AS total_quantity,
                COALESCE(SUM(sl.quantity * sl.price), 0)        AS total_revenue
            FROM "SaleLine" sl
            INNER JOIN "Sale" s ON s.id = sl."saleId"
            INNER JOIN "StoreProduct" sp ON sp.id = sl."storeProductId"
            INNER JOIN "BusinessProduct" bp ON bp.id = sp."businessProductId"
            LEFT JOIN "ProductCatalogue" pc ON pc.id = bp."productId"
            WHERE s."storeId" = $1
              AND s."createdAt" >= NOW() - ($2::int * INTERVAL '1 day')
              AND s."paymentStatus" NOT IN ('CANCELLED', 'REFUNDED')
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
        pool = get_db_pool()

        query = """
            SELECT
                sp."businessProductId"                          AS business_product_id,
                COALESCE(bp.name, pc.name, 'Unknown Product')   AS product_name,
                COALESCE(SUM(sl.quantity), 0)                   AS total_quantity,
                COALESCE(SUM(sl.quantity * sl.price), 0)        AS total_revenue
            FROM "SaleLine" sl
            INNER JOIN "Sale" s ON s.id = sl."saleId"
            INNER JOIN "StoreProduct" sp ON sp.id = sl."storeProductId"
            INNER JOIN "BusinessProduct" bp ON bp.id = sp."businessProductId"
            LEFT JOIN "ProductCatalogue" pc ON pc.id = bp."productId"
            WHERE s."storeId" = $1
              AND s."createdAt" >= NOW() - ($2::int * INTERVAL '1 day')
              AND s."paymentStatus" NOT IN ('CANCELLED', 'REFUNDED')
              AND EXTRACT(DOW FROM s."createdAt") IN (0, 6)
            GROUP BY sp."businessProductId", bp.name, pc.name
            ORDER BY total_quantity DESC, total_revenue DESC
            LIMIT $3;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, store_id, days, limit)

        return [dict(row) for row in rows]

    async def get_product_sales_by_month_for_store(
        self,
        store_id: str,
        days: int = 365,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        pool = get_db_pool()

        query = """
            SELECT
                sp."businessProductId"                          AS business_product_id,
                COALESCE(bp.name, pc.name, 'Unknown Product')   AS product_name,
                COALESCE(bp.brand, pc.brand, 'Unknown Brand')   AS brand,
                EXTRACT(MONTH FROM s."createdAt")::int          AS sale_month,
                COALESCE(SUM(sl.quantity), 0)                   AS total_quantity,
                COALESCE(SUM(sl.quantity * sl.price), 0)        AS total_revenue
            FROM "SaleLine" sl
            INNER JOIN "Sale" s ON s.id = sl."saleId"
            INNER JOIN "StoreProduct" sp ON sp.id = sl."storeProductId"
            INNER JOIN "BusinessProduct" bp ON bp.id = sp."businessProductId"
            LEFT JOIN "ProductCatalogue" pc ON pc.id = bp."productId"
            WHERE s."storeId" = $1
              AND s."createdAt" >= NOW() - ($2::int * INTERVAL '1 day')
              AND s."paymentStatus" NOT IN ('CANCELLED', 'REFUNDED')
            GROUP BY
                sp."businessProductId",
                bp.name, pc.name,
                bp.brand, pc.brand,
                EXTRACT(MONTH FROM s."createdAt")
            ORDER BY total_quantity DESC, total_revenue DESC
            LIMIT $3;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, store_id, days, limit)

        return [dict(row) for row in rows]

    
    async def get_restock_candidates_by_business_type(
        self,
        business_type: str,
        county: str | None = None,
        constituency: str | None = None,
        ward: str | None = None,
        days: int = 30,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """
        Products with strong demand in a region for a given business type.
        This is used as a restock recommendation source.
        """
        pool = get_db_pool()

        filters = ['b.business::text = $1']
        params: list = [business_type.upper(), days]  # $1=business_type, $2=days

        if county:
            params.append(county)
            filters.append(f'LOWER(b.county) = LOWER(${len(params)})')

        if constituency:
            params.append(constituency)
            filters.append(f'LOWER(b.constituency) = LOWER(${len(params)})')

        if ward:
            params.append(ward)
            filters.append(f'LOWER(b.ward) = LOWER(${len(params)})')

        params.append(limit)
        limit_placeholder = f'${len(params)}'

        where_clause = "\n              AND ".join(filters)

        query = f"""
            SELECT
                sp."businessProductId"                              AS business_product_id,
                COALESCE(bp.name, pc.name, 'Unknown Product')       AS product_name,
                COALESCE(bp.brand, pc.brand, 'Unknown Brand')       AS brand,
                b.business::text                                    AS business_type,
                b.county,
                b.constituency,
                b.ward,
                COALESCE(SUM(sl.quantity), 0)                       AS total_quantity,
                COALESCE(SUM(sl.quantity * sl.price), 0)            AS total_revenue,
                COUNT(DISTINCT s.id)                                AS transaction_count
            FROM "SaleLine" sl
            INNER JOIN "Sale" s   ON s.id = sl."saleId"
            INNER JOIN "Business" b ON b.id = s."businessId"
            INNER JOIN "StoreProduct" sp ON sp.id = sl."storeProductId"
            INNER JOIN "BusinessProduct" bp ON bp.id = sp."businessProductId"
            LEFT JOIN "ProductCatalogue" pc ON pc.id = bp."productId"
            WHERE {where_clause}
              AND s."createdAt" >= NOW() - ($2::int * INTERVAL '1 day')
              AND s."paymentStatus" NOT IN ('CANCELLED', 'REFUNDED')
            GROUP BY
                sp."businessProductId",
                bp.name, pc.name,
                bp.brand, pc.brand,
                b.business,
                b.county, b.constituency, b.ward
            ORDER BY total_quantity DESC, total_revenue DESC, transaction_count DESC
            LIMIT {limit_placeholder};
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [dict(row) for row in rows]