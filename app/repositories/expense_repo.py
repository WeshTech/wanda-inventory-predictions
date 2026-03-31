from typing import Any

from app.db import get_db_pool


class ExpenseRepository:
    async def get_daily_expenses_by_business(
        self,
        business_id: str,
        days: int = 180,
    ) -> list[dict[str, Any]]:
        pool = get_db_pool()

        query = """
            SELECT
                DATE(e.date)                        AS expense_date,
                COALESCE(SUM(e.amount), 0)          AS total_expenses,
                COUNT(e.id)                         AS total_expense_entries
            FROM "Expense" e
            WHERE e."businessId" = $1
              AND e.date >= NOW() - ($2::int * INTERVAL '1 day')
            GROUP BY DATE(e.date)
            ORDER BY expense_date ASC;
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, business_id, days)

        return [dict(row) for row in rows]