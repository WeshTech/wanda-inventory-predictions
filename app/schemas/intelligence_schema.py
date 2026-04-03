from pydantic import BaseModel, Field
from typing import List, Optional


class ProductIntelligenceItem(BaseModel):
    store_product_id: str
    business_product_id: str
    product_catalogue_id: Optional[str] = None

    barcode: Optional[str] = None
    sku: Optional[str] = None
    name: str
    brand: Optional[str] = None
    unit: Optional[str] = None

    category_name: Optional[str] = None
    selling_price: float
    quantity_on_hand: int
    min_stock_level: Optional[int] = None
    instock: bool = True

    ward_daily_sale_rate: float = 0.0
    store_daily_sale_rate: float = 0.0
    blended_daily_sale_rate: float = 0.0

    ward_sale_frequency: float = 0.0
    store_sale_frequency: float = 0.0
    sale_frequency: float = 0.0

    prophet_daily_forecast: float = 0.0
    forecast_next_7_days_units: float = 0.0
    forecast_next_30_days_units: float = 0.0
    forecast_trend: str = "flat"

    days_of_inventory: Optional[float] = None
    stockout_risk_score: float = 0.0
    stockout_risk_level: str = "low"

    dead_stock_risk_score: float = 0.0
    dead_stock_risk_level: str = "low"

    last_sale_at: Optional[str] = None
    days_since_last_sale: Optional[int] = None

    suggested_action: str


class StoreRankSummary(BaseModel):
    store_id: str
    store_name: str
    ward: str
    county: str
    constituency: str

    ward_rank: int
    total_stores_in_ward: int
    composite_score: float

    sale_volume_average: float
    revenue_gain_average: float
    supply_average: float
    stockout_risk_average: float
    days_of_inventory_average: float


class IntelligenceResponse(BaseModel):
    filters: dict
    store_rank: StoreRankSummary
    total_products: int
    items: List[ProductIntelligenceItem]