class OutputTables:
    def __init__(self, db: str):
        self.ORDER_EXTRACTS = f"{db}.order_extracts"
        self.PRODUCT_EXTRACTS = f"{db}.product_extracts"
        self.CUSTOMER_EXTRACTS = f"{db}.customer_extracts"

        self.ORDER_TRANSFORMED = f"{db}.order_transformed"
        self.PRODUCT_TRANSFORMED = f"{db}.product_transformed"
        self.CUSTOMER_TRANSFORMED = f"{db}.customer_transformed"
        
        self.ENRICHED_CUSTOMERS = f"{db}.enriched_customers"
        self.ENRICHED_PRODUCTS = f"{db}.enriched_products"
        self.ENRICHED_ORDERS = f"{db}.enriched_orders"

        self.PROFIT_AGGREGATES = f"{db}.profit_aggregates"

        self.PROFIT_BY_YEAR = f"{db}.profit_by_year"
        self.PROFIT_BY_YEAR_CATEGORY = f"{db}.profit_by_year_category"
        self.PROFIT_BY_CUSTOMER = f"{db}.profit_by_customer"
        self.PROFIT_BY_YEAR_CUSTOMER = f"{db}.profit_by_year_customer"