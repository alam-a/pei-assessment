def test_output_tables():
    from assessment.config.output_tables import OutputTables
    
    output_tables = OutputTables("test_db")
    
    assert output_tables.ORDER_EXTRACTS == "test_db.order_extracts"
    assert output_tables.PRODUCT_EXTRACTS == "test_db.product_extracts"
    assert output_tables.CUSTOMER_EXTRACTS == "test_db.customer_extracts"
    
    assert output_tables.CUSTOMER_TRANSFORMED == "test_db.customer_transformed"
    assert output_tables.ORDER_TRANSFORMED == "test_db.order_transformed"
    assert output_tables.PRODUCT_TRANSFORMED == "test_db.product_transformed"
    
    assert output_tables.ENRICHED_CUSTOMERS == "test_db.enriched_customers"
    assert output_tables.ENRICHED_ORDERS == "test_db.enriched_orders"
    assert output_tables.ENRICHED_PRODUCTS == "test_db.enriched_products"
    
    assert output_tables.PROFIT_AGGREGATES == "test_db.profit_aggregates"

    assert output_tables.PROFIT_BY_YEAR == "test_db.profit_by_year"
    assert output_tables.PROFIT_BY_YEAR_CATEGORY == "test_db.profit_by_year_category"
    assert output_tables.PROFIT_BY_CUSTOMER == "test_db.profit_by_customer"
    assert output_tables.PROFIT_BY_YEAR_CUSTOMER == "test_db.profit_by_year_customer"