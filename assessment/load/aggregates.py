from pyspark.sql.functions import col, round, year, sum, avg
from pyspark.sql import SparkSession

def create_aggregate_table(spark: SparkSession, base_path: str) -> None:
    """Create aggregate table showing profit by Year, Product Category, Product Sub Category, Customer"""
    print("Creating aggregate table...")
    
    # Load enriched orders
    enriched_orders = spark.read.parquet(f"{base_path}/enriched/enriched_orders.parquet")
    
    # Create aggregate table with multiple dimensions
    aggregate_df = enriched_orders \
        .withColumn("year", year(col("order_date"))) \
        .groupBy("year", "category", "sub_category", "customer_id", "customer_name") \
        .agg(round(sum("profit"), 2).alias("total_profit")) \
        .orderBy("year", "category", "sub_category", "customer_name")
    
    aggregate_df.write.mode("overwrite").parquet(f"{base_path}/aggregate/profit_aggregates.parquet")

def show_aggregates_in_sql(spark: SparkSession, base_path: str) -> None:
    """Load aggregate table"""
    spark.read.parquet(f"{base_path}/aggregate/profit_aggregates.parquet").createOrReplaceTempView("profit_aggregates")

    # Profit by Year
    spark.sql("""
        SELECT 
            year,
            ROUND(SUM(total_profit), 2) as total_profit
        FROM profit_aggregates 
        GROUP BY year 
        ORDER BY year
    """).show()

    # Profit by Year + Product Category
    spark.sql("""
        SELECT 
            year,
            category,
            ROUND(SUM(total_profit), 2) as total_profit
        FROM profit_aggregates 
        GROUP BY year, category 
        ORDER BY year, total_profit DESC
    """).show()

    # Profit by Customer
    spark.sql("""
        SELECT 
            customer_id,
            customer_name,
            ROUND(SUM(total_profit), 2) as total_profit
        FROM profit_aggregates 
        GROUP BY customer_id, customer_name 
        ORDER BY total_profit DESC
    """).show()

    # Profit by Customer + Year
    spark.sql("""
        SELECT 
            year,
            customer_id,
            customer_name,
            ROUND(SUM(total_profit), 2) as total_profit
        FROM profit_aggregates 
        GROUP BY year, customer_id, customer_name 
        ORDER BY year, total_profit DESC
    """).show()

