from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import regexp_replace, col, to_date, monotonically_increasing_id, row_number, initcap
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from typing import List, Optional, Dict, Any

def clean_phone_numbers(column: Column) -> Column:
    return regexp_replace(column, "[^0-9]", "")

def deduplicate_rows(df: DataFrame, primary_keys: List[str]) -> DataFrame:
    if not primary_keys:
        raise ValueError("Primary keys list cannot be empty")
    
    # Create a window spec to identify duplicates
    window_spec = Window.partitionBy(*primary_keys).orderBy(col("temp_row_number"))
    
    # Add row number and filter to keep only first occurrence
    df_with_row_num = df.withColumn("temp_row_number", monotonically_increasing_id())
    df_with_duplicates = df_with_row_num.withColumn("temp_duplicate_rank", row_number().over(window_spec))
    
    # Filter to keep only the first occurrence (rank = 1)
    deduplicated_df = df_with_duplicates.filter(col("temp_duplicate_rank") == 1).drop("temp_duplicate_rank", "temp_row_number")
    
    return deduplicated_df

def deduplicate_using_aggregation(df: DataFrame, primary_keys: List[str], aggregation_rules: Optional[Dict[str, str]] = None) -> DataFrame:
    """
    Remove duplicate rows using aggregation. Maybe useful for things like price, where we may want average price
    """
    if not primary_keys:
        logger.warning("Primary keys list is empty, falling back to simple deduplication")
        return deduplicate_rows(df, primary_keys)
    
    if aggregation_rules is None:
        aggregation_rules = {}
    
    from pyspark.sql.functions import first, last, max, min, sum, avg, count
    aggregation_function_mapping = {
        "first": first,
        "last": last,
        "max": max,
        "min": min,
        "sum": sum,
        "avg": avg,
        "count": count,
    }

    # Get non-key columns
    non_key_columns = [col for col in df.columns if col not in primary_keys]
    aggregation_functions = {
        column: aggregation_function_mapping[aggregation_rules.get(column, "first")] for column in non_key_columns
    }
    
    # Build aggregation expressions - use first as default for primary keys - we can also let it fail here for unsupported aggregation functions
    agg_cols = [aggregation_functions.get(column, first)(col(column)).alias(column) for column in non_key_columns]
    
    # Group by primary keys and aggregate
    result_df = df.groupBy(*primary_keys).agg(*agg_cols)
    
    return result_df

def convert_date_str_to_date(date_column: Column, input_format: str = "dd/MM/yyyy") -> Column:
    """
    Convert date string to date.
    """
    # return to_date(date_column, input_format).cast("string").alias(date_column._jc.toString())
    return to_date(date_column, input_format)

def convert_name_to_alphabetic_with_name_case(name_column: Column) -> Column:
    """
    Remove non-alphabetic characters from name and convert to proper case.
    """
    # Remove non-alphabetic characters (keep only letters and spaces)
    cleaned = regexp_replace(name_column, r"[^a-zA-Z\s]", "")
    
    # Convert to proper case (first letter uppercase, rest lowercase)
    # Note: Spark doesn't have a built-in proper case function, so we'll use initcap
    proper_case = initcap(cleaned)
    
    return proper_case

def flatten_json_df(df: DataFrame) -> DataFrame:
    # Recursively flatten a nested dataframe, such as one loaded from JSON
    # We could limit the levels of recursion if needed, but I have not implemented that yet
    from pyspark.sql.types import ArrayType, StructType
    change_needed = True
    while change_needed:
        change_needed = False
        for col in df.schema.fields:
            print(col.name, col.dataType)
            if isinstance(col.dataType, ArrayType):
                df = df.withColumn(col.name, F.explode(col.name))
                change_needed = True
            if isinstance(col.dataType, StructType):
                for field in col.dataType.fields:
                    df = df.withColumn(f"{col.name}_{field.name}", F.col(f"{col.name}.{field.name}"))
                df = df.drop(col.name)
                change_needed = True
    return df

# def clean_json(spark: SparkSession, json_path: str) -> DataFrame:
#     # Flatten the JSON
#     json_df = spark.read.json(json_path)
#     json_df = flatten_json_df(json_df)
    
#     return json_df