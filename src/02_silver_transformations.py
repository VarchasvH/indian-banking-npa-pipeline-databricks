from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import col, last, monotonically_increasing_id, when, regexp_replace, isnan
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

# Defining the Schema that we want to write our silver data with
schema = StructType([
    StructField("year", IntegerType(), nullable=False),
    StructField("bank_name", StringType(), nullable=False),
    StructField("gross_npa", DoubleType(), nullable=True),
    StructField("gross_advances", DoubleType(), nullable=True),
    StructField("npa_ratio", DoubleType(), nullable=True),
    StructField("is_summary_row", BooleanType(), nullable=False)
])

# Table metadata
@dp.table(
    name = "silver_npa_bankwise",
    comment = "Ingesting the raw data from the bronze layer, cleaning and transforming the data.",
    schema = schema,
    partition_cols = ["year"],
    table_properties={
        "quality" : "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect("valid_np_ratio", "npa_ratio IS NULL OR npa_ratio BETWEEN 0 AND 100")
@dp.expect_or_drop("valid_year", "year BETWEEN 2004 AND 2025")
@dp.expect_or_fail("valid_bank_name", "bank_name IS NOT NULL")
def silver_npa_bankwise():
    # Reading the data from the bronze table 
    bronze_df = dp.read("bronze_npa_bankwise")

    # Removing the commas from the the columns that will interfere with schema enforcement
    for c in ["gross_npa", "gross_advances", "npa_ratio"]:
        bronze_df = bronze_df.withColumn(c, regexp_replace(c, ",", ""))
        bronze_df = bronze_df.withColumn(c, 
            when((col(c) == "NaN") | (col(c) == ""), None).otherwise(col(c))
        )
        
    # Casting these values to DoubleType
    bronze_df = bronze_df\
        .withColumn("gross_npa", col("gross_npa").cast(DoubleType()))\
        .withColumn("gross_advances", col("gross_advances").cast(DoubleType()))\
        .withColumn("npa_ratio", col("npa_ratio").cast(DoubleType()))

    # Casting year to IntegerType 
    bronze_df = bronze_df.withColumn("year", col("year").cast(IntegerType()))

    '''
    Since our year column has null values after the new year starts in the data, as the dataset included all a spanning year column for all of that, we need to add the last year we saw before the current row if it's null currently otherwise if it is a new value we will keep it as it is and continue doing so.
    '''
    # All the rows before including the current row
    windowSpec = Window.orderBy(monotonically_increasing_id()).rowsBetween(Window.unboundedPreceding, 0)

    # Fill the null values with the previous non-null value
    bronze_df = bronze_df.withColumn('year', last('year', ignorenulls = True).over(windowSpec))

    # Drop the rows with all the columns NaN
    bronze_df = bronze_df.filter(col("gross_advances").isNotNull())

    # Add the is_summary_row flag for easier downstream operations
    bronze_df = bronze_df.withColumn('is_summary_row', when(col('bank_name').isin(["PUBLIC SECTOR BANKS", "PRIVATE SECTOR BANKS", "FOREIGN BANKS", "ALL SCHEDULED COMMERCIAL BANKS", "NATIONALISED BANKS", "SMALL FINANCE BANKS", "STATE BANK OF INDIA AND ITS ASSOCIATES"]), True).otherwise(False))

    return bronze_df
