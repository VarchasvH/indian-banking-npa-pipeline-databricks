from pyspark import pipelines as dp
from pyspark.sql.functions import col, dense_rank, lag, round
from pyspark.sql.window import Window

@dp.table(
    name = "gold_npa_trend_by_bank_group",
    comment = "Year-wise NPA ratio trend for each bank group (Public, Private, Foreign, Small Finance). Use this table to analyze the 2015-2018 Indian banking crisis and subsequent recovery.",
    table_properties = {
        "quality" : "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_npa_trend_by_bank_group():
    # Read the silver table
    silver_df = dp.read("silver_npa_bankwise")

    # We are gonna add two new columns to our data, previous year ratio and the yoy change per year.
    # We partition by bank_name so each bank group gets its own independent window for lag calculation
    windowSpec = Window.partitionBy("bank_name").orderBy("year")
    npa_trend = silver_df\
        .filter(col("is_summary_row") == True)\
        .withColumn("prev_year_ratio", lag("npa_ratio", 1).over(windowSpec))\
        .withColumn("yoy_change", round(col("npa_ratio") - col("prev_year_ratio"), 2))\
        .select("year", "bank_name", "npa_ratio", "yoy_change")\
        .orderBy(col("bank_name"), col("year").desc())
    return npa_trend


@dp.table(
    name = "gold_worst_performing_banks",
    comment = "Top 5 worst performing individual banks by NPA ratio for each year from 2004-2025. Excludes summary/aggregate rows. Use this table to identify chronically stressed banks over time.",
    table_properties = {
        "quality" : "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_worst_performing_banks():
    # Read the silver table
    silver_df = dp.read("silver_npa_bankwise")

    # We are going to take the top 5 worst performing banks for each year based on their npa_ratio
    # We partition by year so the ranking resets for every year independently
    windowSpec = Window.partitionBy(col('year')).orderBy(col("npa_ratio").desc())
    worst_perf = silver_df\
        .filter(col("is_summary_row") == False)\
        .withColumn("rank", dense_rank().over(windowSpec))\
        .filter(col("rank") < 6)\
        .select(col("year"), col("bank_name"), col("gross_npa"), col("gross_advances"), col("npa_ratio"))

    return worst_perf


@dp.table(
    name = "gold_credit_growth_vs_npa",
    comment = "Bank group level analysis of credit growth percentage versus NPA ratio over time. Reveals the correlation between aggressive lending periods and subsequent NPA deterioration, particularly visible in public sector banks between 2010-2018.",
    table_properties = {
        "quality" : "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_credit_growth_vs_npa():
    # Read Silver table
    silver_df = dp.read("silver_npa_bankwise")

    # We are calculating credit growth percentage year over year and tracking how npa_ratio changed
    # alongside it to reveal the correlation between aggressive lending and subsequent NPA deterioration
    windowSpec = Window.partitionBy(col("bank_name")).orderBy(col('year'))
    bank_group = silver_df\
        .filter(col("is_summary_row") == True)\
        .withColumn("prev_advances", lag("gross_advances", 1).over(windowSpec))\
        .withColumn("credit_growth_pct", 
            round((col("gross_advances") - col("prev_advances")) / col("prev_advances") * 100, 2))\
        .withColumn("prev_year_ratio", lag("npa_ratio", 1).over(windowSpec))\
        .withColumn("yoy_change", round(col("npa_ratio") - col("prev_year_ratio"), 2))\
        .select("year", "bank_name", "gross_advances","credit_growth_pct", "npa_ratio", "yoy_change")\
        .orderBy("bank_name", "year")
    return bank_group
