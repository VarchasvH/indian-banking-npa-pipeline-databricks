from pyspark import pipelines as dp
import pandas as pd

@dp.table(
    name="bronze_npa_bankwise",
    comment="Raw NPA data ingested from RBI source file, no transformations applied",
    table_properties={
        "quality" : "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_npa_bankwise():
    pdf = pd.read_csv(
        "/Volumes/workspace/default/rbi_banking_raw/rbi_npa_bankwise_2004_2025.txt",
        sep="\t",
        header=None,
        encoding="utf-8",
        skiprows=6,
        names=["year", "bank_name", "gross_npa", "gross_advances", "npa_ratio"],
    )
    return spark.createDataFrame(pdf)
