from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import concat_ws, col, row_number, current_timestamp
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

class transformations:

    #Deduplication
    def dedup(self, df: DataFrame, dedup_cols: List, cdc: str):

        df = df.withColumn("dedupKey", concat_ws("_", *dedup_cols))

        window_spec = Window.partitionBy("dedupKey").orderBy(col(cdc).desc())

        df = df.withColumn("dedupCounts", row_number().over(window_spec))

        df = df.filter(col('dedupCounts') == 1)

        df = df.drop("dedupKey", "dedupCounts")

        return df


    #Add processing timestamp
    def process_timestamp(self, df: DataFrame):

        df = df.withColumn("process_timestamp", current_timestamp())

        return df


    #Upsert into Delta table (Silver layer)
    def upsert(self, df: DataFrame, key_cols: List, table: str, cdc: str):
    
        # Build merge condition
        merge_condition = " AND ".join(
            [f"src.{col} = trg.{col}" for col in key_cols]
        )

        # Reference target Delta table
        delta_obj = DeltaTable.forName(spark, f"pysparkdbt.silver.{table}")

        # Perform merge (UPSERT)
        (
            delta_obj.alias("trg")
            .merge(df.alias("src"), merge_condition)
            .whenMatchedUpdateAll(
                condition=f"src.{cdc} >= trg.{cdc}"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        return 1
