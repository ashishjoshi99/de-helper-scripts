"""Utilities to load raw files into a Bronze Delta table.

This module provides `BronzeLoader`, a small helper that reads files
from a path, enriches them with ingestion metadata, and writes to a
Delta table in the configured catalog/schema.

Example:
    loader = BronzeLoader(spark, catalog="my_catalog", schema="bronze")
    loader.load_to_bronze("/mnt/raw/events", "events_bronze", "parquet")
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
from typing import Dict, Optional
import logging
import re


class BronzeLoader:

    def __init__(self, spark: SparkSession, catalog: str, schema: str, logger: Optional[logging.Logger] = None):
        """Create a BronzeLoader.

        Args:
            spark: active SparkSession
            catalog: target catalog name
            schema: target schema/database name
            logger: optional Python logger; a module logger is used by default
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.logger = logger or logging.getLogger(__name__)

    def load_to_bronze(
        self,
        volume_path: str,
        bronze_table: str,
        file_format: str,
        options: Optional[Dict[str, str]] = None,
        mode: str = "append",
    ) -> None:
        """Read data from `volume_path`, add ingestion metadata, and save it.

        This method catches exceptions to log a helpful message and re-raises.
        """
        try:
            # Read volume files into DataFrame
            df = self.read_from_volume(volume_path, file_format, options)

            cleaned_df = self.cleanColumnNames(df)

            # Enrich dataset with ingestion timestamp and source file
            enriched_df = self.add_metadata(cleaned_df)

            # Write to Bronze table
            self.write_to_bronze(enriched_df, bronze_table, mode)

            self.logger.info(f"Successfully loaded data to {self.catalog}.{self.schema}.{bronze_table}")

        except Exception:
            self.logger.exception("Failed to load data to bronze table")
            raise

    def read_from_volume(self, volume_path: str, file_format: str, options: Optional[Dict[str, str]] = None) -> DataFrame:
        """Read files from `volume_path` using the provided format and options.

        The reader options are applied by chaining `.option(...)` calls on the
        Spark DataFrameReader.
        """
        reader = self.spark.read.format(file_format)

        if options:
            for key, value in options.items():
                reader = reader.option(key, value)

        return reader.load(volume_path)

    def cleanColumnNames(self, df: DataFrame) -> DataFrame:
        "Clean Column Names"
        return df.toDF(*[
        re.sub(r'[^0-9a-zA-Z]+', '_', col).strip('_').lower() 
        for col in df.columns
    ])


    def add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ingestion metadata columns to the DataFrame."""
        return (
            df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", lit("volume"))
        )

    def write_to_bronze(self, df: DataFrame, bronze_table: str,mode: str = "append") -> None:
        """Write the DataFrame as a Delta table in the configured catalog/schema."""
        
        table_write_path = f"{self.catalog}.{self.schema}.{bronze_table.replace('.csv','')}"
            

        (
            df.write
            .format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .saveAsTable(table_write_path)
        )
