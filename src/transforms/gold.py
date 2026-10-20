"""Gold: business-facing outputs.

Pricing rule (simple and transparent on purpose):

* inventory >= 10  -> 5% below estimated market price   (move stock)
* inventory <= 2   -> 5% above estimated market price   (scarce)
* otherwise        -> at estimated market price
* never below a $1.00 floor; rounded to 2 decimals.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

HIGH_INVENTORY_THRESHOLD = 10
LOW_INVENTORY_THRESHOLD = 2
HIGH_INVENTORY_DISCOUNT = 0.05
LOW_INVENTORY_PREMIUM = 0.05
PRICE_FLOOR = 1.00
DATA_ORIGIN = "SIMULATED_DEMO"


def build_recommendations(valid: DataFrame) -> DataFrame:
    market = F.col("estimated_market_price")
    inventory = F.col("inventory_count")
    high = inventory >= HIGH_INVENTORY_THRESHOLD
    low = inventory <= LOW_INVENTORY_THRESHOLD

    rule_price = (
        F.when(high, market * (1 - HIGH_INVENTORY_DISCOUNT))
        .when(low, market * (1 + LOW_INVENTORY_PREMIUM))
        .otherwise(market)
    )
    rule_reason = (
        F.when(high, F.lit("HIGH_INVENTORY_5PCT_BELOW_MARKET"))
        .when(low, F.lit("LOW_INVENTORY_5PCT_ABOVE_MARKET"))
        .otherwise(F.lit("AT_MARKET"))
    )
    floored = rule_price < PRICE_FLOOR
    recommended = F.round(F.greatest(rule_price, F.lit(PRICE_FLOOR)), 2)

    return valid.select(
        "isbn_13",
        "title",
        "author",
        "category",
        "condition",
        F.col("listing_price").alias("current_listing_price"),
        "estimated_market_price",
        recommended.alias("recommended_price"),
        F.round(recommended - F.col("listing_price"), 2).alias("price_change"),
        "inventory_count",
        F.when(floored, F.concat(rule_reason, F.lit("+PRICE_FLOOR")))
        .otherwise(rule_reason)
        .alias("recommendation_reason"),
        F.lit(DATA_ORIGIN).alias("data_origin"),
        "processing_date",
    )


def build_category_summary(
    recommendations: DataFrame, rejected: DataFrame, processing_date: date
) -> DataFrame:
    valid_stats = recommendations.groupBy("category").agg(
        F.countDistinct("isbn_13").alias("valid_book_count"),
        F.count(F.lit(1)).alias("valid_listing_count"),
        F.round(F.avg("current_listing_price"), 2).alias("avg_listing_price"),
        F.round(F.median("current_listing_price"), 2).alias("median_listing_price"),
        F.round(F.avg("recommended_price"), 2).alias("avg_recommended_price"),
        F.sum("inventory_count").alias("total_inventory"),
    )
    invalid_counts = rejected.groupBy("category").agg(
        F.count(F.lit(1)).alias("invalid_record_count")
    )
    # Full outer join: a category whose rows were all rejected still shows up.
    return (
        valid_stats.join(invalid_counts, on="category", how="full_outer")
        .fillna(
            0,
            subset=[
                "valid_book_count",
                "valid_listing_count",
                "total_inventory",
                "invalid_record_count",
            ],
        )
        .withColumn("processing_date", F.lit(processing_date.isoformat()).cast("date"))
        .orderBy("category")
    )
