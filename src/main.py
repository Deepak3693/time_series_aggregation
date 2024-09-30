import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_timestamp(df):
    # generic function to parse the timestamp column with different formats to generic format
    df = df.withColumn(
        "ParsedTimestamp",
        F.when(
            F.col("Timestamp").cast("long").isNotNull(),
            F.from_unixtime(F.col("Timestamp").cast("long")),
        )
        .when(
            F.col("Timestamp").rlike(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z"),
            F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        )
        .when(
            F.col("Timestamp").rlike(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"),
            F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss"),
        )
        .otherwise(
            F.to_timestamp(F.col("Timestamp"))
        ),
    )
    return df


def main(input_path, output_path, bucket_duration):
    # initializing spark
    spark = (
        SparkSession.builder.appName("TimeSeriesAggregation")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # read input data
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    input_df = parse_timestamp(df)

    # applying window function on timestamp based on bucket duration
    aggregated_df = input_df.groupBy(
        F.window("ParsedTimestamp", bucket_duration), "Metric"
    ).agg(
        F.round(F.avg("Value"), 2).alias("Average"),
        F.min("Value").alias("Min"),
        F.max("Value").alias("Max"),
    )

    flattened_df = aggregated_df.selectExpr(
        "window.start as Bucket_start",
        "window.end as Bucket_end",
        "Metric",
        "Average",
        "Min",
        "Max",
    ).orderBy("Bucket_start")

    #exporting the output
    flattened_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Time Series Aggregation Job")
    parser.add_argument(
        "--input_path", type=str, required=True, help="Path to input CSV file"
    )
    parser.add_argument(
        "--output_path",
        type=str,
        required=True,
        help="Directory to save the output CSV",
    )
    parser.add_argument(
        "--bucket_duration",
        type=str,
        default="24 hours",
        help='Duration for the time buckets (default: "24 hours")',
    )

    args = parser.parse_args()

    main(args.input_path, args.output_path, args.bucket_duration)
