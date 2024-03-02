import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CarAnalysisApp").getOrCreate()

# Load the dataset
dataset = "datasets/car_prices.csv"
df = spark.read.options(header="True", inferSchema="True").csv(dataset)

# Print the count of records
print(f"Dataset count: {df.count()}")

# Print the dataset schema
print("Dataset Schema:")
df.printSchema()

# Selecting only the usefull columns for this example
df = df.select(["year", "make", "model", "transmission", "mmr", "sellingprice"])
print("Dataset sample")
df.show(10)

# 1. Identifying popular car models and manufacturers.
# 2. Analyzing trends in sales based on factors like price, year and transmission type.
# 3. Calculating descriptive statistics like average price, sales volume per region, etc.

sales_total = df.agg(F.sum("sellingprice")).first()[0]

# Best selling car models
print("Best selling car models")
df_top_models = (
    df.groupBy("make", "model")
    .agg(F.sum("sellingprice").alias("total_sellingprice"))
    .sort(F.col("total_sellingprice").desc())
)
df_top_models.show(10)

# Best selling car manufacturers
print("Best selling car manufacturers")
df_top_manufacturers = (
    df.groupBy("make")
    .agg(F.sum("sellingprice").alias("total_sellingprice"))
    .sort(F.col("total_sellingprice").desc())
    .withColumn(
        "percentage",
        (F.round(F.col("total_sellingprice") / F.lit(sales_total) * 100, 2)),
    )
)
df_top_manufacturers.show(10)

# Number of cars sold by transmission type over the years
print("Number of cars sold by transmission type over the years")
df_sales_by_transmission_type = (
    df.select(["year", "transmission"])
    .filter(
        ((F.col("transmission") == "automatic") | (F.col("transmission") == "manual"))
    )
    .groupBy(["year", "transmission"])
    .agg(F.count("*").alias("count"))
    .sort(F.col("year").desc(), F.col("count").desc())
)
df_sales_by_transmission_type.show()

# Given that the documentation says that the mmr column is the estimated value of the price of the car,
# it is interesting to calculate the percentage of sales that exceed that expected price
print("Percentage of sales that exceed that expected price of the car")
df_overestimation_selling = (
    df.select(["mmr", "sellingprice"])
    .withColumn(
        "sold_over_estimation",
        F.when((F.col("mmr")) < (F.col("sellingprice")), 1).otherwise(0),
    )
    .agg(
        F.sum("sold_over_estimation").alias("sold_over_estimation_total"),
        F.round(((100 * F.sum("sold_over_estimation")) / df.count()), 2).alias(
            "sold_over_estimation_percentage"
        ),
    )
)
df_overestimation_selling.show(10)

# For those of you who are JDMs car enthusiasts, the Nissan GTR Skyline is a legend,
# so let's find out according to this dataset how its price has behaved over time
df_gtr = df.select(["year", "make", "model", "sellingprice"]).filter(
    (F.col("make") == "Nissan") & (F.col("model") == "GT-R")
)
gtr_min_price = df_gtr.select(F.min("sellingprice").alias("min")).collect()[0][0]
gtr_max_price = df_gtr.select(F.max("sellingprice").alias("max")).collect()[0][0]

print("Nissan GT-R")
print(f"Lowest price: {gtr_min_price}")
print(f"Highest price: {gtr_max_price}")
print("Average price over the years:")
df_gtr.groupBy("year").agg(F.round(F.avg("sellingprice")).alias("avg_price")).sort(
    "year"
)
df.show()
