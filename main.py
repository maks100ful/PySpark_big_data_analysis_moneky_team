from pyspark.sql import SparkSession
from shemas import title_basics_schema
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

    df = spark.read.csv(
    "raw_data/title.basics.tsv.gz",
    sep="\t",
    header=True,
    schema=title_basics_schema,
    nullValue="\\N"
    )
    # df = df.withColumn("startYear", col("startYear").cast("int"))

    df.show(n = 50)

    spark.stop()

if __name__ == "__main__":
    main()
