from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

    data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
    df = spark.createDataFrame(data, ["Name", "Age"])

    df.show()

    spark.stop()

if __name__ == "__main__":
    main()
