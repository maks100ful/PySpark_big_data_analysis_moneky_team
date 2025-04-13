from pyspark.sql import SparkSession
from shemas import *
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

    df_title_basics = spark.read.csv(
    "raw_data/title.basics.tsv.gz",
    sep="\t",
    header=True,
    schema=title_basics_schema,
    nullValue="\\N"
    )
    # df = df.withColumn("startYear", col("startYear").cast("int"))

    df_title_basics.show(n = 10)

    df_name_basics = spark.read.csv(
    "raw_data/name.basics.tsv.gz",
    sep="\t",
    header=True,
    schema=name_basics_schema,
    nullValue="\\N"
    )

    df_name_basics.show(n = 10)

    df_title_akas = spark.read.csv(
    "raw_data/title.akas.tsv.gz",
    sep="\t",
    header=True,
    schema=title_akas_schema,
    nullValue="\\N"
    )

    df_title_akas.show(n = 10)


    df_title_crew = spark.read.csv(
    "raw_data/title.crew.tsv.gz",
    sep="\t",
    header=True,
    schema=title_crew_schema,
    nullValue="\\N"
    )

    df_title_crew.show(n = 10)


    df_title_episode = spark.read.csv(
    "raw_data/title.episode.tsv.gz",
    sep="\t",
    header=True,
    schema=title_episode_schema,
    nullValue="\\N"
    )

    df_title_episode.show(n = 10)


    df_title_principals = spark.read.csv(
    "raw_data/title.principals.tsv.gz",
    sep="\t",
    header=True,
    schema=title_principals_schema,
    nullValue="\\N"
    )

    df_title_principals.show(n = 10)


    df_title_ratings = spark.read.csv(
    "raw_data/title.ratings.tsv.gz",
    sep="\t",
    header=True,
    schema=title_ratings_schema,
    nullValue="\\N"
    )

    df_title_ratings.show(n = 10)

    spark.stop()

if __name__ == "__main__":
    main()
