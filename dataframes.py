from shemas import *
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import split, col

def get_title_basic_df(spark : SparkSession) -> DataFrame:
    df_title_basics = spark.read.csv(
    "raw_data/title.basics.tsv.gz",
    sep="\t",
    header=True,
    schema=title_basics_schema,
    nullValue="\\N"
    )
    df_title_basics = df_title_basics.withColumn(
        "genres", split(col("genres"), ",")
    ).withColumn(
        "isAdult", (col("isAdult") == "1")
    )
    return df_title_basics

def get_name_basic_df(spark : SparkSession) -> DataFrame: 
    df_name_basics = spark.read.csv(
    "raw_data/name.basics.tsv.gz",
    sep="\t",
    header=True,
    schema=name_basics_schema,
    nullValue="\\N"
    )
    df_name_basics = df_name_basics.withColumn(
        "primaryProfession", split(col("primaryProfession"), ",")
    ).withColumn(
        "knownForTitles", split(col("knownForTitles"), ",")
    )
    return df_name_basics

def get_title_akas_df(spark : SparkSession)-> DataFrame: 
    df_title_akas = spark.read.csv(
    "raw_data/title.akas.tsv.gz",
    sep="\t",
    header=True,
    schema=title_akas_schema,
    nullValue="\\N"
    )
    df_title_akas = df_title_akas.withColumn(
        "types", split(col("types"), ",")
    ).withColumn(
        "attributes", split(col("attributes"), ",")
    ).withColumn(
        "isOriginalTitle", (col("isOriginalTitle") == 1)
    )
    return df_title_akas

def get_title_crew_df(spark : SparkSession)-> DataFrame: 
    df_title_crew = spark.read.csv(
    "raw_data/title.crew.tsv.gz",
    sep="\t",
    header=True,
    schema=title_crew_schema,
    nullValue="\\N"
    )
    df_title_crew = df_title_crew.withColumn(
        "directors", split(col("directors"), ",")
    ).withColumn(
        "writers", split(col("writers"), ",")
    )
    return df_title_crew

def get_title_episode_df(spark : SparkSession)-> DataFrame: 

    df_title_episode = spark.read.csv(
    "raw_data/title.episode.tsv.gz",
    sep="\t",
    header=True,
    schema=title_episode_schema,
    nullValue="\\N"
    )
    return df_title_episode

def get_title_principals_df(spark : SparkSession)-> DataFrame: 
    df_title_principals = spark.read.csv(
    "raw_data/title.principals.tsv.gz",
    sep="\t",
    header=True,
    schema=title_principals_schema,
    nullValue="\\N"
    )
    return df_title_principals

def get_title_ratings_df(spark : SparkSession)-> DataFrame: 
    df_title_ratings = spark.read.csv(
    "raw_data/title.ratings.tsv.gz",
    sep="\t",
    header=True,
    schema=title_ratings_schema,
    nullValue="\\N"
    )
    return df_title_ratings