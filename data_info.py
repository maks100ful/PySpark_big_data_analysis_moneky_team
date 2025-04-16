from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, sum
from shemas import *
import os

def read_dataframe(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return spark.read.csv(path, sep="\t", header=True, schema=schema, nullValue="\\N")

def get_num_of_columns(df: DataFrame) -> int:
    return len(df.columns)

def get_num_of_rows(df: DataFrame) -> int:
    return df.count()

def count_nulls_per_column(df: DataFrame):
    df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()


def main():
    spark = SparkSession.builder.appName("DatasetStats").getOrCreate()
    schema_map = {
        "title.basics.tsv.gz": title_basics_schema,
        "title.akas.tsv.gz": title_akas_schema,
        "title.crew.tsv.gz": title_crew_schema,
        "title.episode.tsv.gz": title_episode_schema,
        "title.principals.tsv.gz": title_principals_schema,
        "title.ratings.tsv.gz": title_ratings_schema,
        "name.basics.tsv.gz": name_basics_schema,
    }

    for file_name in os.listdir("raw_data"):
        if file_name.endswith(".tsv") or file_name.endswith(".tsv.gz"):
            path = os.path.join("raw_data", file_name)
            schema = schema_map.get(file_name)
            current_df = read_dataframe(spark, path, schema)

            print(f'First 5 rows of {file_name} dataset:')
            current_df.show(5)
            print(f'The dataframe\'s schema is:')
            current_df.printSchema()

            print(f'Number of columns in {file_name} dataset: {get_num_of_columns(current_df)}')
            print(f'Number of rows in {file_name} dataset: {get_num_of_rows(current_df)}\n')

            print(f'Null counts by column in {file_name} dataset: \n')
            count_nulls_per_column(current_df)

    spark.stop()


if __name__ == "__main__":
    main()