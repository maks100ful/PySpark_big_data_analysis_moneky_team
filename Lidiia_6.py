from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, row_number, count
from pyspark.sql.window import Window



def get_top_voted_movies_per_genre(df_title_basic:DataFrame, df_title_ratings:DataFrame):
    df = df_title_basic.join(df_title_ratings, on="tconst")

    df = df.withColumn("genre", explode(df.genres))

    window_spec = Window.partitionBy("genre").orderBy(df.numVotes.desc())

    result = df.withColumn("rank", row_number().over(window_spec)) \
        .filter("rank = 1") \
        .select("genre", "primaryTitle", "numVotes")
    return result

def get_top_rated_horror_movies_by_year(df_title_basic:DataFrame, df_title_ratings:DataFrame):
    df = df_title_basic.join(df_title_ratings, on="tconst")

    df = df.withColumn("genre", explode(col("genres"))) \
        .filter(col("genre") == "Horror")

    window_spec = Window.partitionBy("startYear").orderBy(df.averageRating.desc())

    result = df.withColumn("rank", row_number().over(window_spec)) \
        .filter("rank = 1") \
        .select("startYear", "primaryTitle", "averageRating")
    return result

def get_top_actors_high_rated_movies(df_title_principals:DataFrame, df_name_basics:DataFrame, df_title_ratings:DataFrame):
    df = df_title_principals.join(df_title_ratings, on="tconst") \
        .join(df_name_basics, on="nconst") \
        .filter((col("category") == "actor") & (col("averageRating") > 9.0))

    df_actor_count = df.groupBy("primaryName").agg(count("*").alias("movie_count"))

    window_spec = Window.orderBy(col("movie_count").desc())

    result = df_actor_count.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 10)
    return result

def get_top_voted_movies_per_genre_in_2025(df_title_basic:DataFrame, df_title_ratings:DataFrame):
    df = df_title_basic.join(df_title_ratings, on="tconst") \
        .filter(df_title_basic.startYear == "2025")

    df = df.withColumn("genre", explode(df.genres))

    window_spec = Window.partitionBy("genre").orderBy(df.numVotes.desc())

    result = df.withColumn("rank", row_number().over(window_spec)) \
        .filter("rank = 1") \
        .select("genre", "primaryTitle", "numVotes")
    return result
        
def get_most_popular_genre_in_2025(df_title_basic:DataFrame):
    df = df_title_basic.filter(df_title_basic.startYear == "2025") \
        .withColumn("genre", explode(df_title_basic.genres))

    df_genre_count = df.groupBy("genre").agg(count("*").alias("movie_count"))

    window_spec = Window.orderBy(col("movie_count").desc())

    result = df_genre_count.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1).drop("rank")
    return result

def get_top_countries_by_movie_count_2025(df_title_akas:DataFrame, df_title_basic:DataFrame):
    df = df_title_basic.filter(col("startYear") == "2025") \
        .join(df_title_akas, df_title_basic.tconst == df_title_akas.titleId) \
        .filter(col("region").isNotNull())  

    df_country_count = df.groupBy("region").count()

    window_spec = Window.orderBy(col("count").desc())

    result = df_country_count.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 10)
    return result