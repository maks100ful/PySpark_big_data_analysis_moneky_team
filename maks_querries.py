from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, row_number, explode, rank, array_intersect, size, countDistinct
from pyspark.sql.window import Window

def get_top_genres(df_title_basics: DataFrame) -> DataFrame:
    df_exploded = df_title_basics.withColumn("genre", explode(col("genres")))
    df_genre_counts = df_exploded.groupBy("genre").agg(count("*").alias("movie_count"))
    window_spec = Window.orderBy(col("movie_count").desc())
    df_top_genres = df_genre_counts.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") <= 5)
    return df_top_genres.select("genre", "movie_count")

def get_highest_rated_movie_per_genre(df_title_basics: DataFrame, df_title_ratings: DataFrame) -> DataFrame:
    df_joined = df_title_basics.join(df_title_ratings, "tconst")
    df_exploded = df_joined.withColumn("genre", explode(col("genres")))
    window_spec = Window.partitionBy("genre").orderBy(col("averageRating").desc())
    df_best_per_genre = df_exploded.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)
    return df_best_per_genre.select("genre", "primaryTitle", "averageRating")

def get_directors_with_more_than_5_movies(df_title_crew: DataFrame, df_name_basics: DataFrame) -> DataFrame:
    df_directors = df_title_crew.withColumn("director", explode(col("directors")))
    df_director_counts = df_directors.groupBy("director").count().filter(col("count") > 5)
    df_director_names = df_director_counts.join(df_name_basics, df_director_counts.director == df_name_basics.nconst)
    return df_director_names.select("primaryName", "count")

def get_longest_running_tv_series(df_title_episode: DataFrame, df_title_basics: DataFrame) -> DataFrame:
    df_episode_counts = (
        df_title_episode.groupBy("parentTconst")
        .agg(count("*").alias("episode_count"))
    )
    
    df_result = df_episode_counts.join(df_title_basics, df_episode_counts.parentTconst == df_title_basics.tconst).select(
        "primaryTitle", "episode_count", "parentTconst"
    ).orderBy(col("episode_count").desc())
    
    return df_result

def get_movies_with_same_director_and_writer(df_title_crew: DataFrame, df_title_basics: DataFrame, df_name_basics: DataFrame) -> DataFrame:
    df_filtered = df_title_crew.filter(size(array_intersect(col("directors"), col("writers"))) > 0)
    df_exploded = df_filtered.withColumn("person", explode(array_intersect(col("directors"), col("writers"))))

    df_with_title = df_exploded.join(df_title_basics, "tconst")

    df_with_person_name = df_with_title.join(df_name_basics, df_exploded.person == df_name_basics.nconst)

    return df_with_person_name.select(
        df_title_basics.primaryTitle.alias("Movie Title"),
        df_name_basics.primaryName.alias("Director & Writer")
    ).distinct()

def get_movies_with_largest_cast(df_title_principals: DataFrame, df_title_basics: DataFrame) -> DataFrame:
    df_cast_counts = (
        df_title_principals.groupBy("tconst")
        .agg(countDistinct("nconst").alias("cast_size"))
        .limit(20)
    )
    
    df_result = df_cast_counts.join(df_title_basics, "tconst").select(
        "primaryTitle", "cast_size", "tconst"
    ).orderBy(col("cast_size").desc())
    
    return df_result