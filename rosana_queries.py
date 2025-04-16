from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, avg, row_number, explode, count
from pyspark.sql.window import Window

def get_top_rated_movies_in_2024_with_more_than_1000_votes(df_title_basic: DataFrame, df_title_ratings: DataFrame) -> DataFrame:
    result = df_title_basic.join(df_title_ratings, on='tconst') \
            .filter((col('startYear') == 2024) & (col('numVotes') > 1000)) \
            .orderBy(col('averageRating').desc()) \
            .select('primaryTitle', 'averageRating', 'numVotes')
    
    return result


def get_average_num_of_votes_by_language(df_title_akas: DataFrame, df_title_ratings: DataFrame) -> DataFrame:
    result = df_title_akas.join(df_title_ratings, df_title_akas.titleId == df_title_ratings.tconst) \
            .groupBy(col('language')) \
            .agg(avg('numVotes').alias('average number of votes')) \
            .orderBy(col('average number of votes').desc()) \
            .select('language', 'average number of votes')
    
    return result


def get_3_top_rated_alive_principals_by_category(df_name_basics: DataFrame, df_title_principals: DataFrame, 
                                               df_title_ratings: DataFrame) -> DataFrame:
    alive_people = df_name_basics.filter(col('deathYear').isNull()) \
                   .select('nconst', 'primaryName', 'birthYear')
    
    alive_principals = alive_people.join(df_title_principals, on='nconst') \
                      .select('tconst', 'category', 'primaryName', 'birthYear')
    
    ratings = df_title_ratings.filter(col('numVotes') > 100000) \
                              .select('tconst', 'averageRating')
    
    principals_ratings = alive_principals.join(ratings, on='tconst')

    window_function = Window.partitionBy(col('category')).orderBy(col('averageRating').desc())
    result = principals_ratings.withColumn('rank', row_number().over(window_function)) \
             .filter(col('rank') <= 3) \
             .select('category', 'primaryName', 'birthYear', 'averageRating')
    result.write.mode('overwrite').option('header', 'true').csv('2_query_csv')
    
    return result
    

def get_5_most_popular_movies_per_country(df_title_akas: DataFrame, df_title_ratings:DataFrame) -> DataFrame:
    movies = df_title_akas.select('titleId', 'title', 'region')
    ratings = df_title_ratings.select('tconst', 'numVotes')

    df = movies.join(ratings, movies.titleId == ratings.tconst)

    window_function= Window.partitionBy('region').orderBy(col('numVotes').desc())
    result = df.withColumn('rank', row_number().over(window_function)) \
               .filter(col('rank') <= 5) \
                .select('region', 'title')

    return result


def get_3_top_rated_short_movies_per_genre(df_title_basics: DataFrame, df_title_ratings: DataFrame) -> DataFrame:
    df_short = df_title_basics.filter(col("titleType") == 'short') \
                              .select("tconst", "primaryTitle", 'genres')

    df = df_short.join(df_title_ratings, on="tconst") \
                 .withColumn('genre', explode(col('genres')))

    window_function = Window.partitionBy('genre').orderBy(col("averageRating").desc())

    result = df.withColumn("rank", row_number().over(window_function)) \
               .filter(col("rank") <= 3) \
               .select('genre', "primaryTitle", "averageRating")

    return result


def get_top_voted_episode_per_season(df_title_episode: DataFrame, df_title_ratings: DataFrame) -> DataFrame:
    df = df_title_episode.join(df_title_ratings, on="tconst")

    window_function = Window.partitionBy("parentTconst", "seasonNumber").orderBy(col("numVotes").desc())
    result = df.withColumn("rank", row_number().over(window_function)) \
               .filter(col("rank") == 1) \
               .select("parentTconst", "seasonNumber", "episodeNumber", "numVotes", "averageRating")

    return result

    