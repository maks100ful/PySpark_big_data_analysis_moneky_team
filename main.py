from pyspark.sql import SparkSession

from dataframes import*

from Lidiia_6 import get_top_voted_movies_per_genre, get_top_rated_horror_movies_by_year, get_top_actors_high_rated_movies,\
get_top_voted_movies_per_genre_in_2025, get_most_popular_genre_in_2025, get_top_countries_by_movie_count_2025


def main():
    spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

    df_name_basics = get_name_basic_df(spark=spark)
    # df_name_basics.show(n=10)
    
    df_title_akas = get_title_akas_df(spark=spark)
    # df_title_akas.show(n=10)
    
    df_title_basic = get_title_basic_df(spark=spark)
    # df_title_basic.show(n=10)

    df_title_crew = get_title_crew_df(spark=spark)
    # df_title_crew.show(n=10)

    df_title_episode = get_title_episode_df(spark=spark)
    # df_title_episode.show(n=10)

    df_title_principals = get_title_principals_df(spark=spark)
    # df_title_principals.show(n=10)

    df_title_ratings = get_title_ratings_df(spark=spark)
    # df_title_ratings.show(n=10)


    

    # get_top_voted_movies_per_genre(df_title_basic, df_title_ratings).show(truncate=False)
    # get_top_rated_horror_movies_by_year(df_title_basic, df_title_ratings).show(truncate=False)
    # get_top_actors_high_rated_movies(df_title_principals, df_name_basics, df_title_ratings).show(truncate=False)
    # get_top_voted_movies_per_genre_in_2025(df_title_basic, df_title_ratings).show(truncate=False)
    # get_most_popular_genre_in_2025(df_title_basic).show(truncate=False)
    get_top_countries_by_movie_count_2025(df_title_akas, df_title_basic).show(truncate=False)
    spark.stop()
    

if __name__ == "__main__":
    main()
