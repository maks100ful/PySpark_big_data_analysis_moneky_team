from pyspark.sql import SparkSession

from dataframes import *

from queries_vitalii import genre_combination_popilarity
from rosana_queries import get_3_top_rated_alive_principals_by_category
from maks_querries import get_longest_running_tv_series
from Lidiia_6 import get_top_rated_horror_movies_by_year

def main():
    spark = SparkSession.builder.appName("MySparkApp") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.5") \
    .getOrCreate()

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

    #genre_combination_popilarity(df_title_basic=df_title_basic, df_title_ratings= df_title_ratings)
    #get_3_top_rated_alive_principals_by_category(df_title_principals=df_title_principals, df_name_basics=df_name_basics, df_title_ratings=df_title_ratings)
    #get_longest_running_tv_series(df_title_episode=df_title_episode, df_title_basics=df_title_basic)
    #get_top_rated_horror_movies_by_year(df_title_basic=df_title_basic, df_title_ratings=df_title_ratings)

    spark.stop()

if __name__ == "__main__":
    main()