from pyspark.sql import SparkSession

from dataframes import*

def main():
    spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

    df_name_basics = get_name_basic_df(spark=spark)
    df_name_basics.show(n=10)
    
    df_title_akas = get_title_akas_df(spark=spark)
    df_title_akas.show(n=10)
    
    df_title_basic = get_title_basic_df(spark=spark)
    df_title_basic.show(n=10)

    df_title_crew = get_title_crew_df(spark=spark)
    df_title_crew.show(n=10)

    df_title_episode = get_title_episode_df(spark=spark)
    df_title_episode.show(n=10)

    df_title_principals = get_title_principals_df(spark=spark)
    df_title_principals.show(n=10)

    df_title_ratings = get_title_ratings_df(spark=spark)
    df_title_ratings.show(n=10)
    spark.stop()

if __name__ == "__main__":
    main()
