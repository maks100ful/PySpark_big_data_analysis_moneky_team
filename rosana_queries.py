from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, avg
from pyspark.sql.window import Window

def get_top_rated_movies_in_2024_with_more_than_1000_votes(df_title_basic: DataFrame, df_title_ratings: DataFrame) -> DataFrame:
    result = df_title_basic.join(df_title_ratings, on='tconst') \
            .filter((col('startYear') == 2024) & (col('numVotes') > 1000)) \
            .orderBy(col('averageRating').desc())
    
    return result

def get_average_num_of_votes_by_language(df_title_akas: DataFrame, df_title_ratings: DataFrame) -> DataFrame:
    result = df_title_akas.join(df_title_ratings, df_title_akas.titleId == df_title_ratings.tconst) \
            .groupBy(col('language')) \
            .agg(avg('numVotes').alias('average number of votes')) \
            .orderBy(col('average number of votes').desc())
    
    return result
    


