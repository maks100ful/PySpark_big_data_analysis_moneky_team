from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, row_number
from pyspark.sql.window import Window
from dataframes import *

def answer_business_questions():
    spark = SparkSession.builder.appName("BusinessQuestions").getOrCreate()

    df_title_akas = get_title_akas_df(spark=spark)
    df_title_basic = get_title_basic_df(spark=spark)
    df_title_ratings = get_title_ratings_df(spark=spark)
    df_title_episode = get_title_episode_df(spark=spark)

    # 1. Усі назви фільмів, доступні українською мовою (filter)
    df_title_akas.filter(col("language") == "uk").select("title").show()

    # 2. Середній рейтинг фільмів по кожному жанру (group by)
    df_title_basic.join(df_title_ratings, "tconst") \
        .groupBy("genres") \
        .agg(avg("averageRating").alias("avg_rating")) \
        .orderBy(col("avg_rating").desc()) \
        .show()

    # 3. Кількість назв, що є оригінальними (filter + group by)
    df_title_akas.filter(col("isOriginalTitle") == True) \
        .groupBy("region") \
        .agg(count("*").alias("original_titles_count")) \
        .orderBy(col("original_titles_count").desc()) \
        .show()

    # 4. Найвищий рейтинг фільмів кожного жанру (window + join)
    window_genre = Window.partitionBy("genres").orderBy(col("averageRating").desc())
    df_with_ranks = df_title_basic.join(df_title_ratings, "tconst") \
        .withColumn("rank", row_number().over(window_genre)) \
        .filter(col("rank") == 1) \
        .select("genres", "primaryTitle", "averageRating") \
        .show()

    # 5. Серіали з більше ніж 5 сезонами (join + filter + group by)
    df_title_episode.filter(col("seasonNumber").isNotNull()) \
        .groupBy("parentTconst") \
        .agg(count("seasonNumber").alias("num_seasons")) \
        .filter(col("num_seasons") > 5) \
        .show()

    # 6. Найкращі епізоди серіалів (join + window)
    episode = df_title_episode.alias("episode")
    rating = df_title_ratings.alias("rating")
    title_basic = df_title_basic.alias("title_basic")

    window_episode = Window.partitionBy("episode.parentTconst").orderBy(col("rating.numVotes").desc())

    top_episodes = episode.join(rating, episode["tconst"] == rating["tconst"]) \
        .join(title_basic, episode["parentTconst"] == title_basic["tconst"]) \
        .withColumn("rank", row_number().over(window_episode)) \
        .filter(col("rank") == 1) \
        .select(
            col("episode.seasonNumber").alias("seasonNumber"),  # Номер сезону
            col("episode.episodeNumber").alias("episodeNumber"),  # Номер епізоду
            col("title_basic.primaryTitle").alias("showTitle"),  # Назва шоу
            col("rating.averageRating"),
            col("rating.numVotes")
        ) \
        .orderBy(col("rating.numVotes").desc())  # Сортуємо по кількості голосів

    # Збереження результату у CSV файл
    top_episodes.write.csv("/workspace/сsv/top_episodes.csv", header=True, mode="overwrite")

    # Показ результатів
    top_episodes.show()

# Завершення роботи Spark
    spark.stop()

if __name__ == "__main__":
    answer_business_questions()
