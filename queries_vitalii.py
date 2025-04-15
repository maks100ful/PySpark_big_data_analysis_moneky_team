from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, floor, row_number, expr, length, avg, count, size,
    array_sort, concat_ws
)
from pyspark.sql.types import ArrayType
from pyspark.sql.window import Window

# Import your data loading functions from your module.
from dataframes import (
    get_title_basic_df,
    get_title_ratings_df,
    get_title_akas_df,
    get_title_episode_df,
    get_title_crew_df,
    get_title_principals_df,
    get_name_basic_df
)

# Create the Spark session.
spark = SparkSession.builder.appName("IMDB_Queries").getOrCreate()

# -----------------------------------------------------------------------------
# Helper function to drop any columns that are of ArrayType.
def drop_array_columns(df):
    cols_to_drop = [field.name for field in df.schema.fields if isinstance(field.dataType, ArrayType)]
    return df.drop(*cols_to_drop)

# -----------------------------------------------------------------------------
# Load DataFrames.
df_title_basic = get_title_basic_df(spark)
df_title_ratings = get_title_ratings_df(spark)
df_title_akas = get_title_akas_df(spark)
df_title_episode = get_title_episode_df(spark)
df_title_crew = get_title_crew_df(spark)
df_title_principals = get_title_principals_df(spark)
df_name_basics = get_name_basic_df(spark)
# -----------------------------------------------------------------------------

# ------------------------------
# Query 1: Top "The-" Movies by Decade
#   - Join: df_title_basic joined with df_title_ratings.
#   - Filters: title starts with "The", released after 1900, rating > 7.
#   - Group by: Compute movie count per decade.
#   - Window: Rank movies by number of votes within each decade.
# ------------------------------
df_q1 = df_title_basic.join(df_title_ratings, "tconst")

df_q1 = df_q1.filter(col("primaryTitle").startswith("The")) \
             .filter(col("startYear") > 1900) \
             .filter(col("averageRating") > 7)

# Create a decade column.
df_q1 = df_q1.withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int"))

# Group by decade to count movies.
df_q1_grouped = df_q1.groupBy("decade").agg(expr("count(tconst) as movie_count"))

# Window function: Rank movies within each decade by numVotes.
win_q1 = Window.partitionBy("decade").orderBy(col("numVotes").desc())
df_q1_ranked = df_q1.withColumn("rank", row_number().over(win_q1))

# Drop array-typed columns.
df_q1_ranked = drop_array_columns(df_q1_ranked)
df_q1_grouped = drop_array_columns(df_q1_grouped)

# Save results.
df_q1_ranked.write.mode("overwrite").csv("output/query1_details")
df_q1_grouped.write.mode("overwrite").csv("output/query1_grouped")

# ------------------------------
# Query 2: Highly Rated Long Titles
#   - Join: df_title_basic with df_title_ratings.
#   - Filters: title length > 20, average rating > 8, and numVotes > 1000.
#   - Group by: Create a title length bucket and compute the average rating per bucket.
#   - Window: Rank movies globally by averageRating.
# ------------------------------
df_q2 = df_title_basic.join(df_title_ratings, "tconst")

df_q2 = df_q2.filter(length(col("primaryTitle")) > 20) \
             .filter(col("averageRating") > 8) \
             .filter(col("numVotes") > 1000)

# Window function: Rank movies by averageRating.
win_q2 = Window.orderBy(col("averageRating").desc())
df_q2_ranked = df_q2.withColumn("rating_rank", row_number().over(win_q2))

# Create a title length bucket.
df_q2_ranked = df_q2_ranked.withColumn("length_bucket", (length(col("primaryTitle")) / 10).cast("int"))

# Group by bucket.
df_q2_grouped = df_q2_ranked.groupBy("length_bucket").agg(expr("avg(averageRating) as avg_rating"))

# Drop array columns.
df_q2_ranked = drop_array_columns(df_q2_ranked)
df_q2_grouped = drop_array_columns(df_q2_grouped)

# Save results.
df_q2_ranked.write.mode("overwrite").csv("output/query2_ranked")
df_q2_grouped.write.mode("overwrite").csv("output/query2_grouped")

# ------------------------------
# Query 3: Non‑US, Non‑English Movies
#   - Join: df_title_akas joined with df_title_ratings (on titleId == tconst).
#   - Filters: region not "US", language not "en", averageRating >= 6.
#   - Group by: Aggregate by language.
#   - Window: Rank movies within each region by averageRating.
# ------------------------------
df_q3 = df_title_akas.join(df_title_ratings, df_title_akas.titleId == df_title_ratings.tconst)

df_q3 = df_q3.filter(col("region") != "US") \
             .filter(col("language") != "en") \
             .filter(col("averageRating") >= 6)

win_q3 = Window.partitionBy("region").orderBy(col("averageRating").desc())
df_q3_ranked = df_q3.withColumn("region_rank", row_number().over(win_q3))

df_q3_grouped = df_q3_ranked.groupBy("language").agg(
    expr("count(titleId) as movie_count"),
    expr("avg(averageRating) as avg_rating")
)

# Drop array columns.
df_q3_ranked = drop_array_columns(df_q3_ranked)
df_q3_grouped = drop_array_columns(df_q3_grouped)

# Save results.
df_q3_ranked.write.mode("overwrite").csv("output/query3_ranked")
df_q3_grouped.write.mode("overwrite").csv("output/query3_grouped")

# ------------------------------
# Query 4: Season Quality in Series Episodes
#   - Join: df_title_episode with df_title_ratings.
#   - Filters: non-null seasonNumber and numVotes >= 50.
#   - Group by: Aggregate by series and season.
#   - Window: Rank seasons within each series by average episode rating.
# ------------------------------
df_q4 = df_title_episode.join(df_title_ratings, "tconst")

df_q4 = df_q4.filter(col("seasonNumber").isNotNull()) \
             .filter(col("numVotes") >= 50)

df_q4_grouped = df_q4.groupBy("parentTconst", "seasonNumber") \
                     .agg(avg("averageRating").alias("avg_episode_rating"))

win_q4 = Window.partitionBy("parentTconst").orderBy(col("avg_episode_rating").desc())
df_q4_ranked = df_q4_grouped.withColumn("season_rank", row_number().over(win_q4))

df_q4_ranked = drop_array_columns(df_q4_ranked)

# Save results.
df_q4_ranked.write.mode("overwrite").csv("output/query4_ranked")

# ------------------------------
# Query 5: Genre Combination Analysis
#   - Join: df_title_basic with df_title_ratings.
#   - Filter: Only movies with at least 2 genres.
#   - Transformation: Create a sorted, concatenated genre string.
#   - Group by: Aggregate by the genre combination.
#   - Window: Rank genre combinations by movie count.
# ------------------------------
df_q5 = df_title_basic.join(df_title_ratings, "tconst")

df_q5 = df_q5.filter(size(col("genres")) >= 2)

df_q5 = df_q5.withColumn("genre_combo", concat_ws(",", array_sort(col("genres"))))

df_q5_grouped = df_q5.groupBy("genre_combo").agg(
    expr("avg(averageRating) as avg_rating"),
    expr("count(tconst) as movie_count")
)

win_q5 = Window.orderBy(col("movie_count").desc())
df_q5_grouped_ranked = df_q5_grouped.withColumn("combo_rank", row_number().over(win_q5))

df_q5_grouped_ranked = drop_array_columns(df_q5_grouped_ranked)

# Save results.
df_q5_grouped_ranked.write.mode("overwrite").csv("output/query5_results")

# ------------------------------
# Query 6: Post‑2010 Actor Productivity
#   - Join: df_title_principals, df_title_basic, df_title_ratings, and df_name_basics.
#   - Filter: Movies released after 2010.
#   - Group by: By actor (unique actor identifier and primaryName) to count movie appearances.
#   - Window: Rank actors by movie appearance count.
# ------------------------------
# First, select only needed columns from df_name_basics and alias the nconst column.
df_name_basics_small = df_name_basics.select(
    col("nconst").alias("actor_nconst"), col("primaryName")
)

# Join principals with basics and ratings, then join with the reduced name basics.
df_q6 = df_title_principals.join(df_title_basic, "tconst") \
       .join(df_title_ratings, "tconst") \
       .join(df_name_basics_small, df_title_principals.nconst == df_name_basics_small.actor_nconst)

df_q6 = df_q6.filter(col("startYear") > 2010)

# Before grouping in Query 6, repartition data to distribute load.
df_q6 = df_q6.repartition(200)

# Group by actor_nconst and primaryName.
df_q6_grouped = df_q6.groupBy("actor_nconst", "primaryName") \
                     .agg(count("tconst").alias("movie_count"))

win_q6 = Window.orderBy(col("movie_count").desc())
df_q6_ranked = df_q6_grouped.withColumn("actor_rank", row_number().over(win_q6))

df_q6_ranked = drop_array_columns(df_q6_ranked)

# Save results.
df_q6_ranked.write.mode("overwrite").csv("output/query6_ranked")

# -----------------------------------------------------------------------------
# Stop the Spark session.
spark.stop()
