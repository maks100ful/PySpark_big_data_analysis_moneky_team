from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, IntegerType

title_akas_schema = StructType([
    StructField("titleId", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", StringType(), True), 
    StructField("attributes", StringType(), True),
    StructField("isOriginalTitle", IntegerType(), True),
])

title_basics_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", StringType(), True),
    StructField("startYear", IntegerType(), True), 
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", StringType(), True),
    StructField("genres", StringType(), True), 
])

title_crew_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True),
])

title_episode_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", IntegerType(), True),
    StructField("episodeNumber", IntegerType(), True),
])

title_principals_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True),
])

title_ratings_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("averageRating", DoubleType(), True),
    StructField("numVotes", IntegerType(), True),
])

name_basics_schema = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", StringType(), True),
    StructField("deathYear", StringType(), True),
    StructField("primaryProfession", StringType(), True)
    StructField("knownForTitles", StringType(), True),
])
