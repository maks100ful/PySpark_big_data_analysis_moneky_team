import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, BooleanType, StringType
from dataframes import *

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("IMDbTests") \
        .getOrCreate()

# --- Title Basics ---
def test_title_basic_df_columns(spark):
    df = get_title_basic_df(spark)
    expected = {"tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"}
    assert expected.issubset(df.columns)

def test_title_basic_df_genres_array(spark):
    df = get_title_basic_df(spark)
    assert isinstance(df.schema["genres"].dataType, ArrayType)

def test_title_basic_df_isAdult_boolean(spark):
    df = get_title_basic_df(spark)
    assert isinstance(df.schema["isAdult"].dataType, BooleanType)

# --- Name Basics ---
def test_name_basic_df_profession_and_known_for_arrays(spark):
    df = get_name_basic_df(spark)
    assert isinstance(df.schema["primaryProfession"].dataType, ArrayType)
    assert isinstance(df.schema["knownForTitles"].dataType, ArrayType)

# --- Title Akas ---
def test_title_akas_df_array_fields_and_bool(spark):
    df = get_title_akas_df(spark)
    assert isinstance(df.schema["types"].dataType, ArrayType)
    assert isinstance(df.schema["attributes"].dataType, ArrayType)
    assert isinstance(df.schema["isOriginalTitle"].dataType, BooleanType)

# --- Title Crew ---
def test_title_crew_df_directors_and_writers_arrays(spark):
    df = get_title_crew_df(spark)
    assert isinstance(df.schema["directors"].dataType, ArrayType)
    assert isinstance(df.schema["writers"].dataType, ArrayType)

# --- Title Episode ---
def test_title_episode_df_columns_exist(spark):
    df = get_title_episode_df(spark)
    expected = {"tconst", "parentTconst", "seasonNumber", "episodeNumber"}
    assert expected.issubset(df.columns)

# --- Title Principals ---
def test_title_principals_df_columns_exist(spark):
    df = get_title_principals_df(spark)
    expected = {"tconst", "ordering", "nconst", "category", "job", "characters"}
    assert expected.issubset(df.columns)

# --- Title Ratings ---
def test_title_ratings_df_columns_and_types(spark):
    df = get_title_ratings_df(spark)
    expected = {"tconst", "averageRating", "numVotes"}
    assert expected.issubset(df.columns)
    assert df.schema["averageRating"].dataType.simpleString() in {"double", "float"}
    assert df.schema["numVotes"].dataType.simpleString() in {"int", "long"}