import os
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from shemas import (
    title_basics_schema,
    title_akas_schema,
    title_crew_schema,
    title_episode_schema,
    title_principals_schema,
    title_ratings_schema,
    name_basics_schema,
)

schema_map = {
    "title.basics.tsv.gz": title_basics_schema,
    "title.akas.tsv.gz": title_akas_schema,
    "title.crew.tsv.gz": title_crew_schema,
    "title.episode.tsv.gz": title_episode_schema,
    "title.principals.tsv.gz": title_principals_schema,
    "title.ratings.tsv.gz": title_ratings_schema,
    "name.basics.tsv.gz": name_basics_schema,
}

def load_and_describe(spark, path, schema=None):
    print(f"\n=== Аналіз {path} ===")
    df = spark.read.csv(
        path,
        sep="\t",
        header=True,
        schema=schema,
        nullValue="\\N"
    )
    df.printSchema()
    df.show(5)

    numeric_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ("int", "bigint", "double", "float")]
    if numeric_cols:
        df.select(numeric_cols).describe().show()
        
        numeric_pdf = df.select(numeric_cols).limit(10000).toPandas()
        for col in numeric_cols:
            series = numeric_pdf[col].dropna()
            if col == "endYear" or series.nunique() < 5 or series.nunique() == 1:
                continue

            plt.figure(figsize=(10, 5))
            plt.hist(series, bins='auto', edgecolor="black")
            plt.title(f"{os.path.basename(path)}: {col}", fontsize=12)
            plt.xlabel(col)
            plt.ylabel("Кількість")
            plt.grid(True, linestyle='--', linewidth=0.5, alpha=0.7)
            plt.tight_layout()

            file_name = f"{os.path.basename(path).replace('.', '_')}_{col}_hist.png"
            plt.savefig(f"output_plots/{file_name}")
            plt.close()
    else:
        print("Немає числових стовпців для аналізу.")

def main():
    spark = SparkSession.builder.appName("DatasetStats").getOrCreate()
    os.makedirs("output_plots", exist_ok=True)

    for file_name in os.listdir("raw_data"):
        if file_name.endswith(".tsv") or file_name.endswith(".tsv.gz"):
            path = os.path.join("raw_data", file_name)
            schema = schema_map.get(file_name)
            load_and_describe(spark, path, schema)

    spark.stop()

if __name__ == "__main__":
    main()